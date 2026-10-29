using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgWorker.Backups.Sql;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Backups;

/// <summary>Итог контроля одного прохода : State — СВЕЖЕЕ состояние шарда
/// ПОСЛЕ контроля (записанное контролем или прежнее при «не время»); ChainBroken —
/// вычисляется из State: state=WAL_CHAIN_ERROR (разрыв цепочки/слот — permanent, 
/// маркер живёт в etcd-ключе и переживает рестарт/takeover) — приёмники шарда
/// замирают сами по WAL_CHAIN_ERROR-гейту (arch/19 §3). Transient-деградация
/// (DEGRADED lag/тишина) держит ChainBroken=false — приёмник живёт в контейнере
/// ноды и продолжает стрим/ретраи без пересоздания (spec §3.2 п.5/п.7).</summary>
public sealed record ControlOutcome(WalStreamState? State)
{
    public bool ChainBroken => State is { State: WalStreamStatus.WalChainError };
}

/// <summary>WalStreamProcess — машина одного тика WAL-архивации шардов кластера
/// под клэймом <C> (/, arch/19 §3): (0) санитарный снос legacy wal-контейнеров
/// pgw-backup-wal-&lt;C&gt;-* (миграция без оператора), (1) креды/ensure-зависимости,
/// (2) резолв ДВУХ источников (мастер + sync-standby — двойная архивация), (3) ensure
/// слота per-instance (живой слот на любой ноде — не WAL_CHAIN_ERROR), (4–5) факты приёмников
/// из статус-ключей etcd agent/&lt;N&gt; (Range; агентов-контейнеров больше нет —
/// приёмник живёт в контейнере ноды) + деградация «источник без живого приёмника»,
/// (6–7) контроль цепочки+lag по расписанию Wal:VerifyIntervalSec; gap (tli_jump) и
/// pool_exhausted — факты статус-ключа МАСТЕР-приёмника, (8) статус etcd с
/// agents-фактами. runtime() == null (Enabled=false) — стоп-семантика через
/// etcd-ключ /pgworker/backups-disabled (docker-сносов нет).
/// Ошибка шарда не роняет остальные; guard'ы: Active-кластер, шард с dsn.
/// Идемпотентность каждого шага (arch/17).</summary>
public sealed class WalStreamProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ShardEndpoints shards,
    IWalSqlExecutor sql,
    BackupS3Factory s3Factory,
    WalStatusWriter status,
    ClaimStore claims,
    WorkJournal journal,
    Func<BackupsRuntimeOptions?> runtime,
    InstallSecrets secrets,
    TimeProvider clock,
    Action<string, string, long?>? lagObserver = null,
    ILogger? logger = null,
    Action<string, string, long?>? uploadedAgeObserver = null, // uploaded-age (arch/18 §2.7)
    TimeSpan? watchdogWindow = null,
    Shared.Core.Hosting.ILoopProgress? progress = null) // аудит долгих фаз: итерации поллинга дают Mark (spec §1.2 п.4)
{
    private const string Op = "backup-wal";

    // Расписание контроля (list S3 — не каждый тик): ключ cluster/shard → unix последнего прохода.
    // При WAL_CHAIN_ERROR-ключе шарда или exhaustion-DEGRADED расписание НЕ действует —
    // контроль каждый тик (/).
    private readonly ConcurrentDictionary<string, long> _lastVerifyUnix = [];

    // Пул-клиент per-pool: кэш теперь в САМОЙ фабрике (arch/19 §5 «Кэш
    // пул-клиентов» — breaker-стейт переживает тики, владелец — фабрика);
    // WalStream-кэш упразднён: локальный dispose кэшированного фабрикой
    // клиента закрывал бы общие HttpClients. Смена контекста — новая запись
    // в кэше фабрики, прежняя живёт до конца процесса.
    private PoolBackupS3 PoolClient(BackupPoolContext pool)
        => s3Factory.Create(pool);

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант arch/14 §4.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"backup-wal {cluster}: claim is not ours (or lost) — mutations are forbidden"));

        //  (arch/19 §3, решение spec §7 №11): санитарный снос legacy
        // wal-контейнеров pgw-backup-wal-<C>-* (без него старый агент держит
        // физический слот — один активный consumer — и блокирует приёмник).
        // Один list на тик кластера, идемпотентно (404 = успех); недоступность
        // docker-хоста НЕ валит тик (multi-host: снос миграционный, мёртвый
        // хост снести нельзя — контроль/стоп-семантика остальных шардов идут
        // дальше) — запись журнала, ретрай следующим тиком.
        var swept = await driver.RemoveBackupAgentsAsync(cluster, shard: null, ct);
        if (!swept.IsSuccess)
        {
            logger?.LogWarning("backup-wal {Cluster}: legacy agent sweep incomplete: {Error}",
                cluster, swept.Error!.Message);
            await journal.WritePhaseAsync(cluster, Op, "legacy-sweep-partial", claims.InstanceId,
                swept.Error!.Message, ct);
        }

        var options = runtime();

        // Стоп-семантика Enabled=false: приёмники замирают по /pgworker/backups-disabled
        // (стоп-гейт приёмника), wal-ключ — финальный STOPPED; неактивная подсистема
        // дальше не идёт.
        if (options is null)
            return await StopAllAsync(cluster, snap, backups, ct);

        // Guard: только Active-кластер (spec §3.2).
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var shardBackups = backups?.Shards
                           ?? (IReadOnlyDictionary<string, ShardBackups>)new Dictionary<string, ShardBackups>();
        //  (arch/19 §4): резолв пула — ОДИН на тик кластера (контекст общий для
        // всех шардов): etcd-чтения policy/пула/кредов не повторяются per-shard;
        // разбор результата (null/Failed-побочные эффекты) — per-shard ниже.
        var poolResult = await s3Factory.ResolveAsync(cluster, ct);
        foreach (var shard in snap.Shards)
        {
            if (shard.Dsn is null)
                continue; // не поднят — домен AddShard

            try
            {
                await TickShardAsync(cluster, snap, shard, shardBackups.GetValueOrDefault(shard.Name),
                    options, poolResult, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // ошибка шарда не роняет остальные (spec §3.2): журнал + следующий шард
                logger?.LogError(ex, "backup-wal {Cluster}/{Shard}: {Message}", cluster, shard.Name, ex.Message);
                await journal.WritePhaseAsync(cluster, Op, "shard-error", claims.InstanceId,
                    $"{shard.Name}: {ex.Message}", ct);
            }
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // ── Шаги одного шарда (spec §3.2 п.1–5) ──

    // Наблюдатели контрольного прохода (arch/18 §2.7): лаг и uploaded-age —
    // один факт одного прохода, одинаковая null-семантика (WAL_CHAIN_ERROR/STOPPED/факта
    // нет — серии исчезают; null идемпотентен).
    private void Observe(string cluster, string shard, long? lagSegments, long? uploadedAgeSec)
    {
        lagObserver?.Invoke(cluster, shard, lagSegments);
        uploadedAgeObserver?.Invoke(cluster, shard, uploadedAgeSec);
    }

    private async Task TickShardAsync(
        string cluster, ClusterSnapshot snap, ShardSpec shard, ShardBackups? shardBackups,
        BackupsRuntimeOptions options, Result<BackupPoolContext?> poolResult, CancellationToken ct)
    {
        //  §3.4 гвард: шард с активной restore-заявкой (PLANNED/RUNNING/
        // REJOINING) — контроль не идёт: демонтаж restore трогает шард, слоты/
        // источники в этот период не манипулируются (контуры не пересекаются).
        if (shardBackups?.Restores.Any(r => r.State
                is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining) == true)
            return;

        // (1) Креды: /clusters/<C>/backup_password : отсутствует → transient-пропуск
        //     шарда с journal-заметкой — приёмник стоит в waiting_password, ретраи тиками.
        var passwordKv = await GetAsync($"/clusters/{cluster}/backup_password", ct);
        if (passwordKv is not { Value: { Length: > 0 } })
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-password", claims.InstanceId,
                $"{shard.Name}: missing /clusters/{cluster}/backup_password (ensure failed)", ct);
            return;
        }

        // (1.5) (arch/19 §4): разбор результата резолва пула (резолв — один
        //     на тик кластера). null (пул не выбран) → стоп-семантика (агенты
        //     вниз, ключ не пишем); Failed (пул удалён/нода не заявлена/профиль
        //     без кредов/без адреса) → transient: journal + маркер
        //     s3-pool-exhausted в wal-статусе — тик не валится.
        if (await ApplyPoolResultAsync(poolResult, cluster, shard.Name, wal: shardBackups?.Wal, ct)
                is not { } pool)
            return;
        // Пул-клиент: кэш per-pool — переиспользование между шардами тика и
        // между тиками (пересоздание клиентов с TLS-хендшейками на каждый
        // контрольный тик росло с числом шардов); НЕ диспозится в конце тика —
        // владелец кэш (инвалидация при дрейфе контекста).
        var poolS3 = PoolClient(pool);

        // (2) Два источника (§3.3 п.2): мастер + sync-standby (двойная архивация,
        //     оба из portalloc; sync нет → только мастер — второй агент тиком при
        //     появлении; sync == мастер → один источник).
        var addresses = await shards.ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            throw new ApplicationException($"portalloc: {addresses.Error!.Message}");
        var master = await shards.ResolveMasterAsync(cluster, shard, addresses.Value, ct);
        if (!master.IsSuccess)
            throw new ApplicationException($"master resolve: {master.Error!.Message}");
        if (master.Value is not { } masterAddr)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-master", claims.InstanceId,
                $"{shard.Name}: master does not resolve (failover window)", ct);
            return;
        }

        var (masterRef, masterPgHost, masterPgPort) = ResolveNodeRef(shard, addresses.Value, masterAddr);
        var sources = new List<WalSource> { new(masterRef, masterAddr, masterPgHost, masterPgPort) };
        var sync = await shards.ResolveSyncStandbyAsync(cluster, shard, addresses.Value, ct);
        if (!sync.IsSuccess)
            throw new ApplicationException($"sync-standby resolve: {sync.Error!.Message}");
        if (sync.Value is { } syncAddr && syncAddr != masterAddr)
        {
            var (syncRef, syncPgHost, syncPgPort) = ResolveNodeRef(shard, addresses.Value, syncAddr);
            sources.Add(new WalSource(syncRef, syncAddr, syncPgHost, syncPgPort));
        }

        var slot = BackupNames.Slot(cluster, shard.Name);
        var wal = shardBackups?.Wal;
        var chainKnown = wal is not null;
        var adminDsn = ShardEndpoints.AdminDsn(masterAddr, snap.Config.DbName, secrets);

        // Стоп-семантика QUARANTINED (все ноды шарда): агент вниз + STOPPED.
        if (shard.Nodes is { Count: > 0 } && shard.Nodes.All(n => n.State == NodeState.Quarantined))
        {
            await StopShardAsync(cluster, shard.Name, wal, ct);
            return;
        }

        // (3) Слот per-instance (§3.3 п.3; arch/19 §3): имя слота одно и то же
        //     на КАЖДОЙ ноде-источнике (слоты разных инстансов независимы); зонд —
        //     существование И wal_status: alive = существует и НЕ lost. WAL_CHAIN_ERROR —
        //     слоты ВСЕХ источников исчезли ИЛИ потеряны при живой цепочке (живой слот
        //     на любой ноде держит WAL); lost одного при живом втором — авто-recreate
        //     без WAL_CHAIN_ERROR (потерянный агент вернётся от хвоста S3 ретраем приёмника).
        var slotProbes = new List<(WalSource Src, string Dsn, bool Exists, string? WalStatus)>();
        foreach (var src in sources)
        {
            var dsn = ShardEndpoints.AdminDsn(src.Addr, snap.Config.DbName, secrets);
            var probe = await sql.SlotProbeAsync(dsn, slot, ct);
            if (!probe.IsSuccess)
                throw new ApplicationException($"slot probe {slot}@{src.Node}: {probe.Error!.Message}");
            slotProbes.Add((src, dsn, probe.Value.Exists, probe.Value.WalStatus));
        }
        // null-статус существующего слота (старые PG/edge) — живой: лечим только явный lost
        static bool Alive((WalSource Src, string Dsn, bool Exists, string? WalStatus) p)
            => p.Exists && p.WalStatus != "lost";

        if (slotProbes.All(p => !Alive(p)))
        {
            if (chainKnown && wal!.State is WalStreamStatus.Active or WalStreamStatus.Degraded)
            {
                //  (arch/19 §3): инвариант писателя (last_uploaded_unix в живом
                // ACTIVE/DEGRADED-ключе) может быть нарушен (ручная правка ключа/
                // будущий писатель при ослабленном гварде парсера — прецедент
                // ed1561b): битый ключ не роняет тик форс-мьютом. Фолбэк
                // clock-сейчас — defensive-значение ТОЛЬКО параметра baseUnix:
                // BreakAsync потребляет его лишь при wal == null (запись с нуля),
                // здесь wal != null (chainKnown) — в WAL_CHAIN_ERROR-запись now() НЕ попадает
                // («факт над записью», ревью Ф4-2 №2); факт битого ключа — в журнале.
                if (wal.LastUploadedUnix is null)
                {
                    var invalid = $"last_uploaded_unix missing — invalid key /pgworker/backups/{cluster}/{shard.Name}/wal "
                        + "(manual edit/another writer); the 'slot disappeared' branch runs with the time fallback";
                    logger?.LogWarning("backup-wal {Cluster}/{Shard}: {Message}", cluster, shard.Name, invalid);
                    await journal.WritePhaseAsync(cluster, Op, $"wal-key-invalid/{shard.Name}",
                        claims.InstanceId, invalid, ct);
                }

                // WAL_CHAIN_ERROR + слот пересоздаётся immediate+reserved СРАЗУ; error
                // различает «исчез» / «потерян (lost)» по факту зонда 
                var anyExists = slotProbes.Any(p => p.Exists);
                await BreakAsync(cluster, shard.Name, wal, slot, masterRef, adminDsn, ct,
                    baseStart: wal.LastUploadedSegment is { Length: > 0 }
                        ? wal.LastUploadedSegment : wal.ChainStartSegment,
                    baseLast: wal.LastUploadedSegment,
                    baseUnix: wal.LastUploadedUnix ?? clock.GetUtcNow().ToUnixTimeSeconds(),
                    error: anyExists
                        ? $"slot {slot} lost (wal_status=lost) on all sources with a live chain — retake the full backup"
                        : $"slot {slot} disappeared with a live chain (max_slot_wal_keep_size invalidation?) — retake the full backup",
                    recreateSlot: true);
                return;
            }

            // Первый старт ИЛИ WAL_CHAIN_ERROR-ключ: ensure-alive на КАЖДОМ источнике
            // (отсутствует → create; lost → recreate — пустой префикс wal/ + lost лечится
            // recreate без WAL_CHAIN_ERROR, свежий слот держит текущую позицию).
            foreach (var dead in slotProbes)
            {
                var ensured = await sql.EnsureSlotAliveAsync(dead.Dsn, slot, ct);
                if (!ensured.IsSuccess)
                    throw new ApplicationException($"ensure slot {slot}@{dead.Src.Node}: {ensured.Error!.Message}");
            }
        }
        else
        {
            // lost при живом втором источнике — НЕ WAL_CHAIN_ERROR: recreate потерянного
            // (journal-before-manipulations, arch/17: фаза ДО мутации — видимость
            // действия при падении между drop и create; тик повторит).
            foreach (var lost in slotProbes.Where(p => p.Exists && p.WalStatus == "lost"))
            {
                await journal.WritePhaseAsync(cluster, Op, $"slot-recreate/{shard.Name}", claims.InstanceId,
                    $"{lost.Src.Node}: slot {slot} wal_status=lost — slot recreated, receiver will catch up from the S3 tail", ct);
                var recreated = await sql.RecreateSlotAsync(lost.Dsn, slot, ct);
                if (!recreated.IsSuccess)
                    throw new ApplicationException($"recreate slot {slot}@{lost.Src.Node}: {recreated.Error!.Message}");
            }

            // отсутствующие на живых источниках → create (ensure-идемпотентность)
            foreach (var missing in slotProbes.Where(p => !p.Exists))
            {
                var created = await sql.EnsureSlotAliveAsync(missing.Dsn, slot, ct);
                if (!created.IsSuccess)
                    throw new ApplicationException($"ensure slot {slot}@{missing.Src.Node}: {created.Error!.Message}");
            }
        }

        // (4) (arch/19 §4): факты приёмников из статус-ключей etcd — Range
        //     /pgworker/backups/<C>/<X>/agent/ (мастер+sync — живые ключи lease).
        //     Транспорт etcd недоступен — null (transient: not-running-проверка и
        //     факты пропускаются, контроль продолжается по S3).
        // (5) Контроль — ControlDueAsync несёт и агентов-факты (не-running
        //     деградация, gap/exhaustion — из фактов мастер-приёмника), и запись
        //     agents в wal-ключ (только при живом ключе — ключ создаёт контроль).
        var shardNodes = addresses.Value.Keys
            .Where(k => k.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
            .Select(k => k.Split('/')[1])
            .ToList();
        var agentFacts = await ReadAgentFactsAsync(cluster, shard.Name, ct);
        // etcd-транспорт недоступен (agentFacts null) — факты НЕНАБЛЮДАЕМЫ:
        // agents не трогаем (сохраняем прежние из ключа), «неизвестно» ≠ «absent».
        var agentStates = wal is not null
            ? (agentFacts is null ? wal.Agents : BuildAgentFacts(shardNodes, agentFacts))
            : null; // факты — ДОБАВКА к живому ключу (ключ создаёт контроль, spec п.8)

        // (6–7) Контроль: WAL_CHAIN_ERROR-исход — ключ WAL_CHAIN_ERROR (слот recreate при «слот
        // исчез»); docker-сносов в тике больше нет.
        await ControlDueAsync(
            cluster, shard.Name, wal, shardBackups, options, sources, slot, adminDsn,
            poolS3, agentFacts, agentStates, ct);
    }

    // Чтение фактов приёмников шарда : Range статус-ключей; транспорт-фейл —
    // null (не исключение — надзор и факты transient, контроль по S3 продолжается).
    private async Task<IReadOnlyList<WalReceiverAgentFacts>?> ReadAgentFactsAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var read = await new WalReceiverStatusReader(etcd, endpoints).ReadAsync(cluster, shard, ct);
        return read.IsSuccess ? read.Value : null;
    }

    //  (arch/19 §4): agents-факты wal-ключа из статус-ключей приёмников —
    // по нодам portalloc шарда: ключ жив (есть в Range) → Running, нет → Absent.
    // exited новыми записями не пишется (легаси-значение панельного парсера).
    internal static IReadOnlyList<WalAgentState> BuildAgentFacts(
        IReadOnlyList<string> shardNodes,
        IReadOnlyList<WalReceiverAgentFacts>? facts)
        => [.. shardNodes.Select(n => new WalAgentState(n,
            facts?.Any(f => f.Node == n) == true
                ? WalAgentPresence.Running
                : WalAgentPresence.Absent))];

    //  (arch/19 §4): разбор ГОТОВОГО результата резолва пула (резолв — один
    // на тик кластера, результат общий для всех шардов). Контекст | null;
    // «пул не выбран» и Failed-резолв оба возвращают null (тик без мутаций),
    // различие — в побочных эффектах ниже.
    private async Task<BackupPoolContext?> ApplyPoolResultAsync(
        Result<BackupPoolContext?> resolved, string cluster, string shardName,
        WalStreamState? wal, CancellationToken ct)
    {
        if (resolved.IsSuccess)
        {
            if (resolved.Value is { } ctx)
                return ctx;
            // Пул не выбран (policy без s3): docker-вызовов НЕТ (— приёмники
            // сами стоят в waiting_policy по гейтам etcd); wal-ключ оставляем
            // (алерт backup-pool-missing — проекция панели).
            // Диагностика (разбор): причина — в host.log (etcd-journal сносит
            // teardown, артефактная телеметрия теряет журнал).
            logger?.LogInformation("backup-wal {Cluster}/{Shard}: pool not selected (policy without s3 section) — receivers wait in waiting_policy",
                cluster, shardName);
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-pool", claims.InstanceId,
                $"{shardName}: policy without s3 section — pool not selected", ct);
            Observe(cluster, shardName, null, null);
            return null;
        }

        // Failed-резолв — transient (arch/19 §4): journal + маркер s3-pool-exhausted
        // в живом wal-статусе (алерт backup-pool-unreachable закрывает и случай
        // «policy записана, но резолв не удался»); ретраи тиком.
        // Диагностика (разбор): причина — в host.log (etcd-journal сносит teardown).
        logger?.LogWarning("backup-wal {Cluster}/{Shard}: pool resolve failed — {Error}",
            cluster, shardName, resolved.Error!.Message);
        await journal.WritePhaseAsync(cluster, Op, "waiting-backup-pool", claims.InstanceId,
            $"{shardName}: {resolved.Error!.Message}", ct);
        if (wal is { State: WalStreamStatus.Active or WalStreamStatus.Degraded }
            && wal.Error?.Contains(S3PoolMarkers.Exhausted, StringComparison.Ordinal) != true)
            await status.WriteIfChangedAsync(cluster, shardName, wal with
            {
                State = WalStreamStatus.Degraded,
                Error = $"{S3PoolMarkers.Exhausted}: {resolved.Error!.Message}",
            }, ct);
        return null;
    }

    /// <summary>Источник WAL-архивации (/): нода-источник (мастер/sync —
    /// имя из portalloc), её адрес (docker-хост контейнера ноды, приёмник
    /// стримит изнутри) и conninfo-пара.</summary>
    private sealed record WalSource(string Node, NodeAddress Addr, string PgHost, int PgPort);

    // nodeRef + conninfo-пара ноды-источника: каноническая нода → alias :5432 в
    // сети нод; усыновлённая (object) → host:pg-port из portalloc (arch/19 §3).
    // DRY: общий для мастера и sync.
    private (string NodeRef, string PgHost, int PgPort) ResolveNodeRef(
        ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, NodeAddress node)
    {
        foreach (var shardNode in shard.Nodes)
            if (addresses.TryGetValue($"{shard.Name}/{shardNode.Name}", out var addr)
                && addr.Host == node.Host
                && addr.Ports == node.Ports)
                return (shardNode.Name, shardNode.Name, 5432); // alias сети нод
        return (node.Object ?? $"{node.Host}:{node.Ports.Pg}", node.Host, node.Ports.Pg);
    }

    private async Task<WalStreamState?> ReadWalAsync(string cluster, string shard, CancellationToken ct)
    {
        var read = await status.ReadAsync(cluster, shard, ct);
        if (!read.IsSuccess)
            throw new ApplicationException($"wal key read: {read.Error!.Message}");
        return read.Value;
    }

    // Точечный GET с failover (паттерн ShardEndpoints.GetAsync); ошибка транспорта —
    // исключение (поймается пер-шардовой обёрткой → журнал), null = ключа нет.
    private async Task<Kv?> GetAsync(string key, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, key, ct);
            if (!result.IsSuccess)
            {
                last = result;
                continue;
            }

            return result.Value;
        }

        if (!last.IsSuccess)
            throw new ApplicationException($"get {key}: {last.Error!.Message}");
        return null;
    }

    // (5–7) Контроль (spec §3.2 п.6–8; arch/19 §3): при WAL_CHAIN_ERROR-ключе — КАЖДЫЙ
    // тик (без VerifyIntervalSec-расчёта: скорость заживления; list дырного
    // префикса дёшев); иначе по расписанию. list S3 → chain_start (:
    // wal_start новейшего verify-OK COMPLETED-полного, ratchet — не понижается
    // ?? min-объект)
    // → CheckChain → дыра: WAL_CHAIN_ERROR (агентов-контейнеров больше нет, только
    // ключ). факты прогресса — ТОЛЬКО из наблюдений: last_uploaded из S3-объектов
    // либо прошлого ключа, никогда от now() («факт над записью», ревью Ф4-2 №2);
    // lag-зонд; transient-деградации (not-running/gap/exhaustion/lag/тишина)
    // docker-сносов не требуют. gap (tli_jump) и pool_exhausted — факты
    // статус-ключа МАСТЕР-приёмника (arch/19 §4); agents-факты — в ключе.
    private async Task<ControlOutcome> ControlDueAsync(
        string cluster, string shard, WalStreamState? wal, ShardBackups? shardBackups,
        BackupsRuntimeOptions options, IReadOnlyList<WalSource> sources, string slot, string adminDsn,
        PoolBackupS3 s3, IReadOnlyList<WalReceiverAgentFacts>? agentFacts,
        IReadOnlyList<WalAgentState>? agentStates, CancellationToken ct)
    {
        var masterRef = sources[0].Node;
        var masterFacts = agentFacts?.FirstOrDefault(f => f.Node == masterRef);
        var key = $"{cluster}/{shard}";
        var now = clock.GetUtcNow().ToUnixTimeSeconds();
        // Расписание — пропуск только для НЕ-WAL_CHAIN_ERROR и НЕ-exhaustion-DEGRADED
        // (: заживление подъёмом ноды пула — контроль каждый тик, симметрично
        // WAL_CHAIN_ERROR; union-листинг — один класс вызовов).
        if (wal is not { State: WalStreamStatus.WalChainError }
            && !IsExhaustionDegraded(wal)
            && _lastVerifyUnix.TryGetValue(key, out var last)
            && now - last < options.WalVerifyIntervalSec)
            return new ControlOutcome(wal);
        // (листим S3 и до создания ключа каждый тик — скорость первого наблюдения,
        // пустой префикс дёшев; подтверждено ревью Ф4-2 как допустимое)

        _lastVerifyUnix[key] = now;

        // Истина прогресса — объекты S3 (arch/19 §3): list префикса wal/ —
        // UNION по всем нодам пула (arch/19 §5 «где данные есть»): первая
        // доступная нода может отставать (лаг mesh) — её соло-листинг даёт
        // ложную дыру и WAL_CHAIN_ERROR (прецедент AC4: нода после рестарта догоняет
        // mesh). Пульс прогресса на время вызова (канон): прямой S3-вызов
        // на недоступном endpoint может висеть до HttpClient-таймаута без
        // отметок — watchdog (порт сноса 30 c) гасил reconcile посреди
        // wal-контроля (bad-S3 — штатный контур, transient повторит следующий тик).
        var listed = await S3PulseAsync(token => s3.ListWalUnionAsync(cluster, shard, ct: token), ct);
        if (!listed.IsSuccess)
            throw new ApplicationException($"list S3 {cluster}/{shard}/wal: {listed.Error!.Message}");
        var objects = listed.Value;

        // chain_start (arch/19 §3): wal_start новейшего (по started_unix)
        // COMPLETED-полного с verify.state=OK — ОДНА точка с cutoff-ретенции
        // (§4 п.5; расщепление точек давало бы ложный WAL_CHAIN_ERROR после среза WAL);
        // ratchet — не понижается ниже записанной границы; нет OK-полного →
        // записанная ?? min-объект потока (как раньше при полных нет).
        var ratchet = wal is { ChainStartSegment.Length: > 0 }
            ? WalFileName.TryParse(wal.ChainStartSegment) : null;
        WalFileName? fromFull = WalChain.RatchetedStart(ratchet,
            RetentionPlanner.LatestVerifiedWalStart(shardBackups?.Full ?? []));
        WalFileName? minObject = objects
            .Select(o => WalFileName.TryParse(o.Name))
            .Where(s => s is not null)
            .Select(s => s!.Value)
            .OrderBy(s => s.Name, StringComparer.Ordinal)
            .Cast<WalFileName?>()
            .FirstOrDefault();
        var chainStart = fromFull ?? minObject; // ratchet уже внутри fromFull (кандидатов нет → recorded)

        // Нет полных, нет объектов, нет прошлого ключа — писать нечего (п.8):
        // ключ не пишется до первого наблюдения; агент работает (объекты появятся).
        if (chainStart is not { } start)
        {
            Observe(cluster, shard, null, null); // факта нет — серии исчезают
            return new ControlOutcome(wal);
        }

        // CheckWithRestart: после restore promote открывает новый TLI, старые
        // сегменты обрезаны легитимно (AC4) — дыра на TLI-границе не деградация
        var chain = WalChain.CheckWithRestart(start, objects.Select(o => o.Name));

        if (!chain.IsContinuous)
        {
            // Дыра: WAL_CHAIN_ERROR даже без прошлого ключа (AC4-тотальность) —
            // BreakAsync строит запись с границей разрыва (приёмники замирают
            // по WAL_CHAIN_ERROR-гейту сами).
            var lastForBase = chain.LastSegment?.Name ?? start.Name;
            var lastModifiedForBase = objects
                .Where(o => o.Name == lastForBase)
                .Select(o => o.LastModified)
                .Select(m => (DateTimeOffset?)m)
                .FirstOrDefault() ?? DateTimeOffset.FromUnixTimeSeconds(wal?.LastUploadedUnix ?? now);
            var broken = await BreakAsync(cluster, shard, wal, slot, masterRef, adminDsn, ct,
                baseStart: chain.LastSegment?.Name ?? start.Name, // граница разрыва (§3.1)
                baseLast: lastForBase, baseUnix: lastModifiedForBase.ToUnixTimeSeconds(),
                error: chain.GapError!, recreateSlot: false);
            return new ControlOutcome(broken);
        }

        // Факты прогресса — только из наблюдений: наблюдаемый последний сегмент
        // цепочки; если цепочка не наблюдалась вовсе (объекты/LastModified недоступны,
        // всё ниже chain_start) — прошлый ключ, НИКОГДА не now() (ревью Ф4-2 №2).
        // Инвариант WalChain: LastSegment всегда из списка объектов; выборка ниже —
        // защитная (ревью P2): при его поломке смешения фактов не будет —
        // невычисленный unix падает на прошлый ключ (transient-DEGRADED, не крах тика).
        var observedLast = chain.LastSegment;
        var lastUploadedName = observedLast?.Name ?? wal?.LastUploadedSegment;
        var observedLastUnix = observedLast is { } seen
            ? objects.Where(o => o.Name == seen.Name).Select(o => o.LastModified)
                .Cast<DateTimeOffset?>().FirstOrDefault()?.ToUnixTimeSeconds()
            : null;
        var lastUploadedUnix = observedLastUnix ?? wal?.LastUploadedUnix;

        // Прогресс не наблюдался и прошлого наблюдения нет — ключ не пишем
        // (spec п.8: «ключ не пишется до первого наблюдения»; писать
        // last_uploaded = chain_start, который не загружался, — подмена факта).
        if (lastUploadedName is null || lastUploadedUnix is null)
        {
            Observe(cluster, shard, null, null); // наблюдения нет — серии исчезают
            return new ControlOutcome(wal);
        }

        // (7) Lag-зонд: pg_current_wal_lsn() мастера → сегмент → дистанция.
        long? lag = null;
        var current = await sql.CurrentWalAsync(adminDsn, ct);
        if (current.IsSuccess && WalFileName.TryParse(lastUploadedName) is { } lastSegment)
        {
            var masterSegment = WalFileName.FromLsn((uint)current.Value.Tli, current.Value.Lsn);
            lag = Math.Max(0, lastSegment.DistanceTo(masterSegment));
        }

        Observe(cluster, shard, lag, now - lastUploadedUnix.Value); // факт прохода

        //  (arch/19 §3/§4): tli-зазор — факт tli_jump из статус-ключа
        // МАСТЕР-приёмника (ключ живёт в etcd до следующего прыжка; docker-лог
        // агента не читается). Персистентная фиксация в ключе wal (переживает
        // рестарты воркера — не зависит от лога контейнера); снятие — покрытием
        // (COMPLETED-полный с wal_start ≥ gap.to) либо исчезновением профиля
        // зазора из листинга (ретенция срезала нижележащие TLI). Установка
        // гейтится теми же предикатами — цикл «снятие ↔ повторная установка»
        // при живом факте исключён. Внутри-TLI прыжок (TLI сегмента From ==
        // TLI прыжка) — обычная дыра (WAL_CHAIN_ERROR путь контроля по объектам), gap
        // НЕ ставится.
        static bool CoveredByFull(IReadOnlyList<FullBackupState> fulls, string toSegment)
            => fulls.Any(f => f.State == FullBackupStatus.Completed
                && f.WalStartSegment is { Length: > 0 } walStart
                && string.CompareOrdinal(walStart, toSegment) >= 0);
        bool ProfileObserved(string toSegment)
            => objects.Select(o => WalFileName.TryParse(o.Name)).OfType<WalFileName>()
                .Any(s => string.CompareOrdinal(s.Name, toSegment) < 0);
        var shardFulls = shardBackups?.Full ?? [];

        WalGapState? gap = wal?.Gap;
        if (gap is not null && (CoveredByFull(shardFulls, gap.To) || !ProfileObserved(gap.To)))
            gap = null; // заживление покрытием / профиль зазора срезан ретенцией

        if (gap is null
            && masterFacts?.TliJump is { } jump
            && TliOfSegment(jump.From) != jump.Tli // внутри-TLI прыжок — путь WAL_CHAIN_ERROR
            && !CoveredByFull(shardFulls, jump.To)
            && ProfileObserved(jump.To))
        {
            gap = new WalGapState(jump.From, jump.To, jump.Tli);
            await journal.WritePhaseAsync(cluster, Op, $"wal-tli-gap/{shard}", claims.InstanceId,
                $"tli jump {TliOfSegment(jump.From)}->{jump.Tli}: segments ({jump.From}, {jump.To}] " +
                "will not be delivered — the worker re-runs the full backup", ct);
        }

        // Деградации transient-природы: docker-сносов не требуют (ChainBroken
        // не нужен — агентов-контейнеров больше нет).
        string? error = null;
        var state = WalStreamStatus.Active;
        if (gap is not null)
        {
            //  (arch/19 §3/§4): первый приоритет деградаций — живой gap. Текст
            // строится ВСЕГДА из полей записи (from_tli = TLI хвостового сегмента
            // gap.From — первые 8 hex имени, через TliOfSegment; имена валидированы
            // парсером/писателем) — шаблон един для установки и удержания,
            // WriteIfChanged стабилен между тиками.
            state = WalStreamStatus.Degraded;
            error = $"tli-gap: сегменты ({gap.From}, {gap.To}] не будут доставлены — приёмник пережил " +
                    $"58P01 tli-переход {TliOfSegment(gap.From)}→{gap.Tli}; воркер переснимает полный";
        }
        else
        {
            //  (arch/19 §4): «источник без живого приёмника» — мастер или sync
            // без живого статус-ключа (lease истёк — процесс мёртв/нода вниз) при
            // не-WAL_CHAIN_ERROR wal-ключе → DEGRADED (transient-семантика, приоритет
            // ПОСЛЕ gap, ДО lag/тишины; алерт панельный по DEGRADED общий).
            // Транспорт etcd недоступен (agentFacts null) — факт ненаблюдаем:
            // проверка пропускается (не деградация).
            var notRunning = agentFacts is null
                ? []
                : sources.Select(s => s.Node)
                    .Where(n => agentFacts.All(f => f.Node != n))
                    .ToList();
            if (notRunning.Count > 0)
            {
                state = WalStreamStatus.Degraded;
                error = $"wal receiver not running on {string.Join(", ", notRunning)}";
            }
        }

        if (error is null && lag > options.WalLagMaxSegments) // null → сравнение false (зонд не удался — не деградация)
        {
            state = WalStreamStatus.Degraded;
            error = $"WAL stream lag {lag} segments (threshold {options.WalLagMaxSegments})";
        }
        else if (error is null && now - lastUploadedUnix > options.WalStaleSec)
        {
            state = WalStreamStatus.Degraded;
            error = $"upload silence {now - lastUploadedUnix} s (threshold {options.WalStaleSec})";
        }

        // // (arch/19 §3): исчерпание write-списка мастер-приёмника —
        // по флагу pool_exhausted статус-ключа (факт персистентен в etcd до
        // первого успешного put — окно вымывания лога ушло); контроль КАЖДЫЙ тик
        // при exhaustion-DEGRADED (IsExhaustionDegraded — расписание не
        // действует); алерт backup-pool-unreachable по маркеру.
        if (state == WalStreamStatus.Active && masterFacts is { PoolExhausted: true })
        {
            state = WalStreamStatus.Degraded;
            error = $"{S3PoolMarkers.Exhausted}: write list exhausted by master receiver (receiver retry loop)";
        }
        else if (state == WalStreamStatus.Degraded && error is not null
                 && wal is not { State: WalStreamStatus.Degraded }
                 && masterFacts is { PoolExhausted: true })
        {
            // Переход в DEGRADED при актуальном исчерпании — маркер-префикс в error.
            error = $"{S3PoolMarkers.Exhausted}: {error}";
        }

        // Диагностика (разбор): смена стейта — в host.log (etcd сносит teardown).
        if (state != wal?.State || error != wal?.Error)
            logger?.LogInformation("backup-wal {Cluster}/{Shard}: control {From}→{To}{Error}",
                cluster, shard, wal?.State.ToString() ?? "<none>", state,
                error is null ? "" : $"; error={error}");

        //  (arch/19 §4): agents-факты — в той же записи ключа (один писатель
        // WalStatusWriter, WriteIfChanged — идемпотентно; null — ключ без фактов).
        var next = new WalStreamState(
            state, slot, masterRef, start.Name, lastUploadedName, lastUploadedName,
            lastUploadedUnix, lag, error, Agents: agentStates, Gap: gap);
        await status.WriteIfChangedAsync(cluster, shard, next, ct);
        return new ControlOutcome(next); // цепочка цела (ACTIVE/DEGRADED)
    }

    // Разрыв цепочки (arch/19 §3): WAL_CHAIN_ERROR + error + граница разрыва в
    // chain_start. Маркер разрыва — ключ etcd (не память): переживает рестарт
    // воркера/takeover; заживление — контроль (COMPLETED-полный ≥ границы с
    // непрерывной цепью → ACTIVE). Агентов-контейнеров больше нет :
    // приёмники замирают сами по STOPPED/WAL_CHAIN_ERROR-гейту etcd — docker-сносов в
    // тике нет. recreateSlot: слот при «слот исчез» пересоздаётся immediate+
    // reserved СРАЗУ — к старту пересъёма полного слот держит позицию ≤ wal_start
    // нового полного; при дыре цепочки слот НЕ трогаем (живой копит WAL в
    // пределах max_slot_wal_keep_size). wal == null — запись создаётся с
    // наблюдаемыми фактами контроля (AC4-тотальность: дыра найдена при первом
    // наблюдении цепочки). Возвращает записанное состояние (AC4c одним тиком).
    // Инвариант baseUnix : параметр потребляется ТОЛЬКО при wal == null
    // (создание записи с нуля); при живом wal запись строится из него —
    // now()-фолбэк вызывающего в ключ не попадает. Если будущая правка начнёт
    // использовать baseUnix при живом wal — место пересмотреть: подмена факта
    // now()-временем запрещена («факт над записью», arch/19 §3).
    private async Task<WalStreamState> BreakAsync(
        string cluster, string shard, WalStreamState? wal, string slot, string masterRef,
        string adminDsn, CancellationToken ct,
        string baseStart, string baseLast, long baseUnix, string error, bool recreateSlot)
    {
        logger?.LogError("backup-wal {Cluster}/{Shard}: {Error}", cluster, shard, error);
        if (recreateSlot)
        {
            // Ошибка ensure — исключение наверх (пер-шардовый catch → журнал;
            // следующий тик повторит — идемпотентно).
            var ensured = await sql.EnsureSlotAliveAsync(adminDsn, slot, ct);
            if (!ensured.IsSuccess)
                throw new ApplicationException($"ensure slot {slot}: {ensured.Error!.Message}");
        }
        var current = wal ?? new WalStreamState(
            WalStreamStatus.Active, slot, masterRef, baseStart, baseLast, baseLast, baseUnix, null, null);
        var broken = current with
        {
            State = WalStreamStatus.WalChainError,
            ChainStartSegment = baseStart, // граница разрыва (ratchet §3)
            Error = error,
        };
        await status.WriteIfChangedAsync(cluster, shard, broken, ct);
        Observe(cluster, shard, null, null); // WAL_CHAIN_ERROR — серии исчезают
        return broken;
    }

    // Стоп-семантика Enabled=false (spec §3.2 «Стоп-семантика»): агентов
    // сносить нечего — приёмники замирают по гейту /pgworker/backups-disabled
    // (ключ поддерживает лидер BackupOrphanSweeperLoop); при живом ключе шарда —
    // финальный STOPPED (последнее касание, иначе застывший ACTIVE кормит ложный
    // wal-stream-lag).
    private async Task<Result<ProcessOutcome>> StopAllAsync(
        string cluster, ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct)
    {
        foreach (var shard in snap.Shards)
        {
            var wal = backups?.Shards.GetValueOrDefault(shard.Name)?.Wal
                      ?? await ReadWalAsync(cluster, shard.Name, ct);
            if (wal is not null && wal.State != WalStreamStatus.Stopped)
                await status.WriteIfChangedAsync(cluster, shard.Name,
                    wal with { State = WalStreamStatus.Stopped }, ct);
            Observe(cluster, shard.Name, null, null); // STOPPED — серии исчезают
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Стоп одного шарда: QUARANTINED (эвакуация) — STOPPED, ключ жив (демонтаж
    // удалит); docker-вызовов нет (: приёмники замирают по STOPPED-гейту).
    private async Task StopShardAsync(string cluster, string shard, WalStreamState? wal, CancellationToken ct)
    {
        wal ??= await ReadWalAsync(cluster, shard, ct);
        if (wal is not null && wal.State != WalStreamStatus.Stopped)
            await status.WriteIfChangedAsync(cluster, shard,
                wal with { State = WalStreamStatus.Stopped }, ct);
        Observe(cluster, shard, null, null); // STOPPED — серии исчезают
    }

    /// <summary>S3-вызовы этого процесса — с пульсом прогресса (S3Pulse, канон
    ///: прямой вызов на недоступном endpoint висит до HttpClient-таймаута
    /// без отметок — watchdog гасил reconcile посреди wal-контроля (bad-S3 —
    /// штатный контур, transient повторит следующий тик).</summary>
    private async Task<Result<T>> S3PulseAsync<T>(
        Func<CancellationToken, Task<Result<T>>> call, CancellationToken ct)
        => await S3Pulse.CallAsync(progress, call, ct, (watchdogWindow ?? TimeSpan.FromSeconds(15)) / 2);

    // Негенерик-перегрузка: вызовы без значения (PutObjectAsync → Result).
    private async Task<Result> S3PulseAsync(
        Func<CancellationToken, Task<Result>> call, CancellationToken ct)
        => await S3Pulse.CallAsync(progress, call, ct, (watchdogWindow ?? TimeSpan.FromSeconds(15)) / 2);

    // / (arch/19 §3): exhaustion-DEGRADED — DEGRADED-ключ с маркером
    // s3-pool-exhausted в error (исчерпание write-списка мастер-приёмника —
    // флаг pool_exhausted его статус-ключа) — контроль КАЖДЫЙ тик, симметрично
    // WAL_CHAIN_ERROR (скорость заживления; union-листинг — один класс вызовов).
    private static bool IsExhaustionDegraded(WalStreamState? wal)
        => wal is { State: WalStreamStatus.Degraded }
            && wal.Error?.Contains(S3PoolMarkers.Exhausted, StringComparison.Ordinal) == true;

    // TLI из 24-hex имени сегмента (первые 8 символов); битое имя — 0 (текст
    // tli-gap строится только из валидированных имён).
    internal static uint TliOfSegment(string segmentName)
        => WalFileName.TryParse(segmentName) is { } f ? f.Tli : 0;
}
