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

/// <summary>Итог контроля одного прохода (t07): State — СВЕЖЕЕ состояние шарда
/// ПОСЛЕ контроля (записанное контролем или прежнее при «не время»); ChainBroken —
/// вычисляется из State: state=BROKEN (разрыв цепочки/слот — permanent, t07,
/// маркер живёт в etcd-ключе и переживает рестарт/takeover) — агента не поднимать.
/// Transient-деградация (DEGRADED lag/тишина) держит ChainBroken=false — exited-
/// агент пересоздаётся со свежими env (spec §3.2 п.5/п.7; ревью Ф4-2 №1).</summary>
public sealed record ControlOutcome(WalStreamState? State)
{
    public bool ChainBroken => State is { State: WalStreamStatus.Broken };
}

/// <summary>WalStreamProcess — машина одного тика WAL-архивации шардов кластера
/// под клэймом <C> (t03/t27, arch/19 §3): (1) креды/ensure-зависимости, (2) резолв
/// ДВУХ источников (мастер + sync-standby — двойная архивация), (3) ensure слота
/// per-instance (живой слот на любой ноде — не BROKEN), (4–5) per-node контейнеры
/// агентов pgw-backup-wal-&lt;C&gt;-&lt;X&gt;-&lt;N&gt; + супервиз (exited/смена источника →
/// пересоздание; старый одноагентный формат сносится тем же демонтажем — миграция),
/// (6–7) контроль цепочки+lag по расписанию Wal:VerifyIntervalSec, (8) статус etcd
/// с супервиз-фактами agents. runtime() == null (Enabled=false) — стоп-семантика.
/// Ошибка шарда не роняет остальные; guard'ы: Active-кластер, шард с dsn.
/// Идемпотентность каждого шага (arch/17).</summary>
public sealed class WalStreamProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ShardEndpoints shards,
    IWalSqlExecutor sql,
    IBackupS3 s3,
    WalStatusWriter status,
    ClaimStore claims,
    WorkJournal journal,
    Func<BackupsRuntimeOptions?> runtime,
    InstallSecrets secrets,
    TimeProvider clock,
    Action<string, string, long?>? lagObserver = null,
    ILogger? logger = null,
    Action<string, string, long?>? uploadedAgeObserver = null) // t14: uploaded-age (arch/18 §2.7)
{
    private const string Op = "backup-wal";

    // Расписание контроля (list S3 — не каждый тик): ключ cluster/shard → unix последнего прохода.
    // При BROKEN-ключе шарда расписание НЕ действует — контроль каждый тик (t07).
    private readonly ConcurrentDictionary<string, long> _lastVerifyUnix = [];

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант arch/14 §4.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"backup-wal {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        var options = runtime();

        // Стоп-семантика Enabled=false: агенты вниз + финальный STOPPED при живом
        // ключе; неактивная подсистема дальше не идёт.
        if (options is null)
            return await StopAllAsync(cluster, snap, backups, ct);

        // Guard: только Active-кластер (spec §3.2).
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var shardBackups = backups?.Shards
                           ?? (IReadOnlyDictionary<string, ShardBackups>)new Dictionary<string, ShardBackups>();
        foreach (var shard in snap.Shards)
        {
            if (shard.Dsn is null)
                continue; // не поднят — домен AddShard

            try
            {
                await TickShardAsync(cluster, snap, shard, shardBackups.GetValueOrDefault(shard.Name), options, ct);
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

    // Наблюдатели контрольного прохода (t14, arch/18 §2.7): лаг и uploaded-age —
    // один факт одного прохода, одинаковая null-семантика (BROKEN/STOPPED/факта
    // нет — серии исчезают; null идемпотентен).
    private void Observe(string cluster, string shard, long? lagSegments, long? uploadedAgeSec)
    {
        lagObserver?.Invoke(cluster, shard, lagSegments);
        uploadedAgeObserver?.Invoke(cluster, shard, uploadedAgeSec);
    }

    private async Task TickShardAsync(
        string cluster, ClusterSnapshot snap, ShardSpec shard, ShardBackups? shardBackups,
        BackupsRuntimeOptions options, CancellationToken ct)
    {
        // t05 §3.4 гвард: шард с активной restore-заявкой (PLANNED/RUNNING/
        // REJOINING) — WAL-агент не обеспечивается: демонтаж restore его снёс,
        // а поднимать агент на снесённом мастере нельзя (контуры не трогают шард).
        if (shardBackups?.Restores.Any(r => r.State
                is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining) == true)
            return;

        // (1) Креды: /clusters/<C>/backup_password (t02): отсутствует → transient-пропуск
        //     шарда с journal-заметкой — агент не поднимается, ретраи тиками.
        var passwordKv = await GetAsync($"/clusters/{cluster}/backup_password", ct);
        if (passwordKv is not { Value: { Length: > 0 } password })
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-password", claims.InstanceId,
                $"{shard.Name}: нет /clusters/{cluster}/backup_password (t02 не смержена/ensure не прошёл)", ct);
            return;
        }

        // (2) Два источника (t27 §3.3 п.2): мастер + sync-standby (двойная архивация,
        //     оба из portalloc; sync нет → только мастер — второй агент тиком при
        //     появлении; sync == мастер → один источник).
        var addresses = await shards.ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            throw new ApplicationException($"portalloc: {addresses.Error!.Message}");
        var master = await shards.ResolveMasterAsync(cluster, shard, addresses.Value, ct);
        if (!master.IsSuccess)
            throw new ApplicationException($"резолв мастера: {master.Error!.Message}");
        if (master.Value is not { } masterAddr)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-master", claims.InstanceId,
                $"{shard.Name}: мастер не резолвится (failover-окно)", ct);
            return;
        }

        var (masterRef, masterPgHost, masterPgPort) = ResolveNodeRef(shard, addresses.Value, masterAddr);
        var sources = new List<WalSource> { new(masterRef, masterAddr, masterPgHost, masterPgPort) };
        var sync = await shards.ResolveSyncStandbyAsync(cluster, shard, addresses.Value, ct);
        if (!sync.IsSuccess)
            throw new ApplicationException($"резолв sync-standby: {sync.Error!.Message}");
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

        // (3) Слот per-instance (t27 §3.3 п.3; t19 arch/19 §3): имя слота одно и то же
        //     на КАЖДОЙ ноде-источнике (слоты разных инстансов независимы); зонд —
        //     существование И wal_status: alive = существует и НЕ lost. BROKEN —
        //     слоты ВСЕХ источников исчезли ИЛИ потеряны при живой цепочке (живой слот
        //     на любой ноде держит WAL); lost одного при живом втором — авто-recreate
        //     без BROKEN (потерянный агент вернётся от хвоста S3 ретраем приёмника).
        var slotProbes = new List<(WalSource Src, string Dsn, bool Exists, string? WalStatus)>();
        foreach (var src in sources)
        {
            var dsn = ShardEndpoints.AdminDsn(src.Addr, snap.Config.DbName, secrets);
            var probe = await sql.SlotProbeAsync(dsn, slot, ct);
            if (!probe.IsSuccess)
                throw new ApplicationException($"слот-зонд {slot}@{src.Node}: {probe.Error!.Message}");
            slotProbes.Add((src, dsn, probe.Value.Exists, probe.Value.WalStatus));
        }
        // null-статус существующего слота (старые PG/edge) — живой: лечим только явный lost
        static bool Alive((WalSource Src, string Dsn, bool Exists, string? WalStatus) p)
            => p.Exists && p.WalStatus != "lost";

        if (slotProbes.All(p => !Alive(p)))
        {
            if (chainKnown && wal!.State is WalStreamStatus.Active or WalStreamStatus.Degraded)
            {
                // t13 (arch/19 §3): инвариант писателя (last_uploaded_unix в живом
                // ACTIVE/DEGRADED-ключе) может быть нарушен (ручная правка ключа/
                // будущий писатель при ослабленном гварде парсера — прецедент
                // ed1561b): битый ключ не роняет тик форс-мьютом. Фолбэк
                // clock-сейчас — defensive-значение ТОЛЬКО параметра baseUnix:
                // BreakAsync потребляет его лишь при wal == null (запись с нуля),
                // здесь wal != null (chainKnown) — в BROKEN-запись now() НЕ попадает
                // («факт над записью», ревью Ф4-2 №2); факт битого ключа — в журнале.
                if (wal.LastUploadedUnix is null)
                {
                    var invalid = $"last_uploaded_unix отсутствует — битый ключ /pgworker/backups/{cluster}/{shard.Name}/wal "
                        + "(ручная правка/иной писатель); ветка «слот исчез» идёт с фолбэком времени";
                    logger?.LogWarning("backup-wal {Cluster}/{Shard}: {Message}", cluster, shard.Name, invalid);
                    await journal.WritePhaseAsync(cluster, Op, $"wal-key-invalid/{shard.Name}",
                        claims.InstanceId, invalid, ct);
                }

                // BROKEN + слот пересоздаётся immediate+reserved СРАЗУ (t07); error
                // различает «исчез» / «потерян (lost)» по факту зонда (t19)
                var anyExists = slotProbes.Any(p => p.Exists);
                await BreakAsync(cluster, shard.Name, wal, slot, masterRef, adminDsn, ct,
                    baseStart: wal.LastUploadedSegment is { Length: > 0 }
                        ? wal.LastUploadedSegment : wal.ChainStartSegment,
                    baseLast: wal.LastUploadedSegment,
                    baseUnix: wal.LastUploadedUnix ?? clock.GetUtcNow().ToUnixTimeSeconds(),
                    error: anyExists
                        ? $"слот {slot} потерян (wal_status=lost) на всех источниках при живой цепочке — пересними полный бэкап"
                        : $"слот {slot} исчез при живой цепочке (инвалидация max_slot_wal_keep_size?) — пересними полный бэкап",
                    recreateSlot: true);
                return;
            }

            // Первый старт ИЛИ BROKEN-ключ: ensure-alive на КАЖДОМ источнике
            // (отсутствует → create; lost → recreate — пустой префикс wal/ + lost лечится
            // recreate без BROKEN, свежий слот держит текущую позицию).
            foreach (var dead in slotProbes)
            {
                var ensured = await sql.EnsureSlotAliveAsync(dead.Dsn, slot, ct);
                if (!ensured.IsSuccess)
                    throw new ApplicationException($"ensure слота {slot}@{dead.Src.Node}: {ensured.Error!.Message}");
            }
        }
        else
        {
            // lost при живом втором источнике — НЕ BROKEN: recreate потерянного
            // (journal-before-manipulations, arch/17: фаза ДО мутации — видимость
            // действия при падении между drop и create; тик повторит).
            foreach (var lost in slotProbes.Where(p => p.Exists && p.WalStatus == "lost"))
            {
                await journal.WritePhaseAsync(cluster, Op, $"slot-recreate/{shard.Name}", claims.InstanceId,
                    $"{lost.Src.Node}: слот {slot} wal_status=lost — слот пересоздан, агент вернётся от хвоста S3", ct);
                var recreated = await sql.RecreateSlotAsync(lost.Dsn, slot, ct);
                if (!recreated.IsSuccess)
                    throw new ApplicationException($"recreate слота {slot}@{lost.Src.Node}: {recreated.Error!.Message}");
            }

            // отсутствующие на живых источниках → create (ensure-идемпотентность, t27)
            foreach (var missing in slotProbes.Where(p => !p.Exists))
            {
                var created = await sql.EnsureSlotAliveAsync(missing.Dsn, slot, ct);
                if (!created.IsSuccess)
                    throw new ApplicationException($"ensure слота {slot}@{missing.Src.Node}: {created.Error!.Message}");
            }
        }

        // (6–7) Контроль — ДО ensure агента. ControlOutcome: State — свежее состояние
        // ПОСЛЕ контроля (восстановление ACTIVE новым полным + подъём агента ОДНИМ
        // тиком, AC4c); ChainBroken (= State BROKEN) — агента НЕ поднимаем (t07:
        // решение — чтение ключа wal.State == BROKEN, переживает рестарт/takeover,
        // spec §3.2); transient-DEGRADED (lag/тишина) — НЕ блокирует супервиз
        // (exited-агент пересоздаётся, spec §3.2 п.5; ревью Ф4-2 №1).
        var controlled = await ControlDueAsync(
            cluster, shard.Name, wal, shardBackups, options, masterRef, slot, adminDsn, ct);

        // (4–5) Per-node агенты + супервиз.
        if (!controlled.ChainBroken)
        {
            var shardNodes = addresses.Value.Keys
                .Where(k => k.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
                .Select(k => k.Split('/')[1])
                .ToList();
            await EnsureAgentsAsync(cluster, shard.Name, sources, shardNodes, options, slot,
                password, controlled.State, ct);
        }
    }

    /// <summary>Источник WAL-архивации (t27): нода-владелец агента (имя из
    /// portalloc), её адрес (docker-хост для контейнера) и conninfo-пара.</summary>
    private sealed record WalSource(string Node, NodeAddress Addr, string PgHost, int PgPort);

    // (4–5) Per-node контейнеры агентов (t27 §3.3 п.5): desired-имена по
    // источникам (мастер [+ sync]); супервиз: живой running с нужным именем →
    // пропуск; иначе (нет/exited/чужое имя — смена источника, старый одноагентный
    // формат) → демонтаж ВСЕХ агентов шарда (сносит и старый формат с его
    // staging-томом — миграция §3.6 тем же механизмом) → создание всех desired.
    // Секреты — env контейнера (§3.1, без argv); restart-политика no (exited —
    // только permanent, супервиз тика пересоздаёт со свежими env).
    private async Task EnsureAgentsAsync(
        string cluster, string shard, IReadOnlyList<WalSource> sources,
        IReadOnlyList<string> shardNodes, BackupsRuntimeOptions options, string slot,
        string password, WalStreamState? wal, CancellationToken ct)
    {
        var listed = await driver.ListBackupAgentsAsync(cluster, ct);
        if (!listed.IsSuccess)
            throw new ApplicationException($"лист агентов: {listed.Error!.Message}");

        var desired = sources
            .Select(src => (Src: src, Name: BackupAgentNames.Container(cluster, shard, src.Node)))
            .ToList();
        // Канон движка — имена контейнеров БЕЗ ведущего "/" (ListContainersAsync
        // trimит); матч обоих форматов («/»-литерал — устаревший формат движков).
        static bool RunningOn(IReadOnlyList<DockerContainer> listed, string name)
            => listed.Any(c => c.State == "running"
                && (c.Names.Contains(name) || c.Names.Contains("/" + name)));

        // Разбор листинга: чужие имена (старый одноагентный формат §3.6, имена
        // сменившихся источников) — снос ВСЕХ агентов шарда и создание всех desired;
        // absent-desired (агента никогда не было — появился sync) — создание ТОЛЬКО
        // недостающих (живые источники не трогаются — двойная архивация не рвётся);
        // все running — skip (супервиз-безделье).
        var listedNames = listed.Value
            .SelectMany(c => c.Names)
            .Select(n => n.TrimStart('/'))
            .Where(n => n.StartsWith(BackupAgentNames.Prefix(cluster), StringComparison.Ordinal))
            .Distinct()
            .ToList();
        var desiredNames = desired.Select(d => d.Name).ToList();
        // Чужие — ТОЛЬКО агенты СВОЕГО шарда не из desired (старый одноагентный
        // формат §3.6, имена сменившихся источников): иначе агент СОСЕДНЕГО
        // шарда кластера (листинг по кластеру) триггерил вечный recreate-цикл
        // этого шарда — E2E-факт t27 (AC3: shard2-агент в листинге кластера).
        var agentPrefix = BackupAgentNames.Prefix(cluster);
        var foreign = listedNames
            .Where(n => !desiredNames.Contains(n, StringComparer.Ordinal))
            .Where(n => ShardOfListedName(n, agentPrefix) == shard)
            .ToList();
        var missing = desired.Where(d => !RunningOn(listed.Value, d.Name)).ToList();
        var recreateAll = foreign.Count > 0
            || missing.Any(m => listedNames.Contains(m.Name, StringComparer.Ordinal)); // exited-контейнер

        var toEnsure = recreateAll ? desired : missing;
        if (recreateAll)
        {
            logger?.LogInformation("backup-wal: агенты {Shard} — пересоздание per-node ({Desired})",
                shard, string.Join(",", desired.Select(d => d.Name)));
            // Снос ВСЕХ агентов шарда: старый одноагентный формат (без суффикса
            // ноды) и его -staging-том сносятся тем же демонтажем (миграция §3.6).
            var removed = await driver.RemoveBackupAgentsAsync(cluster, shard, ct);
            if (!removed.IsSuccess)
                throw new ApplicationException($"демонтаж агентов: {removed.Error!.Message}");
        }

        if (toEnsure.Count > 0)
        {
            foreach (var d in toEnsure)
            {
                var spec = new ContainerSpec(
                    Image: options.WalAgentImage, // t27: образ приёмника pgworker-wal
                    Env: (IReadOnlyDictionary<string, string>)AgentEnv(
                        options, cluster, shard, slot, d.Src.PgHost, d.Src.PgPort, password),
                    VolumeName: null,   // t27: staging-том упразднён — буфер в памяти приёмника
                    VolumeDest: null,
                    Ports: [],
                    Hostname: d.Name,
                    CpuCores: options.AgentCpu,
                    MemoryBytes: options.AgentMem,
                    LabelKey: "pgworker",
                    Label: cluster,
                    ResetEntrypoint: false, // ENTRYPOINT образа приёмника (§3.2)
                    Cmd: null,
                    Network: null, // сеть назначает драйвер (per-cluster pgw-net-<C>)
                    NetworkAliases: null,
                    RestartPolicy: "no");

                // Хост агента = docker-хост своего источника (per-cluster сеть на нём).
                var ensured = await driver.EnsureBackupAgentAsync(
                    cluster, shard, d.Src.Node, spec, d.Src.Addr.Host, ct);
                if (!ensured.IsSuccess)
                    throw new ApplicationException($"подъём агента {d.Name}: {ensured.Error!.Message}");
            }

            logger?.LogInformation("backup-wal: агенты {Shard} подняты (слот {Slot})",
                shard, slot);
        }

        // agents-факты (t27 §3.5, arch/19 §4): срез состояния ПОСЛЕ супервиза
        // ЭТОГО тика (relist): факт = состояние контейнера на момент листинга.
        // exited достижим, только если свежеподнятый контейнер успел уйти
        // permanent-кодом до relist — супервиз пересоздаст его следующим
        // тиком; ноды portalloc шарда вне листинга — absent (неполнота двойной
        // архивации, spec §5). Пишем только при живом wal-ключе — факты
        // ДОБАВКА к контролю, ключ создаёт контроль (spec п.8); WriteIfChanged
        // — идемпотентно. Перечитка упала — пропускаем (напишет следующий тик).
        if (wal is not null && shardNodes.Count > 0)
        {
            var relisted = await driver.ListBackupAgentsAsync(cluster, ct);
            if (relisted.IsSuccess)
            {
                var facts = BuildAgentFacts(cluster, shard, shardNodes, relisted.Value);
                await status.WriteIfChangedAsync(cluster, shard, wal with { Agents = facts }, ct);
            }
        }
    }

    /// <summary>Факты агентов из листинга драйвера (контракт arch/19 §4):
    /// контейнер ноды running → Running, exited → Exited, нет контейнера →
    /// Absent. Матч имени — per-node формат («/»-литерал старых движков).</summary>
    internal static IReadOnlyList<WalAgentState> BuildAgentFacts(
        string cluster, string shard,
        IReadOnlyList<string> shardNodes,
        IReadOnlyList<Shared.Docker.DockerContainer> listed)
        => shardNodes
            .Select(n =>
            {
                var name = BackupAgentNames.Container(cluster, shard, n);
                var state = listed.FirstOrDefault(c =>
                    c.Names.Contains(name) || c.Names.Contains("/" + name));
                return new WalAgentState(n,
                    state is null ? WalAgentPresence.Absent
                    : state.State == "running" ? WalAgentPresence.Running
                    : WalAgentPresence.Exited);
            })
            .ToList();

    // pgw-backup-wal-<C>-<ХВОСТ> → <X>: хвост == <X> (старый формат) или <X>-<нода>
    // (per-node) — зеркально ClusterDriver.AgentShardOf (матчинг одного правила).
    private static string ShardOfListedName(string containerName, string prefix)
    {
        var tail = containerName[prefix.Length..];
        return tail.Split('-')[0];
    }

    // env контейнера агента-приёмника (t27 §3.1): СЕКРЕТЫ — только env, в argv/логи
    // не попадают; приёмник читает Environment.GetEnvironmentVariable (WalReceiverEnv).
    private static IReadOnlyDictionary<string, string> AgentEnv(
        BackupsRuntimeOptions options, string cluster, string shard, string slot,
        string pgHost, int pgPort, string password)
    {
        var env = new Dictionary<string, string>
        {
            ["PG_HOST"] = pgHost,
            ["PG_PORT"] = pgPort.ToString(),
            ["PG_USER"] = "backup_exec",
            ["PG_PASSWORD"] = password,
            ["PG_DBNAME"] = "postgres",
            ["SLOT"] = slot,
            ["CLUSTER"] = cluster,
            ["SHARD"] = shard,
            ["S3_ENDPOINT"] = options.AgentS3Endpoint,
            ["S3_BUCKET"] = options.S3Bucket,
            ["S3_ACCESS_KEY"] = options.S3AccessKey,
            ["S3_SECRET_KEY"] = options.S3SecretKey,
            ["S3_PATHSTYLE"] = options.S3PathStyle ? "true" : "false",
        };
        if (options.S3Region is { Length: > 0 } region)
            env["S3_REGION"] = region;
        return env;
    }

    // nodeRef + conninfo-пара ноды-источника: каноническая нода → alias :5432 в
    // сети нод; усыновлённая (object) → host:pg-port из portalloc (arch/19 §3).
    // DRY: общий для мастера и sync (t27).
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
            throw new ApplicationException($"чтение ключа wal: {read.Error!.Message}");
        return read.Value;
    }

    // Точечный GET с failover (паттерн ShardEndpoints.GetAsync); ошибка транспорта —
    // исключение (поймается пер-шардовой обёрткой → журнал), null = ключа нет.
    private async Task<Kv?> GetAsync(string key, CancellationToken ct)
    {
        Result<Kv?>? last = null;
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

        if (last is not null)
            throw new ApplicationException($"get {key}: {last.Error!.Message}");
        return null;
    }

    // (6–7) Контроль (spec §3.2 п.6–8; t07 arch/19 §3): при BROKEN-ключе — КАЖДЫЙ
    // тик (без VerifyIntervalSec-расчёта: скорость заживления; list дырного
    // префикса дёшев); иначе по расписанию. list S3 → chain_start (t18:
    // wal_start новейшего verify-OK COMPLETED-полного, ratchet — не понижается
    // ?? min-объект)
    // → CheckChain → дыра: BROKEN + ОСТАНОВ агента (в т.ч. без прошлого ключа —
    // AC4-тотальность); факты прогресса — ТОЛЬКО из наблюдений: last_uploaded из
    // S3-объектов либо прошлого ключа, никогда от now() («факт над записью»,
    // ревью Ф4-2 №2); lag-зонд; transient-деградации (lag/тишина) агент не
    // останавливают и супервиз не блокируют (ChainBroken=false).
    private async Task<ControlOutcome> ControlDueAsync(
        string cluster, string shard, WalStreamState? wal, ShardBackups? shardBackups,
        BackupsRuntimeOptions options, string masterRef, string slot, string adminDsn,
        CancellationToken ct)
    {
        var key = $"{cluster}/{shard}";
        var now = clock.GetUtcNow().ToUnixTimeSeconds();
        // Расписание — пропуск только для НЕ-BROKEN (t07: BROKEN контролирует
        // каждый тик — заживление одним тиком после пересъёма полного).
        if (wal is not { State: WalStreamStatus.Broken }
            && _lastVerifyUnix.TryGetValue(key, out var last)
            && now - last < options.WalVerifyIntervalSec)
            return new ControlOutcome(wal);
        // (листим S3 и до создания ключа каждый тик — скорость первого наблюдения,
        // пустой префикс дёшев; подтверждено ревью Ф4-2 как допустимое)

        _lastVerifyUnix[key] = now;

        // Истина прогресса — объекты S3 (arch/19 §3): list префикса wal/.
        var listed = await s3.ListWalAsync(cluster, shard, ct: ct);
        if (!listed.IsSuccess)
            throw new ApplicationException($"list S3 {cluster}/{shard}/wal: {listed.Error!.Message}");
        var objects = listed.Value;

        // chain_start (t18, arch/19 §3): wal_start новейшего (по started_unix)
        // COMPLETED-полного с verify.state=OK — ОДНА точка с cutoff-ретенции
        // (§4 п.5; расщепление точек давало бы ложный BROKEN после среза WAL);
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
            Observe(cluster, shard, null, null); // t14: факта нет — серии исчезают
            return new ControlOutcome(wal);
        }

        // CheckWithRestart: после restore promote открывает новый TLI, старые
        // сегменты обрезаны легитимно (AC4) — дыра на TLI-границе не деградация
        var chain = WalChain.CheckWithRestart(start, objects.Select(o => o.Name));

        // History-fallback (t27 Task 13, канон §3): приёмник не смог запросить
        // TIMELINE_HISTORY во время стрима (replication-команды после
        // START_REPLICATION сервером не обслуживаются — E2E-факт) — контроль
        // докладывает недостающий .history docker-exec'ом ноды при TLI-переходе.
        await DeliverMissingHistoriesAsync(cluster, shard, objects, masterRef, ct);

        if (!chain.IsContinuous)
        {
            // Дыра: BROKEN даже без прошлого ключа (t07, AC4-тотальность) —
            // BreakAsync строит запись с границей разрыва и останавливает агента.
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
        // защитная (ревью t04 P2): при его поломке смешения фактов не будет —
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
            Observe(cluster, shard, null, null); // t14: наблюдения нет — серии исчезают
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

        Observe(cluster, shard, lag, now - lastUploadedUnix.Value); // t14: факт прохода

        // Деградации transient-природы (lag/тишина): агент НЕ останавливаем и
        // супервиз НЕ блокируем (ChainBroken=false — ретраи тиками; exited-агент
        // пересоздаётся со свежими env, spec §3.2 п.5/п.7; ревью Ф4-2 №1).
        string? error = null;
        var state = WalStreamStatus.Active;
        if (lag > options.WalLagMaxSegments) // null → сравнение false (зонд не удался — не деградация)
        {
            state = WalStreamStatus.Degraded;
            error = $"отставание WAL-потока {lag} сегментов (порог {options.WalLagMaxSegments})";
        }
        else if (now - lastUploadedUnix > options.WalStaleSec)
        {
            state = WalStreamStatus.Degraded;
            error = $"тишина загрузок {now - lastUploadedUnix} c (порог {options.WalStaleSec})";
        }

        var next = new WalStreamState(
            state, slot, masterRef, start.Name, lastUploadedName, lastUploadedName,
            lastUploadedUnix, lag, error);
        await status.WriteIfChangedAsync(cluster, shard, next, ct);
        return new ControlOutcome(next); // цепочка цела (ACTIVE/DEGRADED) — супервиз разрешён
    }

    /// <summary>Доклад недостающих .history (t27 Task 13, канон §3): TLI сегментов
    /// из objects, каждый tli > chainStart.Tli без wal/&lt;tli&gt;.history в списке —
    /// docker-exec ноды источника (base64 — бинарная безопасность), decode → put
    /// wal/&lt;tli:x8&gt;.history (sha256). Exec-сбой/файла нет — transient (следующий
    /// контроль повторит; счётчик не вводим — YAGNI). Источник — мастер (жив по
    /// определению резолва).</summary>
    private async Task DeliverMissingHistoriesAsync(
        string cluster, string shard, IReadOnlyList<WalObject> objects,
        string masterRef, CancellationToken ct)
    {
        var listedNames = objects.Select(o => o.Name).ToHashSet(StringComparer.Ordinal);
        if (listedNames.Count == 0)
            return; // сегментов нет — докладывать нечего
        // History нужен для КАЖДОГО TLI-перехода: все TLI набора > 1 без
        // wal/<tli>.history в списке. НЕ «кроме минимального»: history-файл не
        // бывает только у TLI 1; если минимальный TLI набора > 1 (контур
        // стартовал уже на новом таймлайне — приёмники донесли сегменты без
        // предыстории, E2E-факт t27 AC5), склейка без его history невозможна.
        var missingTlis = objects
            .Select(o => WalFileName.TryParse(o.Name))
            .OfType<WalFileName>()
            .Where(f => f.Tli > 1)
            .Select(f => f.Tli)
            .Distinct()
            .Where(tli => !listedNames.Contains($"{tli:x8}.history"))
            .ToList();
        if (missingTlis.Count == 0)
            return;

        foreach (var tli in missingTlis)
        {
            var fileName = $"{tli:x8}.history";
            // Путь pg_wal в Spilo-нодах: USE_DATA_DIR_FOR_WAL=true (NodeConfigBuilders,
            // arch/14 §2.1) — pg_wal ВНУТРИ data-каталога:
            // /home/postgres/pgdata/pgroot/data/pg_wal; при отсутствии файла stdout:
            // маркер + листинг pgroot (фактическая структура в журнал).
            var exec = await driver.ExecNodeAsync(cluster, shard, masterRef,
            [
                "sh", "-c",
                $"f=/home/postgres/pgdata/pgroot/data/pg_wal/{fileName}; " +
                "[ -f \"$f\" ] && base64 -w0 \"$f\" && exit 0; " +
                "echo FILE_NOT_FOUND; ls -la /home/postgres/pgdata/pgroot/data/ 2>&1 | tail -12",
            ], ct);
            if (!exec.IsSuccess)
            {
                // нода недоступна — transient: контроль повторит (YAGNI-счётчик)
                logger?.LogInformation("backup-wal: history {File} недоступна на {Node}: {Error} — повтор следующим контролем",
                    fileName, masterRef, exec.Error!.Message);
                continue;
            }

            if (exec.Value.Contains("FILE_NOT_FOUND", StringComparison.Ordinal))
            {
                // pg_wal ноды не содержит файла (промоут мог ещё не написать/нода старая):
                // transient — контроль повторит; фактическая структура в журнале ниже.
                logger?.LogInformation("backup-wal: history {File} не найдена на {Node}: {Listing}",
                    fileName, masterRef, exec.Value);
                continue;
            }

            byte[] content;
            try
            {
                content = Convert.FromBase64String(exec.Value.Trim());
            }
            catch (FormatException e)
            {
                throw new ApplicationException($"history {fileName}: битый base64 от ноды: {e.Message}: " +
                    $"raw[{exec.Value.Trim()[..Math.Min(120, exec.Value.Trim().Length)]}]", e);
            }

            var sha256 = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(content));
            var put = await s3.PutObjectAsync($"{cluster}/{shard}/wal/{fileName}", content, sha256, ct);
            if (!put.IsSuccess)
                throw new ApplicationException($"put history {fileName}: {put.Error!.Message}");
            logger?.LogInformation("backup-wal: history {File} доложена контролем ({Bytes} байт)",
                fileName, content.Length);
        }
    }

    // Разрыв цепочки (t07, arch/19 §3): BROKEN + error + граница разрыва в
    // chain_start + ОСТАНОВ агента (идемпотентно). Маркер разрыва — ключ etcd
    // (не память): переживает рестарт воркера/takeover; заживление — контроль
    // (COMPLETED-полный ≥ границы с непрерывной цепью → ACTIVE, агент тем же
    // тиком). recreateSlot: слот при «слот исчез» пересоздаётся immediate+
    // reserved СРАЗУ — к старту пересъёма полного слот держит позицию ≤ wal_start
    // нового полного; при дыре цепочки слот НЕ трогаем (живой копит WAL в
    // пределах max_slot_wal_keep_size). wal == null — запись создаётся с
    // наблюдаемыми фактами контроля (AC4-тотальность: дыра найдена при первом
    // наблюдении цепочки). Возвращает записанное состояние (AC4c одним тиком).
    // Инвариант baseUnix (t13): параметр потребляется ТОЛЬКО при wal == null
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
        var removed = await driver.RemoveBackupAgentsAsync(cluster, shard, ct);
        if (!removed.IsSuccess)
            throw new ApplicationException($"стоп агента {shard}: {removed.Error!.Message}");
        if (recreateSlot)
        {
            // Ошибка ensure — исключение наверх (пер-шардовый catch → журнал;
            // следующий тик повторит — идемпотентно).
            var ensured = await sql.EnsureSlotAliveAsync(adminDsn, slot, ct);
            if (!ensured.IsSuccess)
                throw new ApplicationException($"ensure слота {slot}: {ensured.Error!.Message}");
        }
        var current = wal ?? new WalStreamState(
            WalStreamStatus.Active, slot, masterRef, baseStart, baseLast, baseLast, baseUnix, null, null);
        var broken = current with
        {
            State = WalStreamStatus.Broken,
            ChainStartSegment = baseStart, // граница разрыва (t07, ratchet §3)
            Error = error,
        };
        await status.WriteIfChangedAsync(cluster, shard, broken, ct);
        Observe(cluster, shard, null, null); // t14: BROKEN — серии исчезают
        return broken;
    }

    // Стоп-семантика Enabled=false (spec §3.2 «Стоп-семантика»): агенты кластера
    // вниз (идемпотентно); при живом ключе шарда — финальный STOPPED (последнее
    // касание, иначе застывший ACTIVE кормит ложный wal-stream-lag).
    private async Task<Result<ProcessOutcome>> StopAllAsync(
        string cluster, ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct)
    {
        var removed = await driver.RemoveBackupAgentsAsync(cluster, shard: null, ct);
        if (!removed.IsSuccess)
            return Result<ProcessOutcome>.Failed(removed.Error!);

        foreach (var shard in snap.Shards)
        {
            var wal = backups?.Shards.GetValueOrDefault(shard.Name)?.Wal
                      ?? await ReadWalAsync(cluster, shard.Name, ct);
            if (wal is not null && wal.State != WalStreamStatus.Stopped)
                await status.WriteIfChangedAsync(cluster, shard.Name,
                    wal with { State = WalStreamStatus.Stopped }, ct);
            Observe(cluster, shard.Name, null, null); // t14: STOPPED — серии исчезают
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Стоп одного шарда: QUARANTINED (эвакуация) — STOPPED, ключ жив (демонтаж удалит).
    private async Task StopShardAsync(string cluster, string shard, WalStreamState? wal, CancellationToken ct)
    {
        var removed = await driver.RemoveBackupAgentsAsync(cluster, shard, ct);
        if (!removed.IsSuccess)
            throw new ApplicationException($"стоп агента {shard}: {removed.Error!.Message}");
        wal ??= await ReadWalAsync(cluster, shard, ct);
        if (wal is not null && wal.State != WalStreamStatus.Stopped)
            await status.WriteIfChangedAsync(cluster, shard,
                wal with { State = WalStreamStatus.Stopped }, ct);
        Observe(cluster, shard, null, null); // t14: STOPPED — серии исчезают
    }
}
