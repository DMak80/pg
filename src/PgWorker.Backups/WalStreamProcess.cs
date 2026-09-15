using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgWorker.Backups.Sql;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
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
/// под клэймом <C> (t03, arch/19 §3): (1) креды/ensure-зависимости, (2) резолв
/// мастера, (3) ensure слота, (4) контейнер агента, (5) супервиз (exited/смена
/// мастера → пересоздание), (6–7) контроль цепочки+lag по расписанию
/// Wal:VerifyIntervalSec, (8) статус etcd. runtime() == null (Enabled=false) —
/// стоп-семантика. Ошибка шарда не роняет остальные; guard'ы: Active-кластер,
/// шард с dsn. Идемпотентность каждого шага (arch/17).</summary>
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
    ILogger? logger = null)
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

        // (2) Резолв мастера: недоступен → transient-пропуск (failover-окно, статус не деградирует).
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

        // conninfo-пара для агента + masterRef для сверки смены (arch/19 §3 «Подключение»):
        // каноническая нода — alias сети pgw-net :5432; усыновлённая (object) — host:pg-port.
        var (masterRef, pgHost, pgPort) = ResolveMasterRef(shard, addresses.Value, masterAddr);

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

        // (3) Ensure слота (spec §3.2 п.3; t07 arch/19 §3): есть → пропуск; нет при
        //     живой (ACTIVE/DEGRADED) записи о цепочке → инвалидация (BROKEN + стоп
        //     агента + слот пересоздаётся immediate+reserved сразу — держит позицию
        //     ≤ wal_start будущего переснятого полного); нет при BROKEN → просто
        //     ensure (повторный Break не нужен); нет и цепочки нет (первый старт) →
        //     create immediate+reserved.
        var slotExists = await sql.SlotExistsAsync(adminDsn, slot, ct);
        if (!slotExists.IsSuccess)
            throw new ApplicationException($"слот-зонд {slot}: {slotExists.Error!.Message}");
        if (!slotExists.Value)
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

                // BROKEN + слот пересоздаётся immediate+reserved СРАЗУ (t07, spec §3.2):
                // к старту пересъёма полного слот уже держит позицию ≤ wal_start нового.
                await BreakAsync(cluster, shard.Name, wal, slot, masterRef, adminDsn, ct,
                    baseStart: wal.LastUploadedSegment is { Length: > 0 }
                        ? wal.LastUploadedSegment : wal.ChainStartSegment,
                    baseLast: wal.LastUploadedSegment,
                    baseUnix: wal.LastUploadedUnix ?? clock.GetUtcNow().ToUnixTimeSeconds(),
                    error: $"слот {slot} исчез при живой цепочке (инвалидация max_slot_wal_keep_size?) — пересними полный бэкап",
                    recreateSlot: true);
                return;
            }

            // Первый старт ИЛИ BROKEN-ключ (слот ensure: жив не трогаем — выше;
            // исчез — создаём).
            var created = await sql.EnsureSlotAsync(adminDsn, slot, ct);
            if (!created.IsSuccess)
                throw new ApplicationException($"ensure слота {slot}: {created.Error!.Message}");
        }

        // (6–7) Контроль — ДО ensure агента. ControlOutcome: State — свежее состояние
        // ПОСЛЕ контроля (восстановление ACTIVE новым полным + подъём агента ОДНИМ
        // тиком, AC4c); ChainBroken (= State BROKEN) — агента НЕ поднимаем (t07:
        // решение — чтение ключа wal.State == BROKEN, переживает рестарт/takeover,
        // spec §3.2); transient-DEGRADED (lag/тишина) — НЕ блокирует супервиз
        // (exited-агент пересоздаётся, spec §3.2 п.5; ревью Ф4-2 №1).
        var controlled = await ControlDueAsync(
            cluster, shard.Name, wal, shardBackups, options, masterRef, slot, adminDsn, ct);

        // (4–5) Агент + супервиз.
        if (!controlled.ChainBroken)
            await EnsureAgentAsync(cluster, shard.Name, options, slot, masterRef, pgHost, pgPort,
                password, masterAddr.Host, controlled.State, ct);
    }

    // (4–5) Контейнер агента: создание идемпотентно (по образцу EnsureNode, без
    // портов); супервиз: running → пропуск; exited → пересоздание (цикл
    // пересоздания: устаревшие креды/staging переполнен — включая transient-DEGRADED
    // статуса: деградация тишины НЕ запирает restart-контур, ревью Ф4-2 №1);
    // смена мастера (резолв ≠ master_node статуса) → пересоздание с нового мастера.
    private async Task EnsureAgentAsync(
        string cluster, string shard, BackupsRuntimeOptions options, string slot,
        string masterRef, string pgHost, int pgPort, string password, string agentHost,
        WalStreamState? wal, CancellationToken ct)
    {
        var listed = await driver.ListBackupAgentsAsync(cluster, ct);
        if (!listed.IsSuccess)
            throw new ApplicationException($"лист агентов: {listed.Error!.Message}");
        var agentName = BackupAgentNames.Container(cluster, shard);
        // Канон движка — имена контейнеров БЕЗ ведущего "/" (ListContainersAsync
        // trimит); матч обоих форматов: «/»-литерал — устаревший StubDriver-формат
        // (t05-регресс 2026-09-13: существующий агент не находился — супервиз
        // трактовал exited-агента как отсутствующий и не пересоздавал его).
        var existing = listed.Value.FirstOrDefault(c =>
            c.Names.Contains(agentName) || c.Names.Contains("/" + agentName));

        // Смена мастера: резолв разошёлся со статусом → пересоздание с нового мастера.
        var masterChanged = wal is not null && wal.MasterNode != masterRef;
        if (existing is { State: "running" } && !masterChanged)
            return; // жив и на месте — docker unless-stopped держит процесс

        if (existing is not null)
        {
            logger?.LogInformation("backup-wal: агент {Agent} {Reason} — пересоздание",
                agentName, existing.State != "running" ? $"exited({existing.State})" : "смена мастера");
            var removed = await driver.RemoveBackupAgentsAsync(cluster, shard, ct);
            if (!removed.IsSuccess)
                throw new ApplicationException($"демонтаж агента: {removed.Error!.Message}");
        }

        var spec = new ContainerSpec(
            Image: options.JobImage, // общий образ джобов t02 и агентов t03 (arch/19 §2)
            Env: (IReadOnlyDictionary<string, string>)AgentEnv(options, cluster, shard, slot, pgHost, pgPort, password),
            VolumeName: BackupAgentNames.Volume(cluster, shard),
            VolumeDest: options.StagingDir,
            Ports: [],
            Hostname: agentName,
            CpuCores: options.AgentCpu,
            MemoryBytes: options.AgentMem,
            Label: cluster,
            Cmd: WalAgentCommand.Build(),
            Network: null, // сеть назначает драйвер (pgw-net)
            NetworkAliases: null,
            // Без рестарт-политики: обрыв pg_receivewal внутри контейнера
            // переживается скриптом (loop-переподключение, arch/19 §3); exited —
            // только неисправимое (квота staging) — супервиз тика пересоздаёт
            // агента со свежими env за ScanIntervalSec
            RestartPolicy: "no");

        // Хост агента = docker-хост мастера (per-cluster сеть живёт на нём).
        var ensured = await driver.EnsureBackupAgentAsync(cluster, shard, spec, agentHost, ct);
        if (!ensured.IsSuccess)
            throw new ApplicationException($"подъём агента: {ensured.Error!.Message}");
        logger?.LogInformation("backup-wal: агент {Agent} поднят (слот {Slot}, мастер {Master})",
            agentName, slot, masterRef);
    }

    // env контейнера агента: СЕКРЕТЫ — env, не строка команды (arch/19 §7):
    // PG-параметры по-переменно; S3-креды — ОДНОЙ строкой MC_HOST (mc резолвит
    // alias из env; секреты не попадают в argv процессов — ревью Ф4-2 №5).
    private static IReadOnlyDictionary<string, string> AgentEnv(
        BackupsRuntimeOptions options, string cluster, string shard, string slot,
        string pgHost, int pgPort, string password) => new Dictionary<string, string>
    {
        [WalAgentCommand.EnvMcHostVariable] = WalAgentCommand.McHost(
            options.AgentS3Endpoint, options.S3AccessKey, options.S3SecretKey),
        [WalAgentCommand.EnvS3Bucket] = options.S3Bucket,
        [WalAgentCommand.EnvCluster] = cluster,
        [WalAgentCommand.EnvShard] = shard,
        [WalAgentCommand.EnvSlot] = slot,
        [WalAgentCommand.EnvPgHost] = pgHost,
        [WalAgentCommand.EnvPgPort] = pgPort.ToString(),
        [WalAgentCommand.EnvPgUser] = "backup_exec",
        [WalAgentCommand.EnvPgPassword] = password,
        [WalAgentCommand.EnvPgDbName] = "postgres",
        [WalAgentCommand.EnvStagingDir] = options.StagingDir,
        [WalAgentCommand.EnvStagingQuotaBytes] = options.StagingQuotaBytes?.ToString() ?? "",
        [WalAgentCommand.EnvPollSec] = "5",
    };

    // masterRef + conninfo-пара: каноническая нода → alias :5432 в сети нод;
    // усыновлённая (object) → host:pg-port из portalloc (arch/19 §3 «Подключение»).
    private (string MasterRef, string PgHost, int PgPort) ResolveMasterRef(
        ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, NodeAddress master)
    {
        foreach (var node in shard.Nodes)
            if (addresses.TryGetValue($"{shard.Name}/{node.Name}", out var addr)
                && addr.Host == master.Host
                && addr.Ports == master.Ports)
                return (node.Name, node.Name, 5432); // alias сети нод
        return (master.Object ?? $"{master.Host}:{master.Ports.Pg}", master.Host, master.Ports.Pg);
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
    // префикса дёшев); иначе по расписанию. list S3 → chain_start (ratchet:
    // min wal_start COMPLETED-полных ≥ записанной границы ?? записанная ?? min-объект)
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

        // chain_start (п.8; t07 ratchet): min wal_start COMPLETED-полных со стартом
        // ≥ записанного chain_start (полные ниже границы разрыва игнорируются —
        // их цепь может быть цела, дыра выше) ?? записанное ?? min-объект.
        var ratchet = wal is { ChainStartSegment.Length: > 0 }
            ? WalFileName.TryParse(wal.ChainStartSegment) : null;
        WalFileName? fromFull = WalChain.RatchetedStart(ratchet,
            shardBackups?.Full
                .Where(f => f.State == FullBackupStatus.Completed)
                .Select(f => f.WalStartSegment) ?? []);
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
            return new ControlOutcome(wal);

        // CheckWithRestart: после restore promote открывает новый TLI, старые
        // сегменты обрезаны легитимно (AC4) — дыра на TLI-границе не деградация
        var chain = WalChain.CheckWithRestart(start, objects.Select(o => o.Name));
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
            return new ControlOutcome(wal);

        // (7) Lag-зонд: pg_current_wal_lsn() мастера → сегмент → дистанция.
        long? lag = null;
        var current = await sql.CurrentWalAsync(adminDsn, ct);
        if (current.IsSuccess && WalFileName.TryParse(lastUploadedName) is { } lastSegment)
        {
            var masterSegment = WalFileName.FromLsn((uint)current.Value.Tli, current.Value.Lsn);
            lag = Math.Max(0, lastSegment.DistanceTo(masterSegment));
        }

        lagObserver?.Invoke(cluster, shard, lag);

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
            var ensured = await sql.EnsureSlotAsync(adminDsn, slot, ct);
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
    }
}
