using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgWorker.Backups.Sql;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Backups;

/// <summary>Итог контроля одного прохода (Task 10): State — СВЕЖЕЕ состояние шарда
/// ПОСЛЕ контроля (записанное контролем или прежнее при «не время»); ChainBroken —
/// деградация РАЗРЫВА (дыра цепочки/слот): агента не поднимать. Transient-деградация
/// (lag/тишина) держит ChainBroken=false — exited-агент пересоздаётся со свежими
/// env (spec §3.2 п.5/п.7; ревью Ф4-2 №1: guard по «любому DEGRADED» запирал
/// restart-контур навсегда).</summary>
public sealed record ControlOutcome(WalStreamState? State, bool ChainBroken);

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
    private readonly ConcurrentDictionary<string, long> _lastVerifyUnix = [];

    // Маркер «цепочка разорвана» per-shard (живёт между тиками до успешного контроля
    // — агент не поднимается повторными тиками, пока полный бэкап не сдвинет старт).
    private readonly ConcurrentDictionary<string, bool> _chainBroken = [];

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

        // (3) Ensure слота (spec §3.2 п.3): есть → пропуск; нет при живой записи о
        //     цепочке → инвалидация (DEGRADED + стоп агента — chain-broken); нет и
        //     цепочки нет (первый старт) → create immediate+reserved.
        var slotExists = await sql.SlotExistsAsync(adminDsn, slot, ct);
        if (!slotExists.IsSuccess)
            throw new ApplicationException($"слот-зонд {slot}: {slotExists.Error!.Message}");
        if (!slotExists.Value)
        {
            if (chainKnown && wal!.State != WalStreamStatus.Stopped)
            {
                await DegradeAsync(cluster, shard.Name, wal, slot, masterRef, ct,
                    baseStart: wal.ChainStartSegment, baseLast: wal.LastUploadedSegment,
                    baseUnix: wal.LastUploadedUnix!.Value,
                    error: $"слот {slot} исчез при живой цепочке (инвалидация max_slot_wal_keep_size?) — пересними полный бэкап");
                return;
            }

            var created = await sql.EnsureSlotAsync(adminDsn, slot, ct);
            if (!created.IsSuccess)
                throw new ApplicationException($"ensure слота {slot}: {created.Error!.Message}");
        }

        // (6–7) Контроль по расписанию — ДО ensure агента. ControlOutcome:
        // State — свежее состояние ПОСЛЕ контроля (восстановление ACTIVE новым
        // полным + подъём агента ОДНИМ тиком, AC4c); ChainBroken=true — агента
        // НЕ поднимаем (дыра/слот); transient-DEGRADED (lag/тишина) — НЕ блокирует
        // супервиз (exited-агент пересоздаётся, spec §3.2 п.5; ревью Ф4-2 №1).
        // Тело — Task 10; в этой задаче — временная заглушка.
        var controlled = await ControlDueAsync(
            cluster, shard.Name, wal, shardBackups, options, masterRef, slot, adminDsn, ct);

        // (4–5) Агент + супервиз.
        if (!controlled.ChainBroken)
            await EnsureAgentAsync(cluster, shard.Name, options, slot, masterRef, pgHost, pgPort,
                password, masterAddr.Host, controlled.State, ct);
    }

    // (4–5) Контейнер агента: создание идемпотентно (по образцу EnsureNode, без
    // портов); супервиз: running → пропуск; exited/restarting → пересоздание
    // (restart-луп: устаревшие креды/staging переполнен — включая transient-DEGRADED
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
        var existing = listed.Value.FirstOrDefault(c => c.Names.Contains("/" + agentName));

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
            Image: options.AgentImage,
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
            NetworkAliases: null);

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

    // (6–7) Контроль по расписанию VerifyIntervalSec (spec §3.2 п.6–8): list S3 →
    // chain_start (min COMPLETED-полного ?? закреплённый ?? min-объект) → CheckChain →
    // дыра: chain-broken DEGRADED + ОСТАНОВ агента (в т.ч. без прошлого ключа —
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
        if (wal is not null && _lastVerifyUnix.TryGetValue(key, out var last)
            && now - last < options.WalVerifyIntervalSec)
            return new ControlOutcome(wal, ChainBrokenOf(key));
        // (листим S3 и до создания ключа каждый тик — скорость первого наблюдения,
        // пустой префикс дёшев; подтверждено ревью Ф4-2 как допустимое)

        _lastVerifyUnix[key] = now;

        // Истина прогресса — объекты S3 (arch/19 §3): list префикса wal/.
        var listed = await s3.ListWalAsync(cluster, shard, ct: ct);
        if (!listed.IsSuccess)
            throw new ApplicationException($"list S3 {cluster}/{shard}/wal: {listed.Error!.Message}");
        var objects = listed.Value;

        // chain_start (п.8): min wal_start_segment COMPLETED-полных ?? закреплённое
        // из текущего ключа ?? min-объект (первый сегмент потока агента).
        WalFileName? fromFull = shardBackups?.Full
            .Where(f => f.State == FullBackupStatus.Completed && !string.IsNullOrEmpty(f.WalStartSegment))
            .Select(f => WalFileName.TryParse(f.WalStartSegment))
            .Where(s => s is not null)
            .Select(s => s!.Value)
            .OrderBy(s => s.Name, StringComparer.Ordinal)
            .Cast<WalFileName?>()
            .FirstOrDefault();
        WalFileName? minObject = objects
            .Select(o => WalFileName.TryParse(o.Name))
            .Where(s => s is not null)
            .Select(s => s!.Value)
            .OrderBy(s => s.Name, StringComparer.Ordinal)
            .Cast<WalFileName?>()
            .FirstOrDefault();
        var chainStart = fromFull
                         ?? (wal is not null ? WalFileName.TryParse(wal.ChainStartSegment) : null)
                         ?? minObject;

        // Нет полных, нет объектов, нет прошлого ключа — писать нечего (п.8):
        // ключ не пишется до первого наблюдения; агент работает (объекты появятся).
        if (chainStart is not { } start)
            return new ControlOutcome(wal, ChainBrokenOf(key));

        var chain = WalChain.Check(start, objects.Select(o => o.Name));
        if (!chain.IsContinuous)
        {
            // Дыра: DEGRADED даже без прошлого ключа (AC4-тотальность) — DegradeAsync
            // строит запись с наблюдаемыми фактами и маркирует _chainBroken.
            var lastForBase = chain.LastSegment?.Name ?? start.Name;
            var lastModifiedForBase = objects
                .Where(o => o.Name == lastForBase)
                .Select(o => o.LastModified)
                .Select(m => (DateTimeOffset?)m)
                .FirstOrDefault() ?? DateTimeOffset.FromUnixTimeSeconds(wal?.LastUploadedUnix ?? now);
            var degraded = await DegradeAsync(cluster, shard, wal, slot, masterRef, ct,
                baseStart: start.Name, baseLast: lastForBase,
                baseUnix: lastModifiedForBase.ToUnixTimeSeconds(),
                error: chain.GapError!);
            return new ControlOutcome(degraded, ChainBroken: true);
        }

        // Факты прогресса — только из наблюдений: наблюдаемый последний сегмент
        // цепочки; если цепочка не наблюдалась вовсе (объектов нет / все ниже
        // chain_start) — прошлый ключ, НИКОГДА не now() (ревью Ф4-2 №2).
        var observedLast = chain.LastSegment;
        var lastUploadedName = observedLast?.Name ?? wal?.LastUploadedSegment;
        var lastUploadedUnix = observedLast is { } seen
            ? objects.Where(o => o.Name == seen.Name).Select(o => o.LastModified).Max().ToUnixTimeSeconds()
            : wal?.LastUploadedUnix;

        // Прогресс не наблюдался и прошлого наблюдения нет — ключ не пишем
        // (spec п.8: «ключ не пишется до первого наблюдения»; писать
        // last_uploaded = chain_start, который не загружался, — подмена факта).
        if (lastUploadedName is null || lastUploadedUnix is null)
            return new ControlOutcome(null, ChainBrokenOf(key));

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
        _chainBroken[key] = false; // цепочка цела — супервиз разрешён
        return new ControlOutcome(next, ChainBroken: false);
    }

    // Текущий вердикт разрыва для «не время»-веток (грязное чтение словаря — ок:
    // пишет только этот же процесс под клэймом).
    private bool ChainBrokenOf(string key) => _chainBroken.TryGetValue(key, out var broken) && broken;

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

    // Chain-broken деградация: DEGRADED + error + ОСТАНОВ агента (слот не
    // пересоздаётся — продолжение с дырой бессмысленно; лечение — новый полный
    // t02/t07). Маркирует _chainBroken (повторные тики агента не поднимают).
    // wal == null — запись создаётся с наблюдаемыми фактами контроля (AC4-
    // тотальность: дыра найдена при первом наблюдении цепочки). Возвращает
    // записанное состояние (свежий вердикт для вызова — AC4c одним тиком).
    private async Task<WalStreamState> DegradeAsync(
        string cluster, string shard, WalStreamState? wal, string slot, string masterRef,
        CancellationToken ct,
        string baseStart, string baseLast, long baseUnix, string error)
    {
        logger?.LogError("backup-wal {Cluster}/{Shard}: {Error}", cluster, shard, error);
        _chainBroken[$"{cluster}/{shard}"] = true;
        var removed = await driver.RemoveBackupAgentsAsync(cluster, shard, ct);
        if (!removed.IsSuccess)
            throw new ApplicationException($"стоп агента {shard}: {removed.Error!.Message}");
        var current = wal ?? new WalStreamState(
            WalStreamStatus.Active, slot, masterRef, baseStart, baseLast, baseLast, baseUnix, null, null);
        var degraded = current with { State = WalStreamStatus.Degraded, Error = error };
        await status.WriteIfChangedAsync(cluster, shard, degraded, ct);
        return degraded;
    }
}
