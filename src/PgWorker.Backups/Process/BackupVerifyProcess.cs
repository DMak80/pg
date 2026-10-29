using Microsoft.Extensions.Logging;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Backups;

/// <summary>
/// BackupVerifyProcess — тиковая машина проверки полных бэкапов (/,
/// arch/19 §5/§8) под клэймом &lt;C&gt;: due-резолв кандидатов (PENDING от
/// on_create / периодика по verify.interval_sec; FAILED терминален) →
/// цепочечная проверка list-ами S3 + строгий LSN-разбор .history
/// (WalChain.CheckRange + WalHistory, без скачивания сегментов) →
/// verify-операция WalReceiver ноды-источника полного (candidate.Node):
/// POST /api/ops/verify на `alias:8080` её приёмника, поллинг GET — итог.
/// Тик не блокируется на длинную проверку: итог — поллинг GET /api/ops/verify
/// следующих тиков. Transient (S3/приёмник недостижим) — статус кандидата не
/// трогаем; permanent (дыра цепочки, нет wal_start, pg_verifybackup ≠ 0) —
/// FAILED сразу, checksums не запускается (вердикт уже определён).
/// </summary>
public sealed class BackupVerifyProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    ShardEndpoints shardEndpoints,
    BackupS3Factory s3Factory,
    IWalReceiverApiClient receiver,
    ClaimStore claims,
    WorkJournal journal,
    BackupsRuntimeOptions options,
    TimeProvider time,
    ILogger<BackupVerifyProcess> logger,
    Action<string, string, string>? verifyObserver = null, // (cluster, shard, result: ok|failed|transient)
    TimeSpan? watchdogWindow = null, // период S3-пульса — полокна (образец сверстников)
    Shared.Core.Hosting.ILoopProgress? progress = null) // S3-вызовы — пульс S3Pulse (arch/14 §6)
{
    private const string Op = "backup-verify";

    private TimeSpan WatchdogWindow => watchdogWindow ?? TimeSpan.FromSeconds(15);

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Guard 1: клэйм наш (мутации /pgworker/backups/* — только держатель).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: claim is not ours (or lost) — mutations are forbidden"));

        // Guard 2: Backups:Enabled=false → no-op (идущие операции не трогаем).
        if (!options.Enabled)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        // Guard 3: только Active-кластер.
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var mine = backups.FirstOrDefault(b => b.Cluster == cluster)
                   ?? new ClusterBackups(cluster, null, new Dictionary<string, ShardBackups>());
        var intervalSec = mine.Policy?.VerifyIntervalSec ?? options.VerifyIntervalSec;
        var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();

        // Адреса нод один раз на тик (portalloc) — резолв alias:8080 приёмника.
        var addresses = await shardEndpoints.ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Result<ProcessOutcome>.Failed(addresses.Error!);

        //  (arch/19 §5): пул чтения per-cluster — точечные GET/листинги с
        // перебором ReadAll. null/Failed — transient: journal, ретраи тиком
        // (кандидаты остаются PENDING/OK, статусы не трогаем).
        var poolRead = await s3Factory.ResolveAsync(cluster, ct);
        if (!poolRead.IsSuccess || poolRead.Value is not { } poolCtx)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-pool", claims.InstanceId,
                poolRead.IsSuccess ? "policy without s3 section — pool not selected" : poolRead.Error!.Message, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        // Клиент — кэш фабрики (breaker переживает тики) — НЕ диспоузим.
        var s3 = s3Factory.Create(poolCtx);

        foreach (var shard in snap.Shards.Where(s => !s.ToRemove))
        {
            // шард без ключей полных — пропускается (dsn не нужен: verify чисто по S3)
            if (!mine.Shards.TryGetValue(shard.Name, out var shardBackups) || shardBackups.Full.Count == 0)
                continue;

            try
            {
                await TickShardAsync(cluster, shard.Name, shardBackups, addresses.Value, intervalSec, nowUnix, poolCtx, s3, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // ошибка шарда не роняет остальные (arch/17)
                logger.LogError(ex, "{Op} {cluster}/{shard}: shard tick failed", Op, cluster, shard.Name);
                await journal.WritePhaseAsync(cluster, Op, "shard-error", claims.InstanceId,
                    $"{shard.Name}: {ex.Message}", ct);
            }
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    private async Task TickShardAsync(
        string cluster, string shard, ShardBackups shardBackups,
        IReadOnlyDictionary<string, NodeAddress> addresses, long intervalSec, long nowUnix,
        BackupPoolContext pool, PoolBackupS3 s3, CancellationToken ct)
    {
        var fulls = shardBackups.Full;

        // (1) due-резолв: PENDING-очередь раньше периодики; по одному за тик.
        //     verify FAILED — терминален: не попадает ни в один список.
        var pending = fulls
            .Where(f => f.State == FullBackupStatus.Completed
                        && f.Verify is { State: BackupVerifyStatus.Pending }
                        // checked_unix после verify-таймаута — квота попытки:
                        // повторный due через verify.interval_sec (лив-лок немедленных
                        // перезапусков исключён); null — on_create, due сразу;
                        // interval <= 0 — таймаутнувшийся кандидат не перезапускается
                        // автоматически (периодика выключена).
                        && (f.Verify.CheckedUnix is null
                            || intervalSec > 0 && nowUnix - f.Verify.CheckedUnix.Value > intervalSec))
            .OrderBy(f => f.StartedUnix)
            .ToList();
        var periodic = intervalSec > 0
            ? fulls
                .Where(f => f.State == FullBackupStatus.Completed
                            && (f.Verify is null
                                || f.Verify is { State: BackupVerifyStatus.Ok, CheckedUnix: var c }
                                && nowUnix - (c ?? 0) > intervalSec))
                .OrderBy(f => f.Verify?.CheckedUnix ?? 0)
                .ToList()
            : [];
        var candidate = pending.FirstOrDefault() ?? periodic.FirstOrDefault();
        if (candidate is null)
            return; // нет due — нечего проверять

        var id = candidate.Id;

        // (2) wal_start_segment отсутствует у COMPLETED — аномалия: permanent FAILED.
        if (string.IsNullOrEmpty(candidate.WalStartSegment))
        {
            await WritePermanentAsync(cluster, shard, candidate,
                "no wal_start_segment — set is not archiveable (no chain start point)", nowUnix, ct);
            return;
        }

        // (3) Цепочка: list wal/ + list набора + строгие TLI-переходы (по содержимому
        //     history при наличии). Transient list/GET → шард-skip, статус не трогаем.
        //     Листинги — UNION по всем нодам пула (arch/19 §5 «где данные
        //     есть»): полный заливается на первую живую ноду момента заливки —
        //     соло-лист другой ноды (отказ посреди заливки/лаг mesh) неполон и дал бы
        //     недокачанный набор → ложный permanent verify-FAILED.
        var walList = await S3Pulse.CallAsync(progress, token => s3.ListWalUnionAsync(cluster, shard, ct: token), ct, WatchdogWindow / 2);
        if (!walList.IsSuccess)
        {
            observe(cluster, shard, "transient");
            return;
        }

        var setList = await S3Pulse.CallAsync(progress, token => s3.ListUnionAsync(cluster, shard, $"full/{id}/pg_wal/", ct: token), ct, WatchdogWindow / 2);
        if (!setList.IsSuccess)
        {
            observe(cluster, shard, "transient");
            return;
        }

        var start = WalFileName.TryParse(candidate.WalStartSegment);
        if (start is null)
        {
            await WritePermanentAsync(cluster, shard, candidate,
                $"invalid wal_start_segment {candidate.WalStartSegment}", nowUnix, ct);
            return;
        }

        var setSegments = setList.Value
            .Select(o => WalFileName.TryParse(o.Name))
            .OfType<WalFileName>()
            .ToList();
        if (setSegments.Count == 0)
        {
            await WritePermanentAsync(cluster, shard, candidate,
                "set contains no pg_wal WAL segments", nowUnix, ct);
            return;
        }

        var endSegment = setSegments
            .OrderByDescending(s => (s.Tli, s.Log, s.Seg))
            .First();

        // Содержимое history для переходов диапазона (tli > start.Tli && tli <= end.Tli).
        var historyContents = new Dictionary<uint, string>();
        foreach (var obj in walList.Value)
        {
            if (WalFileName.TryParseHistory(obj.Name) is not { } tli
                || tli <= start.Value.Tli || tli > endSegment.Tli)
                continue;
            var content = await S3Pulse.CallAsync(progress, token => s3.GetObjectAsync(cluster, shard, $"wal/{obj.Name}", token), ct, WatchdogWindow / 2);
            if (!content.IsSuccess)
            {
                observe(cluster, shard, "transient");
                return;
            }

            historyContents[tli] = content.Value;
        }

        var check = WalChain.CheckRange(
            start.Value, endSegment, walList.Value.Select(o => o.Name), historyContents);
        if (!check.IsContinuous)
        {
            await WritePermanentAsync(cluster, shard, candidate,
                check.GapError ?? "WAL chain check failed", nowUnix, ct);
            return;
        }

        // (4) Verify-операция WalReceiver ноды-источника полного (arch/19 §5):
        //     alias:8080 приёмника candidate.Node (object — имя её контейнера).
        //     Итог — ТОЛЬКО по API: GET /api/ops/verify; 404 (операции нет —
        //     старт не дошёл/рестарт приёмника) → повторный POST идемпотентно
        //     (снимки терминальные TryBegin перезаписывает); 409 — операция
        //     уже жива (прошлый тик) → только поллинг. Секреты — только в
        //     POST-теле (Р4).
        var receiverUrl = ResolveReceiverUrl(cluster, shard, candidate.Node, addresses);
        if (receiverUrl is null)
        {
            // Нода-источник исчезла из portalloc (шард менялся) — кандидат
            // останется PENDING до переснятия полного на живой ноде.
            await journal.WritePhaseAsync(cluster, Op, $"node-missing/{shard}/{id}",
                claims.InstanceId, $"node {candidate.Node} not in portalloc — verify on next tick", ct);
            observe(cluster, shard, "transient");
            return;
        }

        var reply = await receiver.GetVerifyAsync(receiverUrl, ct);
        var full = candidate;

        if (reply.IsNotFound)
        {
            // Операции нет: запуск (ретрай сократил окно бута — приёмник ноды
            // уже жив; отказ POST — transient, тик повторит).
            var launch = await receiver.StartVerifyAsync(receiverUrl, new StartVerifyRequest(
                id,
                new S3Context(
                    Endpoints: [.. pool.ReadAll],
                    AccessKey: pool.AccessKey,
                    SecretKey: pool.SecretKey,
                    Bucket: options.S3Bucket,
                    Prefix: $"{cluster}/{shard}",
                    CaPem: BackupCaPem.ReadIfConfigured(options.S3CaBundlePath))), ct);
            if (launch.IsAccepted || launch.IsConflict)
            {
                await journal.WritePhaseAsync(cluster, Op, $"started/{shard}/{id}", claims.InstanceId, null, ct);
                logger.LogInformation("{Op} {cluster}/{shard}: verify operation {id} started on node {Node}",
                    Op, cluster, shard, id, candidate.Node);
            }
            else
            {
                observe(cluster, shard, "transient"); // транспорт/5xx — тик повторит
            }
            return;
        }

        if (reply.IsTransient || reply.Status is not { } snapshot)
        {
            // Приёмник ноды недостижим/5xx — transient (тик повторит GET).
            observe(cluster, shard, "transient");
            return;
        }

        switch (snapshot.State)
        {
            case "running":
            {
                // Возрастной бюджет операции — started_unix из снимка приёмника
                // (arch/19 §6): исчерпан → cancel best-effort, кандидат остаётся
                // PENDING с checked_unix=now (попытка зачтена, лив-лок немедленных
                // перезапусков исключён; вердикт FAILED по таймауту НЕ ставится —
                // данные не виноваты).
                if (snapshot.StartedUnix is { } started
                    && SupervisionTimeouts.IsTimedOut(started, nowUnix, options.JobVerifyTimeoutSec))
                {
                    await receiver.CancelOpsAsync(receiverUrl, ct);
                    var quota = full.Verify is null
                        ? new BackupVerify(BackupVerifyStatus.Pending, nowUnix)
                        : full.Verify with { CheckedUnix = nowUnix };
                    var putQuota = await PutAsync(BackupNames.FullKey(cluster, shard, full.Id),
                        BackupStatusJson.Serialize(full with { Verify = quota }), ct);
                    if (!putQuota.IsSuccess)
                        return; // transient — статус не сменился, повторим следующим тиком
                    await journal.WritePhaseAsync(cluster, Op, $"verify-timeout/{shard}/{id}",
                        claims.InstanceId, $"verify operation older than {options.JobVerifyTimeoutSec} s — cancelled, attempt counted", ct);
                }

                return; // ждём — проверка в работе
            }
            case "succeeded":
            {
                // verify OK: checksums прошли (SHA256 + manifest).
                var updated = full with { Verify = new BackupVerify(BackupVerifyStatus.Ok, nowUnix) };
                var put = await PutAsync(BackupNames.FullKey(cluster, shard, full.Id),
                    BackupStatusJson.Serialize(updated), ct);
                if (!put.IsSuccess)
                    return; // transient — статус не сменился, снимок повторит вердикт
                await journal.WritePhaseAsync(cluster, Op, $"verified-ok/{shard}/{id}",
                    claims.InstanceId, null, ct);
                logger.LogInformation("{Op} {cluster}/{shard}: verify {id} OK", Op, cluster, shard, id);
                observe(cluster, shard, "ok");
                return;
            }
            case "cancelled":
            {
                // Операторская отмена (POST /api/ops/cancel приёмника — внешний
                // канал мимо воркера; снимок state=cancelled отличает её
                // от vanished): операция как не начинавшаяся — перманентного
                // вердикта нет. Кандидат → PENDING с checked_unix=now (квота
                // попытки — перепроверка по расписанию verify.interval_sec,
                // симметрия таймаут-ветке выше).
                var quota = full.Verify is null
                    ? new BackupVerify(BackupVerifyStatus.Pending, nowUnix)
                    : full.Verify with { State = BackupVerifyStatus.Pending, CheckedUnix = nowUnix };
                var putQuota = await PutAsync(BackupNames.FullKey(cluster, shard, full.Id),
                    BackupStatusJson.Serialize(full with { Verify = quota }), ct);
                if (!putQuota.IsSuccess)
                    return; // transient — статус не сменился, снимок повторит ветку
                await journal.WritePhaseAsync(cluster, Op, $"cancelled-by-operator/{shard}/{id}",
                    claims.InstanceId, "verify operation cancelled by operator — candidate back to PENDING", ct);
                logger.LogInformation(
                    "{Op} {cluster}/{shard}: verify {id} cancelled by operator — candidate back to PENDING",
                    Op, cluster, shard, id);
                observe(cluster, shard, "transient");
                return;
            }
            case "failed" when snapshot.Phase == "downloading":
            {
                // МАРКЕР ТРАНЗИЕНТА: сбой скачивания — статус не трогаем
                // (PENDING); следующий тик повторит POST (операция начнётся
                // заново — резюмов нет).
                await journal.WritePhaseAsync(cluster, Op, $"download-retry/{shard}/{id}",
                    claims.InstanceId, snapshot.Error ?? "download failed", ct);
                logger.LogWarning(
                    "{Op} {cluster}/{shard}: verify {id} download failed: {Error}",
                    Op, cluster, shard, id, snapshot.Error ?? "download failed");
                observe(cluster, shard, "transient");
                return;
            }
            case "failed":
            {
                // PERMANENT (phase=verifying): pg_verifybackup ≠ 0 — данные плохие.
                var err = snapshot.Error ?? "pg_verifybackup failed";
                var failed = full with { Verify = new BackupVerify(BackupVerifyStatus.Failed, nowUnix, err) };
                var put = await PutAsync(BackupNames.FullKey(cluster, shard, full.Id),
                    BackupStatusJson.Serialize(failed), ct);
                if (!put.IsSuccess)
                    return;
                await journal.WritePhaseAsync(cluster, Op, $"verify-failed/{shard}/{id}",
                    claims.InstanceId, err, ct);
                logger.LogError("{Op} {cluster}/{shard}: verify {id} FAILED: {Error}", Op, cluster, shard, id, err);
                observe(cluster, shard, "failed");
                return;
            }
        }
    }

    // alias:8080 приёмника ноды-источника (arch/19 §2/§5): имя docker-
    // контейнера в общей сети воркера и нод (object — имя её контейнера из
    // portalloc); ноды нет в portalloc — null (transient тика).
    private static string? ResolveReceiverUrl(
        string cluster, string shard, string? node, IReadOnlyDictionary<string, NodeAddress> addresses)
    {
        if (node is not { Length: > 0 })
            return null;
        return addresses.TryGetValue($"{shard}/{node}", out var addr)
            ? WalReceiverAddress.ForNode(cluster, shard, node, addr.Object)
            : null;
    }

    // permanent FAILED: полная перезапись ключа статуса (BackupStatusJson), журнал
    // и наблюдатель; checked_unix — время вердикта.
    private async Task WritePermanentAsync(
        string cluster, string shard, FullBackupState full, string error, long nowUnix, CancellationToken ct)
    {
        var updated = full with
        {
            Verify = new BackupVerify(BackupVerifyStatus.Failed, nowUnix, error),
        };
        var put = await PutAsync(BackupNames.FullKey(cluster, shard, full.Id),
            BackupStatusJson.Serialize(updated), ct);
        if (!put.IsSuccess)
        {
            observe(cluster, shard, "transient"); // put не прошёл — статус остался прежним
            return;
        }

        await journal.WritePhaseAsync(cluster, Op, $"verify-failed/{shard}/{full.Id}",
            claims.InstanceId, error, ct);
        logger.LogError("{Op} {cluster}/{shard}: verify {full.Id} FAILED: {Error}", Op, cluster, shard, full.Id, error);
        observe(cluster, shard, "failed");
    }

    // Failover-put как BackupProcess.PutAsync (первый успешный endpoint).
    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
        }

        return Result.Failed(new ApplicationException($"etcd put {key} — all endpoints unavailable"));
    }

    private void observe(string cluster, string shard, string result)
        => verifyObserver?.Invoke(cluster, shard, result);
}
