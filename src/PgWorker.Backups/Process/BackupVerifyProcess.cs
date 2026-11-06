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
/// arch/backups/06-s3-layout.md/§8) под клэймом &lt;C&gt;: due-резолв кандидатов (PENDING от
/// on_create / периодика по verify.interval_sec; FAILED терминален) →
/// verify-операция WalReceiver ноды-источника полного (candidate.Node)
/// (полнота набора доказывается манифестом в самой операции; дубль wal/ —
/// общий контроль цепочки, от формата полного не зависит):
/// POST /api/ops/verify на `alias:8080` её приёмника, поллинг GET — итог.
/// Тик не блокируется на длинную проверку: итог — поллинг GET /api/ops/verify
/// следующих тиков. Transient (сбой скачивания: S3/приёмник недостижим,
/// манифест не добыт) — статус кандидата не трогаем, операция перезапускается
/// следующим тиком; permanent (streaming verify failed: данные не сошлись с
/// манифестом) — FAILED сразу, перепроверка не запускается (вердикт уже
/// определён).
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
    TimeSpan? watchdogWindow = null) // период S3-пульса — полокна (образец сверстников)
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

        //  (arch/backups/06-s3-layout.md): пул чтения per-cluster — точечные GET/листинги с
        // перебором ReadAll. null/Failed — transient: journal, ретраи тиком
        // (кандидаты остаются PENDING/OK, статусы не трогаем).
        var poolRead = await s3Factory.ResolveAsync(cluster, ct);
        if (!poolRead.IsSuccess || poolRead.Value is not { } poolCtx)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-pool", claims.InstanceId,
                poolRead.IsSuccess ? "policy without s3 section — pool not selected" : poolRead.Error!.Message, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        foreach (var shard in snap.Shards.Where(s => !s.ToRemove))
        {
            // шард без ключей полных — пропускается (dsn не нужен: verify чисто по S3)
            if (!mine.Shards.TryGetValue(shard.Name, out var shardBackups) || shardBackups.Full.Count == 0)
                continue;

            try
            {
                await TickShardAsync(cluster, shard.Name, shardBackups, addresses.Value, intervalSec, nowUnix, poolCtx, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // ошибка шарда не роняет остальные (arch/synchronization/README.md)
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
        BackupPoolContext pool, CancellationToken ct)
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

        // (3) Полнота набора — манифестом в потоковой verify-операции
        //     приёмника (WAL-сегменты — WAL-Ranges манифеста); дубль wal/ —
        //     общий контроль цепочки ([02-wal-archive §3], от формата
        //     полного не зависит).
        // (4) Verify-операция WalReceiver ноды-источника полного (arch/backups/06-s3-layout.md):
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
            var launch = await receiver.StartVerifyAsync(
                receiverUrl, StartRequest(cluster, shard, full, pool), ct);
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

        // Сверка снимка по id (arch/backups/01-full-backup.md §7): ops-реестр
        // приёмника ОДИН на процесс — GET может вернуть чужой/прошлый снимок
        // (напр., терминал restore/drill-операции с текстом pg_ctl). Чужой id —
        // чужой итог: вердикта по нему нет, кандидат перезапускается
        // vanished-семантикой (повторный POST поверх терминального снимка;
        // чужой running ответит 409 — тик повторит).
        if (!string.Equals(snapshot.Id, full.Id, StringComparison.Ordinal))
        {
            logger.LogWarning(
                "{Op} {cluster}/{shard}: verify {id} — registry holds a foreign snapshot (id={SnapId}, state={State}); relaunching via the vanished path",
                Op, cluster, shard, id, snapshot.Id ?? "<null>", snapshot.State);
            await journal.WritePhaseAsync(cluster, Op, $"foreign-snapshot/{shard}/{id}",
                claims.InstanceId,
                $"receiver registry holds operation {snapshot.Id ?? "<null>"} ({snapshot.State}) — not the verify candidate", ct);
            var relaunch = await receiver.StartVerifyAsync(
                receiverUrl, StartRequest(cluster, shard, full, pool), ct);
            if (!relaunch.IsAccepted && !relaunch.IsConflict)
                observe(cluster, shard, "transient");
            return;
        }

        switch (snapshot.State)
        {
            case "running":
            {
                // Возрастной бюджет операции — started_unix из снимка приёмника
                // (arch/backups/09-operability.md): исчерпан → cancel best-effort, кандидат остаётся
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
            case "succeeded" when full.Verify is { State: BackupVerifyStatus.Ok } v:
            {
                // Различаем снимок НАШЕЙ запущенной перепроверки и терминальный
                // снимок ПРОШЛОЙ джобы: операция, начатая ПОЗЖЕ последней
                // учтённой проверки (StartedUnix > checked_unix) — наш итог,
                // записываем (AC5: checked_unix растёт). Снимок старее/бездатный
                // — это прошлый терминал при периодическом due: повторный POST,
                // идемпотентно (WalReceiver перезаписывает терминальные снимки
                // в TryBegin). Без ветки реланча воркер поллит завершённую
                // джобу вечно и перепроверка по расписанию (verify.interval_sec)
                // не исполняется никогда (разбор Verify_Corruption).
                if (snapshot.StartedUnix is not { } started || started > v.CheckedUnix)
                {
                    var updated = full with { Verify = new BackupVerify(BackupVerifyStatus.Ok, nowUnix) };
                    var put = await PutAsync(BackupNames.FullKey(cluster, shard, full.Id),
                        BackupStatusJson.Serialize(updated), ct);
                    if (!put.IsSuccess)
                        return; // transient — статус не сменился, снимок повторит вердикт
                    await journal.WritePhaseAsync(cluster, Op, $"verified-ok/{shard}/{id}",
                        claims.InstanceId, null, ct);
                    return;
                }

                var relaunch = await receiver.StartVerifyAsync(
                    receiverUrl, StartRequest(cluster, shard, full, pool), ct);
                if (!relaunch.IsAccepted && !relaunch.IsConflict)
                    observe(cluster, shard, "transient"); // отказ — тик повторит
                return;
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
                // МАРКЕР ТРАНЗИЕНТА: сбой скачивания — вердикта о данных нет
                // (статус не трогаем); операция на приёмнике начата ЗАНОВО
                // (резюмов нет) — иначе терминальный failed-снимок остаётся
                // текущим и перепроверка по расписанию не возобновляется
                // (spec t01 §6.7: сбой скачивания — воркер ретраит).
                await journal.WritePhaseAsync(cluster, Op, $"download-retry/{shard}/{id}",
                    claims.InstanceId, snapshot.Error ?? "download failed", ct);
                logger.LogWarning(
                    "{Op} {cluster}/{shard}: verify {id} download failed: {Error}",
                    Op, cluster, shard, id, snapshot.Error ?? "download failed");
                var retry = await receiver.StartVerifyAsync(
                    receiverUrl, StartRequest(cluster, shard, full, pool), ct);
                if (!retry.IsAccepted && !retry.IsConflict)
                    observe(cluster, shard, "transient"); // отказ — тик повторит
                return;
            }
            case "failed":
            {
                // PERMANENT (phase=verifying): streaming verify failed — данные
                // не сошлись с манифестом набора.
                var err = snapshot.Error ?? "streaming verify failed";
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

    // POST-тело запуска verify-операции (spec t01 §3.7): id + S3-контекст пула
    // чтения (один endpoint-набор, канон §6.4) + ожидаемый sha256 объекта.
    private StartVerifyRequest StartRequest(
        string cluster, string shard, FullBackupState full, BackupPoolContext pool)
        => new(
            full.Id,
            new S3Context(
                Endpoints: [.. pool.ReadAll],
                AccessKey: pool.AccessKey,
                SecretKey: pool.SecretKey,
                Bucket: options.S3Bucket,
                Prefix: $"{cluster}/{shard}",
                CaPem: BackupCaPem.ReadIfConfigured(options.S3CaBundlePath)),
            Sha256: full.Sha256);

    // alias:8080 приёмника ноды-источника (arch/backups/01-full-backup.md/§5): имя docker-
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
