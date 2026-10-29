using Microsoft.Extensions.Logging;
using PgWorker.Backups.Drill;
using PgWorker.Core;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Backups.Process;

/// <summary>
/// RestoreDrillProcess — тиковая машина планового тестового восстановления
/// (reliability /, arch/19 §3.6) под клэймом &lt;C&gt; (Op backup-drill):
/// супервиз активной drill-операции WalReceiver ПЕРВОЙ ноды шарда (фазы
/// downloading|recovering и итог — по API приёмника GetRestoreAsync; команда
/// уходит в живой приёмник — «заявка → операция работает» за секунды),
/// отбор «один шард за проход, наименее свежий по дриллу» (DrillPlanner),
/// валидация кандидата — путь restore-заявки (манифест + WalChain),
/// запуск POST /api/ops/restore (latest, id дрилла — scratch-каталог на ноде);
/// таймаут-бюджет DrillTimeoutSec → FAILED drill-timeout (+ cancel best-effort);
/// операция vanished (GET 404) при молодом ключе — повторный POST, старше
/// бюджета → FAILED drill-vanished. Контроль — «старт postgres + выход из
/// recovery» (restored_to_lsn); SQL-проверок данных нет. Состояние — один
/// etcd-ключ /pgworker/backups/&lt;C&gt;/&lt;X&gt;/drill (перезаписывается).
/// </summary>
public sealed class RestoreDrillProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    ShardEndpoints shardEndpoints,
    BackupS3Factory s3Factory,
    IWalReceiverApiClient receiver,
    ClaimStore claims,
    WorkJournal journal,
    BackupsRuntimeOptions options,
    TimeProvider time,
    ILogger<RestoreDrillProcess> logger,
    Action<string, string, string>? drillObserver = null, // (cluster, shard, ok|failed), arch/18 §2.7
    TimeSpan? watchdogWindow = null,
    Shared.Core.Hosting.ILoopProgress? progress = null) // аудит долгих фаз: итерации поллинга дают Mark (spec §1.2 п.4)
{
    public const string Op = "backup-drill";

    // Окно проверки watchdog для S3-вызовов (дефолт — продовые 15 c).
    private TimeSpan WatchdogWindow => watchdogWindow ?? TimeSpan.FromSeconds(15);

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Guard 1: клэйм наш (мутации /pgworker/backups/* — только держатель).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: claim is not ours (or lost) — mutations are forbidden"));

        // Guard 2: только Active-кластер. Backups:Enabled=false — НЕ выход:
        // блокируются только новые запуски (флаг — в отборе); супервиз
        // активного дрилла продолжается (стоп-семантика).
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var mine = backups.FirstOrDefault(b => b.Cluster == cluster)
                   ?? new ClusterBackups(cluster, null, new Dictionary<string, ShardBackups>());
        var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();

        // Активность дрилла кластера (одна на кластер): RUNNING — супервиз.
        var running = mine.Shards
            .Where(p => p.Value.Drill is { State: DrillStatus.Running })
            .Select(p => (Shard: p.Key, Drill: p.Value.Drill!))
            .OrderBy(p => p.Drill.StartedUnix)
            .FirstOrDefault();

        if (running.Drill is not null)
        {
            await SuperviseRunningAsync(snap, running.Shard, running.Drill, nowUnix, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // ── Отбор кандидата (новых запусков нет, пока есть незавершённый дрилл —
        // выше; arch/19 §3.6 п.3): интервал — policy.drill.interval_days ?? конфига;
        // 0/Enabled=false — выкл новых запусков.
        var intervalDays = mine.Policy?.DrillIntervalDays ?? options.DrillIntervalDays;
        if (intervalDays <= 0 || !options.Enabled)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var drills = mine.Shards
            .Where(p => p.Value.Drill is not null)
            .ToDictionary(p => p.Key, p => p.Value.Drill!);
        var candidate = DrillPlanner.SelectCandidate(snap.Shards, mine.Shards, drills, intervalDays, nowUnix);
        if (candidate is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        return await ValidateAndStartAsync(snap, candidate, mine, nowUnix, ct);
    }

    // ── Валидация кандидата + запуск (arch/19 §3.6 п.4–5) — путь restore-заявки
    // own-source (RestoreProcess.ValidateAsync, чистые функции те же): ──

    private async Task<Result<ProcessOutcome>> ValidateAndStartAsync(
        ClusterSnapshot snap, string shard, ClusterBackups mine, long nowUnix, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        //  (arch/19 §5): пул чтения per-cluster — манифест/label/лист wal с
        // перебором ReadAll. null/Failed — transient: journal, тик повторит.
        var poolRead = await s3Factory.ResolveAsync(cluster, ct);
        if (!poolRead.IsSuccess || poolRead.Value is not { } poolCtx)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-pool", claims.InstanceId,
                poolRead.IsSuccess ? "policy has no s3 section — pool not selected" : poolRead.Error!.Message, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        // Клиент — кэш фабрики (breaker переживает тики) — НЕ диспоузим.
        var s3 = s3Factory.Create(poolCtx);

        if (!mine.Shards.TryGetValue(shard, out var sb))
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        var fresh = sb.Full
            .Where(f => f.State == FullBackupStatus.Completed)
            .MaxBy(f => f.Id, StringComparer.Ordinal);
        if (fresh is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // гвард отбора — молча
        var backupId = fresh.Id;

        // 1. Манифест: upload не атомарен — частичный префикс отсеиваем
        // (образец); отказ — валидационный FAILED без операции.
        var manifest = await S3Pulse.CallAsync(progress, token => s3.DownloadTextAsync(cluster, shard, $"full/{backupId}/backup_manifest", token), ct, WatchdogWindow / 2);
        if (!manifest.IsSuccess)
            return await FailValidationAsync(cluster, shard, backupId, nowUnix,
                $"full backup {backupId} without backup_manifest (incomplete/corrupt)", ct);

        // 2. Стартовая точка WAL: etcd-статус; нет — backup_label из S3.
        var walStart = fresh.WalStartSegment;
        if (walStart is not { Length: > 0 })
        {
            var label = await S3Pulse.CallAsync(progress, token => s3.DownloadTextAsync(cluster, shard, $"full/{backupId}/backup_label", token), ct, WatchdogWindow / 2);
            walStart = label.IsSuccess ? Restore.BackupLabel.WalStartSegment(label.Value) : null;
            if (walStart is null)
                return await FailValidationAsync(cluster, shard, backupId, nowUnix,
                    $"full/{backupId}: backup_label unavailable/corrupt", ct);
        }
        var chainStart = WalFileName.TryParse(walStart);
        if (chainStart is null)
            return await FailValidationAsync(cluster, shard, backupId, nowUnix,
                $"full/{backupId}: wal_start '{walStart}' cannot be parsed", ct);

        // 3. Непрерывность WAL-цепочки (WalChain); S3-отказ → transient
        // (статус не трогаем — тик повторит). Листинг — UNION по всем нодам
        // пула (arch/19 §5 «где данные есть»): соло-лист первой живой
        // ноды не содержит сегментов, залитых на другие (полный — на первую
        // живую момента заливки) → ложный gap «expected wal_start, found N+1»
        // и валидационный FAILED — предшественник шторма пересъёмов.
        var wal = await S3Pulse.CallAsync(progress, token => s3.ListWalUnionAsync(cluster, shard, ct: token), ct, WatchdogWindow / 2);
        if (!wal.IsSuccess)
        {
            await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}",
                claims.InstanceId, "s3 list unavailable — validation on next tick", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        var chain = WalChain.Check(chainStart.Value, wal.Value.Select(o => o.Name));
        if (!chain.IsContinuous)
            return await FailValidationAsync(cluster, shard, backupId, nowUnix,
                chain.GapError ?? "WAL chain gap", ct);

        // ── Запуск: journal-before-manipulations — put RUNNING до команды;
        // операцию исполняет WalReceiver ПЕРВОЙ ноды шарда (arch/19 §3.6).
        var id = BackupPlanner.NextId([], time.GetUtcNow().UtcDateTime);
        var receiverUrl = await ResolveReceiverUrl(snap, shard);
        if (receiverUrl is null)
        {
            await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}",
                claims.InstanceId, "first node not in portalloc — start on next tick", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        var running = new DrillState(id, DrillStatus.Running, backupId, nowUnix);
        var put = await PutDrillAsync(cluster, shard, running, ct);
        if (!put.IsSuccess)
            return Result<ProcessOutcome>.Failed(put.Error!);

        // Команда StartRestoreAsync (latest — БЕЗ target_time, §3.6; id —
        // drill-признак: scratch-каталог backup-drill/<id>/pgdata). 409 =
        // операция уже жива (пере-старт тика). Отказ — transient:
        // следующий тик повторит POST идемпотентно.
        var launch = await receiver.StartRestoreAsync(
            receiverUrl,
            new StartRestoreRequest(
                BackupId: backupId,
                Id: id,
                TargetTime: null,
                RecoveryTimeoutSec: options.RestoreRecoveryTimeoutSec,
                S3: new S3Context(
                    Endpoints: [.. poolCtx.ReadAll],
                    AccessKey: poolCtx.AccessKey,
                    SecretKey: poolCtx.SecretKey,
                    Bucket: options.S3Bucket,
                    Prefix: $"{cluster}/{shard}",
                    CaPem: BackupCaPem.ReadIfConfigured(options.S3CaBundlePath))), ct);
        if (!launch.IsAccepted && !launch.IsConflict)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // transient — тик повторит POST

        await journal.WritePhaseAsync(cluster, Op, $"started/{shard}/{id}", claims.InstanceId, null, ct);
        logger.LogInformation("{Op} {cluster}/{shard}: drill operation {id} started on first node's receiver (full backup {backupId})",
            Op, cluster, shard, id, backupId);
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Валидационный FAILED — БЕЗ запуска операции: ключ сразу чистый терминальный
    // итог (честный исход «восстановимость не доказана» — оператор видит ровно
    // то, что увидел бы DR); scratch не создавался — чистка не нужна.
    private async Task<Result<ProcessOutcome>> FailValidationAsync(
        string cluster, string shard, string backupId, long nowUnix, string error, CancellationToken ct)
    {
        var id = BackupPlanner.NextId([], time.GetUtcNow().UtcDateTime);
        var failed = new DrillState(id, DrillStatus.Failed, backupId, nowUnix,
            FinishedUnix: nowUnix, Error: error);
        var put = await PutDrillAsync(cluster, shard, failed, ct);
        if (!put.IsSuccess)
            return Result<ProcessOutcome>.Failed(put.Error!);
        drillObserver?.Invoke(cluster, shard, "failed"); // FAILED-валидация — итог без операции
        await journal.WritePhaseAsync(cluster, Op, $"failed/{shard}/{id}", claims.InstanceId, error, ct);
        logger.LogWarning("{Op} {cluster}/{shard}: drill failed validation: {error}",
            Op, cluster, shard, error);
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // ── Супервиз RUNNING-дрилла (arch/19 §3.6 п.2) ──

    private async Task SuperviseRunningAsync(
        ClusterSnapshot snap, string shard, DrillState drill, long nowUnix, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // пул чтения для повторного POST (vanished-ветка); null/Failed —
        // transient: тик повторит, статус ключа не трогаем.
        var poolRead = await s3Factory.ResolveAsync(cluster, ct);
        if (!poolRead.IsSuccess || poolRead.Value is not { } poolCtx)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-pool", claims.InstanceId,
                poolRead.IsSuccess ? "policy has no s3 section — pool not selected" : poolRead.Error!.Message, ct);
            return;
        }

        // Адрес приёмника первой ноды (детерминирован, arch/19 §3.6); ноды нет
        // в portalloc (шард демонтируется) — transient: статус ключа не трогаем.
        var receiverUrl = await ResolveReceiverUrl(snap, shard);
        if (receiverUrl is null)
        {
            await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}/{drill.Id}",
                claims.InstanceId, "first node not in portalloc — status left untouched", ct);
            return;
        }

        var reply = await receiver.GetRestoreAsync(receiverUrl, ct);

        if (reply.IsNotFound)
        {
            // Операции нет (старт не дошёл/рестарт приёмника): пока ключ молод —
            // идемпотентный повторный POST (резюмов нет — скачается заново);
            // старше бюджета — FAILED drill-vanished.
            if (SupervisionTimeouts.IsTimedOut(drill.StartedUnix, nowUnix, options.DrillTimeoutSec))
            {
                var vanished = drill with
                {
                    State = DrillStatus.Failed,
                    FinishedUnix = nowUnix,
                    Error = $"drill-vanished: operation missing, age {nowUnix - drill.StartedUnix} s > {options.DrillTimeoutSec} s",
                    Phase = null, // чистый итог — scratch чистит приёмник
                };
                var put = await PutDrillAsync(cluster, shard, vanished, ct);
                if (!put.IsSuccess)
                    return; // ключ остаётся RUNNING — вердикт следующим тиком
                drillObserver?.Invoke(cluster, shard, "failed");
                await journal.WritePhaseAsync(cluster, Op, $"drill-failed/{shard}/{drill.Id}",
                    claims.InstanceId, vanished.Error, ct);
                return;
            }

            await receiver.StartRestoreAsync(
                receiverUrl,
                new StartRestoreRequest(
                    BackupId: drill.BackupId,
                    Id: drill.Id,
                    TargetTime: null,
                    RecoveryTimeoutSec: options.RestoreRecoveryTimeoutSec,
                    S3: new S3Context(
                        Endpoints: [.. poolCtx.ReadAll],
                        AccessKey: poolCtx.AccessKey,
                        SecretKey: poolCtx.SecretKey,
                        Bucket: options.S3Bucket,
                        Prefix: $"{cluster}/{shard}",
                        CaPem: BackupCaPem.ReadIfConfigured(options.S3CaBundlePath))), ct);
            return; // 202/409 — операция жива; отказ — transient, тик повторит
        }

        if (reply.IsTransient || reply.Status is not { } snapshot)
            return; // transient тика — статус ключа не трогаем

        // Бюджет активного дрилла — возраст RUNNING-ключа (etcd-факт + часы):
        // исчерпан → FAILED drill-timeout (+ cancel best-effort — приёмник
        // гасит дерево child-процессов и чистит scratch).
        if (SupervisionTimeouts.IsTimedOut(drill.StartedUnix, nowUnix, options.DrillTimeoutSec))
        {
            await receiver.CancelOpsAsync(receiverUrl, ct);
            var timedOut = drill with
            {
                State = DrillStatus.Failed,
                FinishedUnix = nowUnix,
                Error = $"drill-timeout: {nowUnix - drill.StartedUnix} s > {options.DrillTimeoutSec} s",
                Phase = null, // чистый итог — scratch чистит приёмник
            };
            var putTimedOut = await PutDrillAsync(cluster, shard, timedOut, ct);
            if (!putTimedOut.IsSuccess)
                return; // ключ остаётся RUNNING — вердикт следующим тиком
            drillObserver?.Invoke(cluster, shard, "failed");
            await journal.WritePhaseAsync(cluster, Op, $"drill-failed/{shard}/{drill.Id}",
                claims.InstanceId, timedOut.Error, ct);
            return;
        }

        switch (snapshot.State)
        {
            case "running":
                // Фаза операции (downloading|recovering) — в ключ при изменении.
                if (snapshot.Phase is { Length: > 0 } p && p != drill.Phase)
                    await PutDrillAsync(cluster, shard, drill with { Phase = p }, ct); // fail — тик повторит
                return;

            case "succeeded":
            {
                // старт postgres + выход из recovery доказаны (restored_to_lsn);
                // scratch чистит приёмник — ключ сразу чистый терминальный итог.
                var terminal = drill with
                {
                    State = DrillStatus.Succeeded,
                    FinishedUnix = nowUnix,
                    RestoredToLsn = snapshot.RestoredToLsn,
                    Phase = null, // чистый итог — scratch чистит приёмник
                };
                var putTerminal = await PutDrillAsync(cluster, shard, terminal, ct);
                if (!putTerminal.IsSuccess)
                    return; // ключ остаётся RUNNING — вердикт выведется заново
                drillObserver?.Invoke(cluster, shard, "ok");
                await journal.WritePhaseAsync(cluster, Op, $"drill-done/{shard}/{drill.Id}",
                    claims.InstanceId, null, ct);
                logger.LogInformation("{Op} {cluster}/{shard}: drill {id} — succeeded (restored_to_lsn {Lsn})",
                    Op, cluster, shard, drill.Id, snapshot.RestoredToLsn ?? "-");
                return;
            }

            case "cancelled":
            {
                // Операторская отмена (POST /api/ops/cancel приёмника —
                // внешний канал мимо воркера; снимок state=cancelled отличает
                // её от vanished): ключ дрилла УДАЛЯЕТСЯ — операций нет,
                // записи нет (scratch чистит приёмник при отмене); никакого
                // FAILED (arch/19 §3.6; следующий дрилл шарда — по общим
                // правилам отбора).
                var delOp = await etcd.DeleteAsync(endpoints[0],
                    BackupNames.DrillKey(cluster, shard), prefix: false, ct);
                if (!delOp.IsSuccess)
                    return; // transient — ключ жив, тик повторит ветку
                await journal.WritePhaseAsync(cluster, Op,
                    $"cancelled-by-operator/{shard}/{drill.Id}", claims.InstanceId,
                    "drill operation cancelled by operator — key removed, no trace left", ct);
                logger.LogInformation(
                    "{Op} {cluster}/{shard}: drill {Id} cancelled by operator — key removed, no trace left",
                    Op, cluster, shard, drill.Id);
                return;
            }

            case "failed" when snapshot.Phase == "downloading":
            {
                // МАРКЕР ТРАНЗИЕНТА: сбой скачивания — операция ЗАВЕРШИЛАСЬ
                // (scratch чистит приёмник); следующий тик повторит POST
                // (молодой ключ — vanished-ветка перезапустит операцию).
                await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}/{drill.Id}",
                    claims.InstanceId, snapshot.Error ?? "download failed", ct);
                return;
            }

            case "failed":
            {
                // PERMANENT (recovering): терминальный FAILED.
                var terminal = drill with
                {
                    State = DrillStatus.Failed,
                    FinishedUnix = nowUnix,
                    Error = snapshot.Error ?? "drill operation failed",
                    Phase = null, // чистый итог — scratch чистит приёмник
                };
                var putTerminal = await PutDrillAsync(cluster, shard, terminal, ct);
                if (!putTerminal.IsSuccess)
                    return;
                drillObserver?.Invoke(cluster, shard, "failed");
                await journal.WritePhaseAsync(cluster, Op, $"drill-failed/{shard}/{drill.Id}",
                    claims.InstanceId, terminal.Error, ct);
                return;
            }
        }
    }

    // alias:8080 приёмника ПЕРВОЙ ноды шарда (arch/19 §2/§3.6): имя docker-
    // контейнера в общей сети воркера и нод (object — имя её контейнера из
    // portalloc); ноды нет в portalloc — null (transient тика).
    private async Task<string?> ResolveReceiverUrl(ClusterSnapshot snap, string shard)
    {
        var cluster = snap.Config.Cluster;
        var firstNode = snap.Shards
            .FirstOrDefault(s => s.Name == shard)?.Nodes
            .Select(n => n.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .FirstOrDefault();
        if (firstNode is null)
            return null;
        var addresses = await shardEndpoints.ReadPortAllocAsync(cluster, CancellationToken.None);
        return addresses.IsSuccess
               && addresses.Value.TryGetValue($"{shard}/{firstNode}", out var addr)
            ? WalReceiverAddress.ForNode(cluster, shard, firstNode, addr.Object)
            : null;
    }

    // Failover-put ключа дрилла (первый успешный endpoint — образец verify).
    private async Task<Result> PutDrillAsync(string cluster, string shard, DrillState drill, CancellationToken ct)
    {
        var value = DrillStatusJson.Serialize(drill);
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, BackupNames.DrillKey(cluster, shard), value, null, ct);
            if (result.IsSuccess)
                return result;
        }

        return Result.Failed(new ApplicationException($"etcd put drill {cluster}/{shard} — all endpoints unavailable"));
    }
}
