using Microsoft.Extensions.Logging;
using PgWorker.Backups.Drill;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Docker.Drivers;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Backups.Process;

/// <summary>
/// RestoreDrillProcess — тиковая машина планового тестового восстановления
/// (reliability t02/t26, arch/19 §3.6) под клэймом &lt;C&gt; (Op backup-drill):
/// супервиз активного drill-tool-контейнера pgw-backup-drill-&lt;C&gt;-&lt;X&gt;-&lt;id&gt;
/// (фазы downloading|recovering и итог — по API приёмника GetRestoreAsync),
/// доводимый снос контура фазой cleaning (journal-before-rm; переживает
/// рестарт воркера — идемпотентная доводка по детерминированным именам),
/// отбор «один шард за проход, наименее свежий по дриллу» (DrillPlanner),
/// валидация кандидата — путь restore-заявки t05 (манифест + WalChain),
/// запуск tool-контейнера + StartRestoreAsync (latest); таймаут-бюджет
/// DrillTimeoutSec → FAILED drill-timeout, RUNNING без контейнера старше
/// бюджета → FAILED drill-vanished.
/// Control — «старт postgres + выход из recovery» (restored_to_lsn);
/// SQL-проверок данных нет. Состояние — один etcd-ключ
/// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/drill (перезаписывается).
/// </summary>
public sealed class RestoreDrillProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ShardEndpoints shardEndpoints,
    BackupS3Factory s3Factory,
    IWalReceiverApiClient receiver,
    ClaimStore claims,
    WorkJournal journal,
    BackupsRuntimeOptions options,
    TimeProvider time,
    ILogger<RestoreDrillProcess> logger,
    Action<string, string, string>? drillObserver = null, // (cluster, shard, ok|failed) — t14, arch/18 §2.7
    TimeSpan? watchdogWindow = null,
    Shared.Core.Hosting.ILoopProgress? progress = null) // аудит долгих фаз: итерации поллинга дают Mark (spec §1.2 п.4)
{
    public const string Op = "backup-drill";

    // Окно проверки watchdog для поллинга create-вызовов (дефолт — продовые 15 c)
    // и бюджет фазы create/start drill-джоба (PatroniBootSec-семантика: исчерпание
    // — transient, RUNNING-ключ остаётся, тик досоздаст по имени).
    private TimeSpan WatchdogWindow => watchdogWindow ?? TimeSpan.FromSeconds(15);
    private static readonly TimeSpan JobCreateBudget = TimeSpan.FromSeconds(120);

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
        // активного дрилла и доводка сноса продолжаются (стоп-семантика).
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var mine = backups.FirstOrDefault(b => b.Cluster == cluster)
                   ?? new ClusterBackups(cluster, null, new Dictionary<string, ShardBackups>());
        var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();

        // Активности дрилла кластера (по одному за тик, старейший started_unix):
        // RUNNING — супервиз; терминальный с phase=cleaning — доводка сноса
        // (краш-рекавери). Была активность → Done (отбор — следующим тиком).
        var activities = mine.Shards
            .Where(p => p.Value.Drill is { } d
                        && (d.State == DrillStatus.Running || d.Phase == "cleaning"))
            .Select(p => (Shard: p.Key, Drill: p.Value.Drill!))
            .OrderBy(p => p.Drill.StartedUnix)
            .ToList();

        if (activities.Count > 0)
        {
            var (shard, drill) = activities[0];
            if (drill.State == DrillStatus.Running)
                await SuperviseRunningAsync(snap, shard, drill, nowUnix, ct);
            else
                await FinishCleanupAsync(cluster, shard, drill, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // ── Отбор кандидата (новых запусков нет, пока есть незавершённый дрилл
        // или снос — activities выше вернули; arch/19 §3.6 п.3): интервал —
        // policy.drill.interval_days ?? конфига; 0/Enabled=false — выкл новых
        // запусков (доводка выше продолжается — стоп-семантика).
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
    // own-source (RestoreProcess.ValidateAsync t05, чистые функции те же): ──

    private async Task<Result<ProcessOutcome>> ValidateAndStartAsync(
        ClusterSnapshot snap, string shard, ClusterBackups mine, long nowUnix, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // t36 (arch/19 §5): пул чтения per-cluster — манифест/label/лист wal с
        // перебором ReadAll. null/Failed — transient: journal, тик повторит.
        var poolRead = await s3Factory.ResolveAsync(cluster, ct);
        if (!poolRead.IsSuccess || poolRead.Value is not { } poolCtx)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-pool", claims.InstanceId,
                poolRead.IsSuccess ? "policy has no s3 section — pool not selected" : poolRead.Error!.Message, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        await using var s3 = s3Factory.Create(poolCtx);

        if (!mine.Shards.TryGetValue(shard, out var sb))
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        var fresh = sb.Full
            .Where(f => f.State == FullBackupStatus.Completed)
            .MaxBy(f => f.Id, StringComparer.Ordinal);
        if (fresh is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // гвард отбора — молча
        var backupId = fresh.Id;

        // 1. Манифест: upload t02 не атомарен — частичный префикс отсеиваем
        // (t05-образец); отказ — валидационный FAILED без джоба.
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

        // 3. Непрерывность WAL-цепочки (WalChain t03); S3-отказ → transient
        // (статус не трогаем — тик повторит). Листинг — UNION по всем нодам
        // пула (t02, arch/19 §5 «где данные есть»): соло-лист первой живой
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

        // ── Запуск: journal-before-manipulations — put RUNNING до create
        // (arch/17); хост — первая нода шарда из portalloc (как restore-джоб).
        var id = BackupPlanner.NextId([], time.GetUtcNow().UtcDateTime);
        var (engine, startHost) = await EngineForShardAsync(snap, shard, ct);
        if (engine is null)
        {
            await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}",
                claims.InstanceId, "docker host not resolvable — start on next tick", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        var running = new DrillState(id, DrillStatus.Running, backupId, nowUnix);
        var put = await PutDrillAsync(cluster, shard, running, ct);
        if (!put.IsSuccess)
            return Result<ProcessOutcome>.Failed(put.Error!);

        var jobName = BackupNames.DrillContainerName(cluster, shard, id);
        // Tool-контейнер drill (t26): свой volume (одно имя с контейнером —
        // D1 жив), PGDATA Spilo-layout; API-порт — прод: зонд динамического,
        // контур — без публикации. ensure-семантика: уже есть → только старт.
        var contourMode = options.ContourNetwork is { Length: > 0 };
        int? hostPort = null;
        if (!contourMode)
        {
            var portProbe = Tool.FreePortProbe.Allocate();
            if (!portProbe.IsSuccess)
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // transient — тик повторит
            hostPort = portProbe.Value;
        }
        var listed0 = await engine.ListContainersAsync(jobName, all: true, ct);
        if (!listed0.IsSuccess)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // transient
        if (!listed0.Value.Any(c => c.Names.Contains(jobName)))
        {
            var created = await engine.CreateContainerAsync(
                Tool.ToolJobSpec.Build(options, cluster, jobName,
                    BackupNames.DrillVolumeName(cluster, shard, id),
                    "/home/postgres/pgdata", hostPort), jobName, ct);
            if (!created.IsSuccess)
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // transient — RUNNING остаётся
        }

        var started = await engine.StartContainerAsync(jobName, ct);
        if (!started.IsSuccess)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // transient — тик повторит старт

        // Команда StartRestoreAsync (latest — БЕЗ target_time, spec §3.6);
        // 409 = операция уже жива (пере-старт тика). Отказ — transient:
        // супервиз увидит контейнер без операции (IsNotFound) и пересоздаст.
        // Ретрай команды в окне бута приложения (симметрично BackupVerifyProcess):
        // .NET self-contained слушает :8080 не мгновенно — POST в окне бута
        // даёт connection refused, и без ретрая цикл «create → refused →
        // 404 → снос» не сходится, пока бут дольше тика; POST у приёмника
        // 404 не возвращает (202/409) — первый ответ проснувшегося приложения
        // запускает операцию.
        var receiverUrl = await ToolReceiverUrlAsync(engine, startHost, jobName, ct);
        if (receiverUrl is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // transient — супервиз пере-POST
        ReceiverReply launch;
        for (var attempt = 1; ; attempt++)
        {
            launch = await receiver.StartRestoreAsync(
                receiverUrl,
                new StartRestoreRequest(
                    BackupId: backupId,
                    TargetTime: null,
                    RecoveryTimeoutSec: options.RestoreRecoveryTimeoutSec,
                    S3: new S3Context(
                        Endpoints: [.. poolCtx.ReadAll],
                        AccessKey: poolCtx.AccessKey,
                        SecretKey: poolCtx.SecretKey,
                        Bucket: options.S3Bucket,
                        Prefix: $"{cluster}/{shard}",
                        CaPem: BackupCaPem.ReadIfConfigured(options.S3CaBundlePath))), ct);
            if (launch.IsAccepted || launch.IsConflict || !launch.IsTransient || attempt >= 5)
                break;
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
        }
        if (!launch.IsAccepted && !launch.IsConflict)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // transient — супервиз пере-POST

        await journal.WritePhaseAsync(cluster, Op, $"started/{shard}/{id}", claims.InstanceId, null, ct);
        logger.LogInformation("{Op} {cluster}/{shard}: drill tool container {job} started (full backup {backupId})",
            Op, cluster, shard, jobName, backupId);
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Валидационный FAILED — БЕЗ запуска джоба: ключ сразу чистый терминальный
    // итог (контейнера не было — снос не нужен); честный исход «восстановимость
    // не доказана» — оператор видит ровно то, что увидел бы DR.
    private async Task<Result<ProcessOutcome>> FailValidationAsync(
        string cluster, string shard, string backupId, long nowUnix, string error, CancellationToken ct)
    {
        var id = BackupPlanner.NextId([], time.GetUtcNow().UtcDateTime);
        var failed = new DrillState(id, DrillStatus.Failed, backupId, nowUnix,
            FinishedUnix: nowUnix, Error: error);
        var put = await PutDrillAsync(cluster, shard, failed, ct);
        if (!put.IsSuccess)
            return Result<ProcessOutcome>.Failed(put.Error!);
        drillObserver?.Invoke(cluster, shard, "failed"); // t14: FAILED-валидация — итог без джоба
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
        var containerName = BackupNames.DrillContainerName(cluster, shard, drill.Id);

        // t36: пул чтения для досоздачи джоба (env-списки); null/Failed —
        // transient: досоздача следующим тиком, супервизRunning продолжается.
        var poolRead = await s3Factory.ResolveAsync(cluster, ct);
        if (!poolRead.IsSuccess || poolRead.Value is not { } poolCtx)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-pool", claims.InstanceId,
                poolRead.IsSuccess ? "policy has no s3 section — pool not selected" : poolRead.Error!.Message, ct);
            return;
        }

        // Резолв хоста джоба: основной путь — первая нода шарда из portalloc
        // (там джоб создавался); fallback (portalloc/шард исчезли — шард
        // демонтируется) — джоб мог жить на ЛЮБОМ хосте: листим ВСЕ хосты
        // таблицы и супервизим найденный. Лист одного «чужого» хоста дал бы
        // ложное «контейнера нет» (drill-vanished) или досоздачу-дубликат.
        var (engine, host, fallback) = await ResolveSuperviseEngineAsync(snap, shard, ct);
        if (engine is null)
        {
            await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}/{drill.Id}",
                claims.InstanceId, "docker host not resolvable — status left untouched", ct);
            return; // transient — следующий тик
        }

        var prefix = BackupNames.DrillContainerName(cluster, shard, string.Empty);
        var listed = await engine.ListContainersAsync(prefix, all: true, ct);
        if (!listed.IsSuccess)
        {
            await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}/{drill.Id}",
                claims.InstanceId, "docker list unavailable — status left untouched", ct);
            return; // transport-сбой list → transient (тик повторит)
        }
        var container = listed.Value.FirstOrDefault(c =>
            c.Names.Any(n => n.TrimStart('/') == containerName));

        // Fallback-путь: контейнера нет на первом хосте — ищем на остальных (мульти-хост).
        IDockerEngine? foundEngine = container is null ? null : engine;
        if (container is null && fallback)
        {
            (foundEngine, host) = await FindContainerEngineAsync(prefix, containerName, engine, host, ct);
            if (foundEngine is not null)
            {
                var foundList = await foundEngine.ListContainersAsync(prefix, all: true, ct);
                if (!foundList.IsSuccess)
                    return; // transient — найденный хост перестал отвечать, тик повторит
                container = foundList.Value.FirstOrDefault(c =>
                    c.Names.Any(n => n.TrimStart('/') == containerName));
            }
        }

        // Контейнера нет при успешном list — takeover-аномалия (arch/19 §3.6):
        // возраст < бюджета — досоздача по детерминированному имени (create
        // прошедшего тика не дошёл; канон §3.3 п.5 «джоб пересоздаётся по имени
        // идемпотентно»; create не прошёл — имя занято живым джобом → ждём,
        // супервиз следующим тиком); старше — FAILED drill-vanished (+ снос).
        if (container is null)
        {
            var age = nowUnix - drill.StartedUnix;
            if (SupervisionTimeouts.IsTimedOut(drill.StartedUnix, nowUnix, options.DrillTimeoutSec))
            {
                // Вердикт ОДНИМ put сразу с phase=cleaning (spec §2.5): между
                // «терминал» и «снос» нет окна — transient put оставляет ключ
                // RUNNING, вердикт выводится заново по факту отсутствия джоба.
                var vanished = drill with
                {
                    State = DrillStatus.Failed,
                    FinishedUnix = nowUnix,
                    Error = $"drill-vanished: container missing, age {age} s > {options.DrillTimeoutSec} s",
                    Phase = "cleaning",
                };
                var put = await PutDrillAsync(cluster, shard, vanished, ct);
                if (!put.IsSuccess)
                    return; // ключ остаётся RUNNING — вердикт следующим тиком
                await FinishCleanupAsync(cluster, shard, vanished, ct);
                return;
            }

            // Молодой RUNNING без контейнера — идемпотентная досоздача по имени
            // (create/start прошедшего тика не дошёл — transient) + пере-POST
            // (latest; 409-гвард делает команду идемпотентной).
            var contour = options.ContourNetwork is { Length: > 0 };
            int? recreatePort = null;
            if (!contour)
            {
                var probe = Tool.FreePortProbe.Allocate();
                if (!probe.IsSuccess)
                    return; // transient — следующий тик
                recreatePort = probe.Value;
            }
            var recreated = await engine.CreateContainerAsync(
                Tool.ToolJobSpec.Build(options, cluster, containerName,
                    BackupNames.DrillVolumeName(cluster, shard, drill.Id),
                    "/home/postgres/pgdata", recreatePort), containerName, ct);
            if (!recreated.IsSuccess)
                return; // transient — следующий тик повторит
            await engine.StartContainerAsync(containerName, ct);
            var recreateUrl = await ToolReceiverUrlAsync(engine, host, containerName, ct);
            if (recreateUrl is null)
                return; // transient — адрес недоступен, POST следующим тиком
            var poolRecreate = await s3Factory.ResolveAsync(cluster, ct);
            if (poolRead.IsSuccess && poolRecreate.Value is { } recreatePool)
            {
                await receiver.StartRestoreAsync(recreateUrl, new StartRestoreRequest(
                    BackupId: drill.BackupId,
                    TargetTime: null,
                    RecoveryTimeoutSec: options.RestoreRecoveryTimeoutSec,
                    S3: new S3Context(
                        Endpoints: [.. recreatePool.ReadAll],
                        AccessKey: recreatePool.AccessKey,
                        SecretKey: recreatePool.SecretKey,
                        Bucket: options.S3Bucket,
                        Prefix: $"{cluster}/{shard}",
                        CaPem: BackupCaPem.ReadIfConfigured(options.S3CaBundlePath))), ct);
            }
            return;
        }

        // Бюджет активного дрилла — возраст RUNNING-ключа (etcd-факт + часы):
        // исчерпан → FAILED drill-timeout → снос (kill+rm через cleaning).
        if (SupervisionTimeouts.IsTimedOut(drill.StartedUnix, nowUnix, options.DrillTimeoutSec))
        {
            // Вердикт ОДНИМ put сразу с phase=cleaning (spec §2.5; повторный
            // тик по RUNNING перепроверит возраст — вердикт идемпотентен).
            var timedOut = drill with
            {
                State = DrillStatus.Failed,
                FinishedUnix = nowUnix,
                Error = $"drill-timeout: {nowUnix - drill.StartedUnix} s > {options.DrillTimeoutSec} s",
                Phase = "cleaning",
            };
            var put = await PutDrillAsync(cluster, shard, timedOut, ct);
            if (!put.IsSuccess)
                return; // ключ остаётся RUNNING — вердикт следующим тиком
            await FinishCleanupAsync(cluster, shard, timedOut, ct);
            return;
        }

        var jobEngine = foundEngine ?? engine; // супервиз — на хосте найденного контейнера

        if (container.State is "created")
        {
            await jobEngine.StartContainerAsync(containerName, ct); // создан, но не стартован
            return;
        }

        // Итог — ТОЛЬКО по API (t26): exit-инспект/логи упразднены.
        var receiverUrl = await ToolReceiverUrlAsync(jobEngine, host, containerName, ct);
        if (receiverUrl is null)
            return; // transient — адрес недоступен, статус ключа не трогаем
        var reply = await receiver.GetRestoreAsync(receiverUrl, ct);

        if (reply.IsNotFound)
        {
            // Контейнер жив, операции нет (рестарт приёмника) — transient:
            // снос контейнера; следующий тик досоздаст и повторит POST.
            await jobEngine.RemoveContainerAsync(containerName, force: true, ct);
            await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}/{drill.Id}",
                claims.InstanceId, "drill operation vanished — container recreated", ct);
            return;
        }

        if (reply.IsTransient || reply.Status is not { } snapshot)
            return; // transient тика — статус ключа не трогаем

        switch (snapshot.State)
        {
            case "running":
                // Фаза операции (downloading|recovering) — в ключ при изменении.
                if (snapshot.Phase is { Length: > 0 } p && p != drill.Phase)
                    await PutDrillAsync(cluster, shard, drill with { Phase = p }, ct); // fail — тик повторит
                return;

            case "succeeded":
            {
                // старт postgres + выход из recovery доказаны (restored_to_lsn).
                // Вердикт ОДНИМ put сразу с phase=cleaning (spec §2.5).
                var terminal = drill with
                {
                    State = DrillStatus.Succeeded,
                    FinishedUnix = nowUnix,
                    RestoredToLsn = snapshot.RestoredToLsn,
                    Phase = "cleaning",
                };
                var putTerminal = await PutDrillAsync(cluster, shard, terminal, ct);
                if (!putTerminal.IsSuccess)
                    return; // ключ остаётся RUNNING — вердикт выведется заново
                await FinishCleanupAsync(cluster, shard, terminal, ct);
                return;
            }

            case "cancelled":
            {
                // Операторская отмена (POST /api/ops/cancel tool-контейнера —
                // внешний канал мимо воркера; снимок state=cancelled отличает
                // её от vanished): scratch-контейнер и volume сносятся
                // (следов не осталось), ключ дрилла УДАЛЯЕТСЯ — операций нет,
                // записи нет; никакого FAILED (arch/19 §3.6; следующий дрилл
                // шарда — по общим правилам отбора).
                await jobEngine.RemoveContainerAsync(containerName, force: true, ct);
                var rmVolume = await jobEngine.RemoveVolumeAsync(
                    BackupNames.DrillVolumeName(cluster, shard, drill.Id), ct);
                if (!rmVolume.IsSuccess)
                    return; // volume не снесся — ключ не удаляем, тик повторит ветку
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
                // МАРКЕР ТРАНЗИЕНТА: сбой скачивания — операция ЗАВЕРШИЛАСЬ,
                // контейнер сносим; следующий тик досоздаст и повторит POST.
                await jobEngine.RemoveContainerAsync(containerName, force: true, ct);
                await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}/{drill.Id}",
                    claims.InstanceId, snapshot.Error ?? "download failed", ct);
                return;
            }

            case "failed":
            {
                // PERMANENT (recovering): терминальный FAILED + cleaning → снос.
                var terminal = drill with
                {
                    State = DrillStatus.Failed,
                    FinishedUnix = nowUnix,
                    Error = snapshot.Error ?? "drill operation failed",
                    Phase = "cleaning",
                };
                var putTerminal = await PutDrillAsync(cluster, shard, terminal, ct);
                if (!putTerminal.IsSuccess)
                    return;
                await FinishCleanupAsync(cluster, shard, terminal, ct);
                return;
            }
        }
    }

    // Адрес API tool-контейнера drill (spec §3.1): контур — alias имени :8080
    // в сети контура; прод — инспект публикации :8080 (динамический порт);
    // недоступен — null (transient тика: статус ключа не трогаем).
    private async Task<string?> ToolReceiverUrlAsync(
        IDockerEngine engine, string host, string containerName, CancellationToken ct)
    {
        if (options.ContourNetwork is { Length: > 0 })
            return WalReceiverAddress.ForTool(containerName, host, null, contourMode: true);
        var inspect = await engine.InspectContainerAsync(containerName, ct);
        if (!inspect.IsSuccess)
            return null;
        var published = inspect.Value.Ports
            .FirstOrDefault(p => p.ContainerPort == 8080)?.HostPort;
        return published is null
            ? null
            : WalReceiverAddress.ForTool(containerName, host, published, contourMode: false);
    }

    // ── Доводимый снос контура после терминального исхода (все пути сходятся) ──

    // Мульти-хост: джоб жил на первой ноде шарда НА МОМЕНТ ЗАПУСКА — краш-рекавери
    // (терминальный ключ с cleaning) и повторные тики не знают, на каком хосте он
    // был; portalloc мог уже смениться. Поэтому снос перебирает ВСЮ таблицу
    // Docker:Hosts: rm контейнера+volume по детерминированному имени на КАЖДОМ
    // (идемпотентно, 404 = ok), чистый итог — только после подтверждения
    // отсутствия контейнера на всех хостах (иначе rm «чужого» хоста 404-успехом
    // зачитал бы снос, а реальный контур остался бы навсегда — AC4).
    private async Task FinishCleanupAsync(
        string cluster, string shard, DrillState drill, CancellationToken ct)
    {
        var hosts = await driver.GetHostsAsync(ct);
        if (!hosts.IsSuccess || hosts.Value.Count == 0)
            return; // transient — ключ остаётся как есть, следующий тик повторит

        var containerName = BackupNames.DrillContainerName(cluster, shard, drill.Id);

        // journal-before-manipulations: факт сноса — ДО rm (идемпотентная запись;
        // фаза cleaning уже в ключе — вердикт писался одним put с ней, окно
        // «терминал без cleaning», теряющего снос, исключено).
        await journal.WritePhaseAsync(cluster, Op, $"drill-cleanup/{shard}/{drill.Id}",
            claims.InstanceId, null, ct);

        // rm контейнера (force) и volume — одно имя, на КАЖДОМ хосте
        // (404 = ок — идемпотентность); отказ rm (не 404, а реальный сбой) —
        // cleaning остаётся, следующий тик повторит.
        var engines = new List<IDockerEngine>();
        foreach (var host in hosts.Value)
        {
            var hostEngine = driver.EngineFor(host.Name);
            if (hostEngine is null)
                continue;
            engines.Add(hostEngine);
            await hostEngine.RemoveContainerAsync(containerName, force: true, ct);
            var rmVolume = await hostEngine.RemoveVolumeAsync(
                BackupNames.DrillVolumeName(cluster, shard, drill.Id), ct);
            if (!rmVolume.IsSuccess)
                return; // volume не снесся — чистый итог преждевременен
        }

        // Подтверждение: контейнера нет НИ НА ОДНОМ хосте (list по каждому) →
        // чистый итог; list-fail / не пусто / хост не резолвится → ключ остаётся
        // с cleaning, следующий тик повторяет (идемпотентно, переживает рестарт).
        foreach (var hostEngine in engines)
        {
            var confirm = await hostEngine.ListContainersAsync(containerName, all: true, ct);
            if (!confirm.IsSuccess || confirm.Value.Count > 0)
                return;
        }

        var clean = drill with { Phase = null };
        var put = await PutDrillAsync(cluster, shard, clean, ct);
        if (!put.IsSuccess)
            return; // cleaning-фаза осталась — следующий тик перепишет (идемпотентно)
        drillObserver?.Invoke(cluster, shard,
            drill.State == DrillStatus.Succeeded ? "ok" : "failed"); // t14: чистый терминальный итог
        await journal.WritePhaseAsync(cluster, Op,
            $"{(drill.State == DrillStatus.Succeeded ? "drill-done" : "drill-failed")}/{shard}/{drill.Id}",
            claims.InstanceId, null, ct);
        logger.LogInformation("{Op} {cluster}/{shard}: drill {id} — {state}, topology torn down on all hosts ({hosts})",
            Op, cluster, shard, drill.Id, drill.State, hosts.Value.Count);
    }

    // Резолв движка супервиза: (engine, fallback) — fallback = portalloc/шард
    // недоступны, движок = первый хост таблицы Docker:Hosts (образец verify).
    private async Task<(IDockerEngine? Engine, string Host, bool Fallback)> ResolveSuperviseEngineAsync(
        ClusterSnapshot snap, string shard, CancellationToken ct)
    {
        var addresses = await shardEndpoints.ReadPortAllocAsync(snap.Config.Cluster, ct);
        var firstNode = snap.Shards
            .FirstOrDefault(s => s.Name == shard)?.Nodes
            .Select(n => n.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .FirstOrDefault();
        if (addresses.IsSuccess && firstNode is not null
            && addresses.Value.TryGetValue($"{shard}/{firstNode}", out var addr))
            return (driver.EngineFor(addr.Host), addr.Host, false);

        var hosts = await driver.GetHostsAsync(ct); // таблица Docker:Hosts (канон)
        var first = hosts.IsSuccess ? hosts.Value.FirstOrDefault() : null;
        return (first is null ? null : driver.EngineFor(first.Name),
            first?.Name ?? string.Empty, true);
    }

    // Fallback-поиск джоба на остальных хостах таблицы (мульти-хост): движок,
    // где найден контейнер по имени; не найден/чужой хост недоступен — null.
    private async Task<(IDockerEngine? Engine, string Host)> FindContainerEngineAsync(
        string prefix, string containerName, IDockerEngine skip, string skipHost, CancellationToken ct)
    {
        var hosts = await driver.GetHostsAsync(ct);
        if (!hosts.IsSuccess)
            return (null, string.Empty);
        foreach (var host in hosts.Value)
        {
            var hostEngine = driver.EngineFor(host.Name);
            if (hostEngine is null || ReferenceEquals(hostEngine, skip) || host.Name == skipHost)
                continue;
            var listed = await hostEngine.ListContainersAsync(prefix, all: true, ct);
            if (!listed.IsSuccess)
                continue; // хост недоступен — ищем дальше
            if (listed.Value.Any(c => c.Names.Any(n => n.TrimStart('/') == containerName)))
                return (hostEngine, host.Name);
        }

        return (null, string.Empty);
    }

    private async Task<(IDockerEngine? Engine, string Host)> EngineForShardAsync(
        ClusterSnapshot snap, string shard, CancellationToken ct)
    {
        var addresses = await shardEndpoints.ReadPortAllocAsync(snap.Config.Cluster, ct);
        var firstNode = snap.Shards
            .FirstOrDefault(s => s.Name == shard)?.Nodes
            .Select(n => n.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .FirstOrDefault();
        if (addresses.IsSuccess && firstNode is not null
            && addresses.Value.TryGetValue($"{shard}/{firstNode}", out var addr))
            return (driver.EngineFor(addr.Host), addr.Host);

        var hosts = await driver.GetHostsAsync(ct); // таблица Docker:Hosts (канон)
        var first = hosts.IsSuccess ? hosts.Value.FirstOrDefault() : null;
        return (first is null ? null : driver.EngineFor(first.Name), first?.Name ?? string.Empty);
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

    private long NowUnix() => time.GetUtcNow().ToUnixTimeSeconds();
}
