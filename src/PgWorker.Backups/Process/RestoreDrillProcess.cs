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
/// (reliability t02, arch/19 §3.6) под клэймом &lt;C&gt; (Op backup-drill):
/// супервиз активного drill-джоба pgw-backup-drill-&lt;C&gt;-&lt;X&gt;-&lt;id&gt; (фазы
/// downloading|recovering из логов; exit-код+result-JSON — истина итога),
/// доводимый снос контура фазой cleaning (journal-before-rm; переживает
/// рестарт воркера — идемпотентная доводка по детерминированным именам),
/// отбор «один шард за проход, наименее свежий по дриллу» (DrillPlanner),
/// валидация кандидата — путь restore-заявки t05 (манифест + WalChain),
/// запуск джоба; таймаут-бюджет DrillTimeoutSec → FAILED drill-timeout,
/// RUNNING без контейнера старше бюджета → FAILED drill-vanished.
/// Control — «старт postgres + выход из recovery» (restored_to_lsn);
/// SQL-проверок данных нет. Состояние — один etcd-ключ
/// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/drill (перезаписывается).
/// </summary>
public sealed class RestoreDrillProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ShardEndpoints shardEndpoints,
    IBackupS3 s3,
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
                $"{Op} {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

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
                $"полный {backupId} без backup_manifest (недокачан/бит)", ct);

        // 2. Стартовая точка WAL: etcd-статус; нет — backup_label из S3.
        var walStart = fresh.WalStartSegment;
        if (walStart is not { Length: > 0 })
        {
            var label = await S3Pulse.CallAsync(progress, token => s3.DownloadTextAsync(cluster, shard, $"full/{backupId}/backup_label", token), ct, WatchdogWindow / 2);
            walStart = label.IsSuccess ? Restore.BackupLabel.WalStartSegment(label.Value) : null;
            if (walStart is null)
                return await FailValidationAsync(cluster, shard, backupId, nowUnix,
                    $"full/{backupId}: backup_label недоступен/бит", ct);
        }
        var chainStart = WalFileName.TryParse(walStart);
        if (chainStart is null)
            return await FailValidationAsync(cluster, shard, backupId, nowUnix,
                $"full/{backupId}: wal_start '{walStart}' не разбирается", ct);

        // 3. Непрерывность WAL-цепочки (WalChain t03); S3-отказ → transient
        // (статус не трогаем — тик повторит).
        var wal = await S3Pulse.CallAsync(progress, token => s3.ListWalAsync(cluster, shard, ct: token), ct, WatchdogWindow / 2);
        if (!wal.IsSuccess)
        {
            await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}",
                claims.InstanceId, "s3 list недоступен — валидация следующим тиком", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        var chain = WalChain.Check(chainStart.Value, wal.Value.Select(o => o.Name));
        if (!chain.IsContinuous)
            return await FailValidationAsync(cluster, shard, backupId, nowUnix,
                chain.GapError ?? "дыра WAL-цепочки", ct);

        // ── Запуск: journal-before-manipulations — put RUNNING до create
        // (arch/17); хост — первая нода шарда из portalloc (как restore-джоб).
        var id = BackupPlanner.NextId([], time.GetUtcNow().UtcDateTime);
        var engine = await EngineForShardAsync(snap, shard, ct);
        if (engine is null)
        {
            await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}",
                claims.InstanceId, "docker-хост не резолвится — запуск следующим тиком", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        var running = new DrillState(id, DrillStatus.Running, backupId, nowUnix);
        var put = await PutDrillAsync(cluster, shard, running, ct);
        if (!put.IsSuccess)
            return Result<ProcessOutcome>.Failed(put.Error!);

        var jobName = BackupNames.DrillContainerName(cluster, shard, id);
        // Поллинг create/start джоба (arch/14 §6 инвариант поллинга; аудит
        // spec §3.2): ensure-семантика — контейнер уже есть → только старт.
        var launched = await LongCallPolling.EnsureAsync(
            $"create/start drill-джоба {jobName}",
            async token =>
            {
                var list = await engine.ListContainersAsync(jobName, all: true, token);
                if (!list.IsSuccess)
                    return Result.Failed(list.Error!);
                if (list.Value.Any(c => c.Names.Contains(jobName)))
                    return await engine.StartContainerAsync(jobName, token);
                var created = await engine.CreateContainerAsync(
                    DrillJobSpec.Build(options, cluster, shard, id, backupId), jobName, token);
                if (!created.IsSuccess)
                    return created;
                return await engine.StartContainerAsync(jobName, token);
            },
            progress, logger,
            TimeSpan.FromTicks(Math.Max(TimeSpan.TicksPerSecond, WatchdogWindow.Ticks / 2)),
            JobCreateBudget, ct);
        if (!launched.IsSuccess)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // transient — RUNNING остаётся, тик досоздаст по имени

        await journal.WritePhaseAsync(cluster, Op, $"started/{shard}/{id}", claims.InstanceId, null, ct);
        logger.LogInformation("{Op} {cluster}/{shard}: drill-джоб {job} запущен (полный {backupId})",
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
        logger.LogWarning("{Op} {cluster}/{shard}: дрилл провален валидацией: {error}",
            Op, cluster, shard, error);
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // ── Супервиз RUNNING-дрилла (arch/19 §3.6 п.2) ──

    private async Task SuperviseRunningAsync(
        ClusterSnapshot snap, string shard, DrillState drill, long nowUnix, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        var containerName = BackupNames.DrillContainerName(cluster, shard, drill.Id);

        // Резолв хоста джоба: основной путь — первая нода шарда из portalloc
        // (там джоб создавался); fallback (portalloc/шард исчезли — шард
        // демонтируется) — джоб мог жить на ЛЮБОМ хосте: листим ВСЕ хосты
        // таблицы и супервизим найденный. Лист одного «чужого» хоста дал бы
        // ложное «контейнера нет» (drill-vanished) или досоздачу-дубликат.
        var (engine, fallback) = await ResolveSuperviseEngineAsync(snap, shard, ct);
        if (engine is null)
        {
            await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}/{drill.Id}",
                claims.InstanceId, "docker-хост не резолвится — статус не трогаем", ct);
            return; // transient — следующий тик
        }

        var prefix = BackupNames.DrillContainerName(cluster, shard, string.Empty);
        var listed = await engine.ListContainersAsync(prefix, all: true, ct);
        if (!listed.IsSuccess)
        {
            await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}/{drill.Id}",
                claims.InstanceId, "docker list недоступен — статус не трогаем", ct);
            return; // transport-сбой list → transient (тик повторит)
        }
        var container = listed.Value.FirstOrDefault(c =>
            c.Names.Any(n => n.TrimStart('/') == containerName));

        // Fallback-путь: джоба нет на первом хосте — ищем на остальных (мульти-хост).
        IDockerEngine? foundEngine = container is null ? null : engine;
        if (container is null && fallback)
        {
            foundEngine = await FindContainerEngineAsync(prefix, containerName, engine, ct);
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
                    Error = $"drill-vanished: контейнер отсутствует, возраст {age} с > {options.DrillTimeoutSec} с",
                    Phase = "cleaning",
                };
                var put = await PutDrillAsync(cluster, shard, vanished, ct);
                if (!put.IsSuccess)
                    return; // ключ остаётся RUNNING — вердикт следующим тиком
                await FinishCleanupAsync(cluster, shard, vanished, ct);
                return;
            }

            // Молодой RUNNING без контейнера — статус не трогаем (transient).
            // Поллинг create/start — тот же инвариант (arch/14 §6).
            await LongCallPolling.EnsureAsync(
                $"create/start drill-джоба {containerName}",
                async token =>
                {
                    var created = await engine.CreateContainerAsync(
                        DrillJobSpec.Build(options, cluster, shard, drill.Id, drill.BackupId), containerName, token);
                    if (!created.IsSuccess)
                        return created;
                    return await engine.StartContainerAsync(containerName, token);
                },
                progress, logger,
                TimeSpan.FromTicks(Math.Max(TimeSpan.TicksPerSecond, WatchdogWindow.Ticks / 2)),
                JobCreateBudget, ct);
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
                Error = $"drill-timeout: {nowUnix - drill.StartedUnix} с > {options.DrillTimeoutSec} с",
                Phase = "cleaning",
            };
            var put = await PutDrillAsync(cluster, shard, timedOut, ct);
            if (!put.IsSuccess)
                return; // ключ остаётся RUNNING — вердикт следующим тиком
            await FinishCleanupAsync(cluster, shard, timedOut, ct);
            return;
        }

        var jobEngine = foundEngine ?? engine; // супервиз — на хосте найденного джоба

        if (container.State is "created")
        {
            await jobEngine.StartContainerAsync(containerName, ct); // создан, но не стартован
            return;
        }

        if (container.State is "running" or "restarting")
        {
            // Фаза джоба (downloading|recovering — протокол t05) — в ключ при
            // изменении; иначе ждём (InProgress — тик не блокируется).
            var logs = await jobEngine.GetContainerLogsAsync(containerName, 200, ct);
            if (!logs.IsSuccess)
                return; // transient — фаза недоступна
            var phase = DrillJobLog.Parse(logs.Value).Phase;
            if (phase is { } p && p != drill.Phase)
                await PutDrillAsync(cluster, shard, drill with { Phase = p }, ct); // fail — тик повторит
            return;
        }

        if (container.State != "exited")
            return; // прочие состояния — вне протокола супервиза

        // Итог: exit-код + result-JSON (истина итога, arch/19 §3.5-образец).
        var inspect = await jobEngine.InspectContainerAsync(containerName, ct);
        if (!inspect.IsSuccess)
            return; // transient — итог недоступен
        var exitCode = inspect.Value.ExitCode ?? -1;
        var exitLogs = await jobEngine.GetContainerLogsAsync(containerName, 200, ct);
        if (!exitLogs.IsSuccess)
            return; // transient — итог недоступен
        var result = DrillJobLog.Parse(exitLogs.Value);

        // Вердикт ОДНИМ put сразу с phase=cleaning (spec §2.5): окно
        // «терминал без cleaning» исключено — transient put оставляет ключ
        // RUNNING, exited-джоб инспектируется заново следующим тиком.
        DrillState terminal;
        if (exitCode == 0 && result.Result is { Ok: true })
        {
            // старт postgres + выход из recovery доказаны (restored_to_lsn).
            terminal = drill with
            {
                State = DrillStatus.Succeeded,
                FinishedUnix = nowUnix,
                RestoredToLsn = result.Result.RestoredToLsn,
                Phase = "cleaning",
            };
        }
        else
        {
            terminal = drill with
            {
                State = DrillStatus.Failed,
                FinishedUnix = nowUnix,
                Error = result.Result is { Ok: false, Error: { } err } ? err : $"exit {exitCode}",
                Phase = "cleaning",
            };
        }

        var putTerminal = await PutDrillAsync(cluster, shard, terminal, ct);
        if (!putTerminal.IsSuccess)
            return; // ключ остаётся RUNNING — вердикт выведется заново следующим тиком
        await FinishCleanupAsync(cluster, shard, terminal, ct);
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
        logger.LogInformation("{Op} {cluster}/{shard}: дрилл {id} — {state}, контур снесён на всех хостах ({hosts})",
            Op, cluster, shard, drill.Id, drill.State, hosts.Value.Count);
    }

    // Резолв движка супервиза: (engine, fallback) — fallback = portalloc/шард
    // недоступны, движок = первый хост таблицы Docker:Hosts (образец verify).
    private async Task<(IDockerEngine? Engine, bool Fallback)> ResolveSuperviseEngineAsync(
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
            return (driver.EngineFor(addr.Host), false);

        var hosts = await driver.GetHostsAsync(ct); // таблица Docker:Hosts (канон)
        var first = hosts.IsSuccess ? hosts.Value.FirstOrDefault() : null;
        return (first is null ? null : driver.EngineFor(first.Name), true);
    }

    // Fallback-поиск джоба на остальных хостах таблицы (мульти-хост): движок,
    // где найден контейнер по имени; не найден/чужой хост недоступен — null.
    private async Task<IDockerEngine?> FindContainerEngineAsync(
        string prefix, string containerName, IDockerEngine skip, CancellationToken ct)
    {
        var hosts = await driver.GetHostsAsync(ct);
        if (!hosts.IsSuccess)
            return null;
        foreach (var host in hosts.Value)
        {
            var hostEngine = driver.EngineFor(host.Name);
            if (hostEngine is null || ReferenceEquals(hostEngine, skip))
                continue;
            var listed = await hostEngine.ListContainersAsync(prefix, all: true, ct);
            if (!listed.IsSuccess)
                continue; // хост недоступен — ищем дальше
            if (listed.Value.Any(c => c.Names.Any(n => n.TrimStart('/') == containerName)))
                return hostEngine;
        }

        return null;
    }

    private async Task<IDockerEngine?> EngineForShardAsync(
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
            return driver.EngineFor(addr.Host);

        var hosts = await driver.GetHostsAsync(ct); // таблица Docker:Hosts (канон)
        var first = hosts.IsSuccess ? hosts.Value.FirstOrDefault() : null;
        return first is null ? null : driver.EngineFor(first.Name);
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

        return Result.Failed(new ApplicationException($"etcd put drill {cluster}/{shard} — все endpoints недоступны"));
    }

    private long NowUnix() => time.GetUtcNow().ToUnixTimeSeconds();
}
