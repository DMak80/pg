using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OwnS3.Replication.Cursors;
using OwnS3.Storage;
using OwnS3.Replication.Fetching;
using OwnS3.Replication.Inbound;
using OwnS3.Replication.Lww;
using OwnS3.Replication.Notifier;
using OwnS3.Replication.Outbox;
using OwnS3.Replication.Transport;

namespace OwnS3.Replication;

// Hosted-координатор репликации App (глава 07 §8/§8-а/§9/§11): исполнители per-сосед
// (notifier + фетчер), перестройка по TargetsChanged без рестарта, цикл обслуживания
// (60 с + при старте), graceful stop (drain → flush курсоров → маркер clean).
// Стартовый reconcile при грязном маркере (WasCleanShutdown == false — глава 07 §9):
// «пустой том» покрывается тем же механизмом (нет state.json — маркер грязный);
// триггеры добавления соседа/админ-команды — ReconcileRunner (задача 13).
// Без соседей координатор неактивен (Existing-регресс: ни исполнителей, ни чисток).
public sealed class ReplicationCoordinator(ReplicationTargets targets, OutboxJournal journal,
    TargetCursors cursors, IReplicationTransport transport, ReplicaApplier applier,
    FetchQueueRegistry queues,
    IOutcomeCounters counters, ReplicationState state, ReplicationMaintenance maintenance,
    Reconcile.ReconcileRunner reconcile, ReplicationOptions options, string originHostId,
    Func<long> getFreeDiskBytes,
    TimeProvider timeProvider, ILogger<ReplicationCoordinator> logger,
    ReplicationMetrics metrics, ILogger<NotificationDispatcher> dispatcherLogger,
    ILogger<ObjectFetcher> fetcherLogger) : BackgroundService
{
    // Период цикла обслуживания (канон §8-а: 60 с).
    internal static readonly TimeSpan MaintenancePeriod = TimeSpan.FromSeconds(60);

    private readonly object _lock = new();
    private readonly Dictionary<string, (NotificationDispatcher Dispatcher, ObjectFetcher Fetcher,
        CancellationTokenSource Cts)> _executors = new(StringComparer.Ordinal);
    private readonly List<Task> _executorTasks = [];

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Неактивная репликация (соседей нет): ЖДАТЬ активации — hot-apply состава
        // (PUT targets) включает исполнители без рестарта (глава 07 §2); поллинг 1 с
        while (!targets.Active && !stoppingToken.IsCancellationRequested)
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        if (stoppingToken.IsCancellationRequested)
            return;
        logger.LogInformation("Репликация активна ({Count} соседей) — координатор запущен", targets.Snapshot.Count);
        // Экспорт метрик следует за составом (глава 07 §2): hot-apply состава
        // включает/выключает серии без рестарта (A5/A9)
        targets.TargetsChanged += snapshot => metrics.SetEnabled(snapshot.Count > 0);

        // Маркер: чистота ПРЕДЫДУЩЕЙ остановки — до сброса (глава 07 §7);
        // триггеры reconcile §9 (2) «пустой том» и (3) «грязная остановка» — по нему
        var wasClean = state.WasCleanShutdown();
        state.MarkDirty();
        if (!wasClean)
        {
            logger.LogWarning("Грязный старт репликации (маркер state.json) — стартовый reconcile по всем соседям");
            foreach (var neighbor in targets.Snapshot)
                reconcile.Trigger(neighbor.Name, stoppingToken);
        }

        StartExecutors(targets.Snapshot);
        // Триггер §9 (1): добавление соседа — reconcile по НОВОМУ (в OnTargetsChanged)
        targets.TargetsChanged += OnTargetsChanged;

        // Триггер §9 (4): периодический reconcile по расписанию + цикл обслуживания §8-а
        await RunMaintenanceLoopAsync(stoppingToken);
    }

    // Исполнители per-сосед: notifier + фетчер (неактивные соседи пропускаются).
    private void StartExecutors(IReadOnlyList<TargetRecord> snapshot)
    {
        lock (_lock)
        {
            foreach (var target in snapshot)
            {
                if (_executors.ContainsKey(target.Name))
                    continue;
                var cts = new CancellationTokenSource();
                var dispatcher = new NotificationDispatcher(target, journal, cursors, transport, options,
                    metrics, originHostId, timeProvider, dispatcherLogger);
                var fetcher = new ObjectFetcher(target, queues.ForSource(target.Name), transport, applier,
                    counters, getFreeDiskBytes, options, timeProvider, fetcherLogger, metrics);
                _executors[target.Name] = (dispatcher, fetcher, cts);
                _executorTasks.Add(dispatcher.RunAsync(cts.Token));
                _executorTasks.Add(fetcher.RunAsync(cts.Token));
                logger.LogInformation("Исполнители репликации запущены: {Target}", target.Name);
            }
        }
    }

    // Замена состава на лету (глава 07 §2): убранный — исполнитель останавливается,
    // курсор сохраняется; добавленный — стартуется.
    private void OnTargetsChanged(IReadOnlyList<TargetRecord> snapshot)
    {
        List<string> removed;
        lock (_lock)
        {
            var names = snapshot.Select(t => t.Name).ToHashSet(StringComparer.Ordinal);
            removed = _executors.Keys.Where(n => !names.Contains(n)).ToList();
            foreach (var name in removed)
            {
                var (_, _, cts) = _executors[name];
                cts.Cancel();
                _executors.Remove(name);
                logger.LogInformation("Сосед {Target} выведен — исполнители остановлены (курсор сохранён)", name);
            }
        }
        // Добавленные — те, кого ещё нет среди исполнителей (§9 триггер 1)
        HashSet<string> addedNames;
        lock (_lock)
            addedNames = snapshot.Select(t => t.Name).ToHashSet(StringComparer.Ordinal)
                .Except(_executors.Keys).ToHashSet(StringComparer.Ordinal);
        StartExecutors(snapshot);
        foreach (var name in addedNames)
            reconcile.Trigger(name, CancellationToken.None);
    }

    // Цикл обслуживания: проход при старте и каждые 60 с (§8-а) + периодический
    // reconcile раз в ReconcileIntervalHours (§9 триггер 4); graceful по cancel.
    private async Task RunMaintenanceLoopAsync(CancellationToken ct)
    {
        var timerPeriod = MaintenancePeriod;
        var nextReconcile = timeProvider.GetUtcNow().AddHours(options.ReconcileIntervalHours);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await maintenance.RunMaintenancePassAsync(ct);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Проход обслуживания репликации не удался");
                }
                if (timeProvider.GetUtcNow() >= nextReconcile)
                {
                    foreach (var neighbor in targets.Snapshot)
                        reconcile.Trigger(neighbor.Name, ct);
                    nextReconcile = timeProvider.GetUtcNow().AddHours(options.ReconcileIntervalHours);
                }
                await Task.Delay(timerPeriod, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // graceful stop — штатный выход
        }
    }

    // Graceful stop (глава 07 §7): drain fetch-очередей (бюджет 30 с) → flush курсоров
    // → маркер clean. Исполнители отменяются ПОСЛЕ drain.
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (!targets.Active)
        {
            await base.StopAsync(cancellationToken);
            return;
        }

        var fetchers = GetAllFetchers();
        var drainedClean = true;
        using var drainCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        drainCts.CancelAfter(TimeSpan.FromSeconds(30)); // бюджет drain (глава 07 §7)
        foreach (var fetcher in fetchers)
        {
            try
            {
                await fetcher.DrainAsync(drainCts.Token);
                if (fetcher.HasUnapplied)
                {
                    // Не всё применено (источник недоступен в окне drain) — чистый
                    // маркер НЕ ставить: он заблокирует стартовый reconcile, а дыры
                    // не закрылись бы до периодического (до 24 ч) — глава 07 §7/§9
                    drainedClean = false;
                    logger.LogWarning(
                        "Drain {Fetcher}: есть НЕ применённые ключи — маркер останется грязным (стартовый reconcile закроет дыры)",
                        fetcher.Source.Name);
                }
            }
            catch (OperationCanceledException)
            {
                drainedClean = false;
                logger.LogWarning("Drain fetch-очередей не уложился в 30 с — маркер останется грязным (стартовый reconcile закроет дыры)");
            }
        }

        CancelExecutors();
        cursors.Flush();                 // graceful-персист watermark приёма (§5)
        if (drainedClean)
        {
            state.MarkCleanAndPersist(); // маркер чистого завершения (§7)
            logger.LogInformation("Репликация остановлена штатно (drain+flush+clean)");
        }
        else
        {
            state.MarkDirty();           // грязный маркер → стартовый reconcile (§9, R2)
            logger.LogWarning("Репликация остановлена с незавершённым drain — маркер ГРЯЗНЫЙ (стартовый reconcile)");
        }
        await base.StopAsync(cancellationToken);
    }

    private List<ObjectFetcher> GetAllFetchers()
    {
        lock (_lock)
            return _executors.Values.Select(e => e.Fetcher).ToList();
    }

    private void CancelExecutors()
    {
        lock (_lock)
        {
            foreach (var (_, _, cts) in _executors.Values)
                cts.Cancel();
            _executors.Clear();
        }
    }
}
