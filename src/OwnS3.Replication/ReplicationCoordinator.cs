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
    ReplicationOptions options, string originHostId, Func<long> getFreeDiskBytes,
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
        if (!targets.Active)
        {
            logger.LogInformation("Репликация неактивна (соседей нет) — координатор не запускается");
            return;
        }

        // Маркер: при старте — сразу грязный (до готовности, глава 07 §7)
        state.MarkDirty();
        var dirtyStart = !state.WasCleanShutdown();
        if (dirtyStart)
        {
            // Грязная остановка (крах/kill без drain; «пустой том» — тот же маркер):
            // стартовый reconcile-проход по всем соседям (глава 07 §9, триггеры 2/3 —
            // вызов ReconcileRunner подключается задачей 13)
            logger.LogWarning("Грязный старт репликации (маркер state.json) — стартовый reconcile по всем соседям");
        }

        StartExecutors(targets.Snapshot);
        targets.TargetsChanged += OnTargetsChanged;

        // Цикл обслуживания: проход при старте и периодически 60 с (§8-а)
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
                    counters, getFreeDiskBytes, options, timeProvider, fetcherLogger);
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
        StartExecutors(snapshot);
    }

    // Цикл обслуживания: проход при старте и каждые 60 с (§8-а; graceful по cancel).
    private async Task RunMaintenanceLoopAsync(CancellationToken ct)
    {
        var timerPeriod = MaintenancePeriod;
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
        using var drainCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        drainCts.CancelAfter(TimeSpan.FromSeconds(30)); // бюджет drain (глава 07 §7)
        foreach (var fetcher in fetchers)
        {
            try
            {
                await fetcher.DrainAsync(drainCts.Token);
            }
            catch (OperationCanceledException)
            {
                logger.LogWarning("Drain fetch-очередей не уложился в 30 с — остаток закроет стартовый reconcile");
            }
        }

        CancelExecutors();
        cursors.Flush();                 // graceful-персист watermark приёма (§5)
        state.MarkCleanAndPersist();     // маркер чистого завершения (§7)
        logger.LogInformation("Репликация остановлена штатно (drain+flush+clean)");
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
