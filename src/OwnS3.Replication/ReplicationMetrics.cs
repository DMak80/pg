using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using OwnS3.Replication.Inbound;

namespace OwnS3.Replication;

// Метрики mesh-репликации (t35, глава 07 §10; каркас Shared.Metrics, Meter "ownS3").
// Исходящие серии notifier'а: link_online/queue_depth/delivery_lag_seconds
// (per-target gauge) + notified_total{target,result}/retries_total (счётчики).
// Приёмные серии (Task 12): fetch_queue_depth/fetch_lag_seconds/fetch_paused (gauge)
// + fetched_total/applied_total{source,result} (счётчики) + conflicts_total;
// ReplicationMetrics — боевой IOutcomeCounters (точки инкремента — inbox и фетчер,
// каждый по результату СВОЕГО вызова). reconcile.position — задача 13,
// outbox-серии (outbox_bytes/budget_exceeded) — 15. Экспорт — при активной репликации.
public sealed class ReplicationMetrics : IOutcomeCounters
{
    private readonly ConcurrentDictionary<string, long> _linkOnline = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _queueDepth = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, double> _deliveryLagSeconds = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _fetchQueueDepth = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, double> _fetchLagSeconds = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _fetchPaused = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _reconcilePosition = new(StringComparer.Ordinal);
    // Внутренние long-счётчики per-name (снапшот статуса, глава 07 §10 / Task 14)
    private readonly ConcurrentDictionary<string, long> _notifiedAccepted = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _notifiedBackoff = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _notifiedRetry = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _fetchedByName = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _appliedByName = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _skippedByName = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _conflictByName = new(StringComparer.Ordinal);

    private readonly Counter<long>? _notified;
    private readonly Counter<long>? _retries;
    private readonly Counter<long>? _fetched;
    private readonly Counter<long>? _applied;
    private readonly Counter<long>? _conflictsCounter;

    // Делегаты outbox-серий (журнал известен точке сборки — Program; null — серии нет).
    private readonly Func<long>? _outboxBytes;
    private readonly Func<long>? _outboxBudgetExceeded;

    // enabled=false (соседей нет, Existing-регресс): инкременты no-op, gauge-колбэки
    // возвращают пусто → серии не экспортируются. ПЕРЕКЛЮЧАЕМО (SetEnabled при
    // TargetsChanged): узел, стартовавший без соседей и активированный hot-apply,
    // начинает экспортировать серии без рестарта (глава 07 §2, A5/A9).
    private volatile bool _enabled;

    public ReplicationMetrics(Meter meter, bool enabled = true, Func<long>? outboxBytes = null,
        Func<long>? outboxBudgetExceeded = null)
    {
        _enabled = enabled;
        _outboxBytes = outboxBytes;
        _outboxBudgetExceeded = outboxBudgetExceeded;
        _notified = meter.CreateCounter<long>("ownS3.replication.notified.total", unit: "{event}",
            description: "Исходы рассылки уведомлений соседям (accepted/backoff/retry)");
        _retries = meter.CreateCounter<long>("ownS3.replication.retries.total", unit: "{retry}",
            description: "Повторы рассылки уведомлений");
        _fetched = meter.CreateCounter<long>("ownS3.replication.fetched.total", unit: "{object}",
            description: "Успешные фетчи тел у источника");
        _applied = meter.CreateCounter<long>("ownS3.replication.applied.total", unit: "{event}",
            description: "Исходы применения реплики (applied/skipped/conflict)");
        _conflictsCounter = meter.CreateCounter<long>("ownS3.replication.conflicts_total", unit: "{conflict}",
            description: "Зафиксированные LWW-конфликты (журналируются)");
        meter.CreateObservableGauge<long>("ownS3.replication.link.online",
            () => _enabled
                ? _linkOnline.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("target", p.Key)))
                : [],
            description: "1 — сосед онлайн по исходам notifier");
        meter.CreateObservableGauge<long>("ownS3.replication.queue.depth",
            () => _enabled
                ? _queueDepth.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("target", p.Key)))
                : [],
            description: "Событий журнала после watermark приёма соседа");
        meter.CreateObservableGauge<double>("ownS3.replication.delivery.lag.seconds",
            () => _enabled
                ? _deliveryLagSeconds.Select(p => new Measurement<double>(p.Value, new KeyValuePair<string, object?>("target", p.Key)))
                : [],
            unit: "s", description: "Возраст старейшего события, не подтверждённого приёмом (лаг уведомлений)");
        meter.CreateObservableGauge<long>("ownS3.replication.fetch.queue.depth",
            () => _enabled
                ? _fetchQueueDepth.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("source", p.Key)))
                : [],
            description: "Ключей в fetch-очереди приёмника");
        meter.CreateObservableGauge<double>("ownS3.replication.fetch.lag.seconds",
            () => _enabled
                ? _fetchLagSeconds.Select(p => new Measurement<double>(p.Value, new KeyValuePair<string, object?>("source", p.Key)))
                : [],
            unit: "s", description: "Возраст старейшего принятого-но-несохранённого ключа (лаг данных)");
        meter.CreateObservableGauge<long>("ownS3.replication.fetch.paused",
            () => _enabled
                ? _fetchPaused.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("source", p.Key)))
                : [],
            description: "1 — фетчи на паузе (нет места)");
        meter.CreateObservableGauge<long>("ownS3.replication.reconcile.position",
            () => _enabled
                ? _reconcilePosition.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("source", p.Key)))
                : [],
            description: "Прогресс reconcile (обработано ключей)");
        if (_outboxBytes is not null)
            meter.CreateObservableGauge<long>("ownS3.replication.outbox.bytes",
                () => _enabled ? [new Measurement<long>(_outboxBytes())] : [],
                unit: "By", description: "Текущий размер журнала доставки");
        if (_outboxBudgetExceeded is not null)
            meter.CreateObservableGauge<long>("ownS3.replication.outbox.budget.exceeded",
                () => _enabled ? [new Measurement<long>(_outboxBudgetExceeded())] : [],
                description: "1 — бюджет журнала превышен (алерт-состояние)");
    }

    // Переключение экспорта (глава 07 §2: hot-apply состава). Включение — серии
    // появляются с первыми данными; выключение — gauge-колбэки пусты, инкременты no-op.
    public void SetEnabled(bool enabled) => _enabled = enabled;

    public void SetLinkOnline(string target, bool online)
    {
        if (!_enabled) return;
        _linkOnline[target] = online ? 1 : 0;
    }

    public void SetQueueDepth(string target, long depth)
    {
        if (!_enabled) return;
        _queueDepth[target] = depth;
    }

    public void SetDeliveryLagSeconds(string target, double seconds)
    {
        if (!_enabled) return;
        _deliveryLagSeconds[target] = seconds;
    }

    public void Notified(string target, string result)
    {
        if (!_enabled) return;
        _notified?.Add(1, new KeyValuePair<string, object?>("target", target),
            new KeyValuePair<string, object?>("result", result));
        var bucket = result switch
        {
            "accepted" => _notifiedAccepted,
            "backoff" => _notifiedBackoff,
            "retry" => _notifiedRetry,
            _ => null,
        };
        if (bucket is not null)
            bucket.AddOrUpdate(target, 1, (_, v) => v + 1);
    }

    public void Retried(string target)
    {
        if (!_enabled) return;
        _retries?.Add(1, new KeyValuePair<string, object?>("target", target));
    }

    // ── приёмные серии + IOutcomeCounters (точки инкремента — inbox и фетчер) ──

    public void SetFetchQueueDepth(string source, long depth)
    {
        if (!_enabled) return;
        _fetchQueueDepth[source] = depth;
    }

    public void SetFetchLagSeconds(string source, double seconds)
    {
        if (!_enabled) return;
        _fetchLagSeconds[source] = seconds;
    }

    public void SetFetchPaused(string source, bool paused)
    {
        if (!_enabled) return;
        _fetchPaused[source] = paused ? 1 : 0;
    }

    public void SetReconcilePosition(string source, long processed)
    {
        if (!_enabled) return;
        _reconcilePosition[source] = processed;
    }

    public void Fetched(string source)
    {
        if (!_enabled) return;
        _fetched?.Add(1, new KeyValuePair<string, object?>("source", source));
        _fetchedByName.AddOrUpdate(source, 1, (_, v) => v + 1);
    }

    public void Applied(string source) => ApplyOutcome(source, "applied");

    public void Skipped(string source) => ApplyOutcome(source, "skipped");

    public void Conflict(string source)
    {
        if (!_enabled) return;
        // Конфликт — исход применения (applied_total{source,conflict}) И общий
        // счётчик конфликтов (глава 07 §10); событие ConflictDetected аплайера —
        // только лог (без инкремента — нет sourceName, мульти-счёт)
        ApplyOutcome(source, "conflict");
        Interlocked.Increment(ref _conflictsTotal);
        _conflictsCounter?.Add(1);
    }

    private void ApplyOutcome(string source, string result)
    {
        if (!_enabled) return;
        _applied?.Add(1, new KeyValuePair<string, object?>("source", source),
            new KeyValuePair<string, object?>("result", result));
        var bucket = result switch
        {
            "applied" => _appliedByName,
            "skipped" => _skippedByName,
            "conflict" => _conflictByName,
            _ => null,
        };
        if (bucket is not null)
            bucket.AddOrUpdate(source, 1, (_, v) => v + 1);
    }

    // Снапшот счётчиков исходов соседа (имя фигурирует и как {target}, и как {source}).
    public SourceCounters SnapshotCounters(string neighborName) => new(
        _notifiedAccepted.GetValueOrDefault(neighborName),
        _notifiedBackoff.GetValueOrDefault(neighborName),
        _notifiedRetry.GetValueOrDefault(neighborName),
        _fetchedByName.GetValueOrDefault(neighborName),
        _appliedByName.GetValueOrDefault(neighborName),
        _skippedByName.GetValueOrDefault(neighborName),
        _conflictByName.GetValueOrDefault(neighborName));

    private long _conflictsTotal;

    // Конфликтов всего (снапшот статуса Task 14).
    public long ConflictsTotal => Interlocked.Read(ref _conflictsTotal);

    // Текущие значения (снапшот статуса главы 07 §10 — админ-грань; юнит-ассерты).
    public long LinkOnlineOf(string target) => _linkOnline.TryGetValue(target, out var v) ? v : 0;
    public long QueueDepthOf(string target) => _queueDepth.TryGetValue(target, out var v) ? v : 0;
    public double DeliveryLagOf(string target) => _deliveryLagSeconds.TryGetValue(target, out var v) ? v : 0;
    public long FetchQueueDepthOf(string source) => _fetchQueueDepth.TryGetValue(source, out var v) ? v : 0;
    public double FetchLagOf(string source) => _fetchLagSeconds.TryGetValue(source, out var v) ? v : 0;
    public long FetchPausedOf(string source) => _fetchPaused.TryGetValue(source, out var v) ? v : 0;
    public long ReconcilePositionOf(string source) => _reconcilePosition.TryGetValue(source, out var v) ? v : 0;
}

// Снапшот счётчиков исходов соседа (статус /admin/replication/status — глава 07 §10;
// имя фигурирует и как {target}, и как {source}).
public sealed record SourceCounters(long NotifiedAccepted, long NotifiedBackoff, long NotifiedRetry,
    long Fetched, long Applied, long Skipped, long Conflict);
