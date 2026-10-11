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

    private readonly Counter<long> _notified;
    private readonly Counter<long> _retries;
    private readonly Counter<long> _fetched;
    private readonly Counter<long> _applied;
    private readonly Counter<long> _conflictsCounter;

    public ReplicationMetrics(Meter meter)
    {
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
            () => _linkOnline.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("target", p.Key))),
            description: "1 — сосед онлайн по исходам notifier");
        meter.CreateObservableGauge<long>("ownS3.replication.queue.depth",
            () => _queueDepth.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("target", p.Key))),
            description: "Событий журнала после watermark приёма соседа");
        meter.CreateObservableGauge<double>("ownS3.replication.delivery.lag.seconds",
            () => _deliveryLagSeconds.Select(p => new Measurement<double>(p.Value, new KeyValuePair<string, object?>("target", p.Key))),
            unit: "s", description: "Возраст старейшего события, не подтверждённого приёмом (лаг уведомлений)");
        meter.CreateObservableGauge<long>("ownS3.replication.fetch.queue.depth",
            () => _fetchQueueDepth.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("source", p.Key))),
            description: "Ключей в fetch-очереди приёмника");
        meter.CreateObservableGauge<double>("ownS3.replication.fetch.lag.seconds",
            () => _fetchLagSeconds.Select(p => new Measurement<double>(p.Value, new KeyValuePair<string, object?>("source", p.Key))),
            unit: "s", description: "Возраст старейшего принятого-но-несохранённого ключа (лаг данных)");
        meter.CreateObservableGauge<long>("ownS3.replication.fetch.paused",
            () => _fetchPaused.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("source", p.Key))),
            description: "1 — фетчи на паузе (нет места)");
        meter.CreateObservableGauge<long>("ownS3.replication.reconcile.position",
            () => _reconcilePosition.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("source", p.Key))),
            description: "Прогресс reconcile (обработано ключей)");
    }

    public void SetLinkOnline(string target, bool online) =>
        _linkOnline[target] = online ? 1 : 0;

    public void SetQueueDepth(string target, long depth) =>
        _queueDepth[target] = depth;

    public void SetDeliveryLagSeconds(string target, double seconds) =>
        _deliveryLagSeconds[target] = seconds;

    public void Notified(string target, string result) =>
        _notified.Add(1, new KeyValuePair<string, object?>("target", target),
            new KeyValuePair<string, object?>("result", result));

    public void Retried(string target) => _retries.Add(1, new KeyValuePair<string, object?>("target", target));

    // ── приёмные серии + IOutcomeCounters (точки инкремента — inbox и фетчер) ──

    public void SetFetchQueueDepth(string source, long depth) =>
        _fetchQueueDepth[source] = depth;

    public void SetFetchLagSeconds(string source, double seconds) =>
        _fetchLagSeconds[source] = seconds;

    public void SetFetchPaused(string source, bool paused) =>
        _fetchPaused[source] = paused ? 1 : 0;

    public void SetReconcilePosition(string source, long processed) =>
        _reconcilePosition[source] = processed;

    public void Fetched(string source) =>
        _fetched.Add(1, new KeyValuePair<string, object?>("source", source));

    public void Applied(string source) => ApplyOutcome(source, "applied");

    public void Skipped(string source) => ApplyOutcome(source, "skipped");

    public void Conflict(string source)
    {
        // Конфликт — исход применения (applied_total{source,conflict}) И общий
        // счётчик конфликтов (глава 07 §10); событие ConflictDetected аплайера —
        // только лог (без инкремента — нет sourceName, мульти-счёт)
        ApplyOutcome(source, "conflict");
        Interlocked.Increment(ref _conflictsTotal);
        _conflictsCounter.Add(1);
    }

    private void ApplyOutcome(string source, string result) =>
        _applied.Add(1, new KeyValuePair<string, object?>("source", source),
            new KeyValuePair<string, object?>("result", result));

    private long _conflictsTotal;

    // Конфликтов всего (снапшот статуса Task 14).
    public long ConflictsTotal => Interlocked.Read(ref _conflictsTotal);

    // Внутренние значения (снапшот статуса Task 14/юнит-ассерты).
    internal long LinkOnlineOf(string target) => _linkOnline.TryGetValue(target, out var v) ? v : 0;
    internal long QueueDepthOf(string target) => _queueDepth.TryGetValue(target, out var v) ? v : 0;
    internal double DeliveryLagOf(string target) => _deliveryLagSeconds.TryGetValue(target, out var v) ? v : 0;
    internal long FetchQueueDepthOf(string source) => _fetchQueueDepth.TryGetValue(source, out var v) ? v : 0;
    internal double FetchLagOf(string source) => _fetchLagSeconds.TryGetValue(source, out var v) ? v : 0;
    internal long FetchPausedOf(string source) => _fetchPaused.TryGetValue(source, out var v) ? v : 0;
    internal long ReconcilePositionOf(string source) => _reconcilePosition.TryGetValue(source, out var v) ? v : 0;
}
