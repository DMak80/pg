using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace OwnS3.Replication;

// Метрики mesh-репликации (t35, глава 07 §10; каркас Shared.Metrics, Meter "ownS3").
// Task 11 — исходящие серии notifier'а: link_online/queue_depth/delivery_lag_seconds
// (per-target gauge) + notified_total{target,result}/retries_total (счётчики).
// Приёмные fetch-серии и IOutcomeCounters добавляет задача 12, reconcile.position — 13,
// outbox-серии (outbox_bytes/budget_exceeded) — 15 (доведение словаря до полного).
// Экспорт — при активной репликации (без соседей серий нет, Existing-регресс).
public sealed class ReplicationMetrics
{
    private readonly ConcurrentDictionary<string, long> _linkOnline = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _queueDepth = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, double> _deliveryLagSeconds = new(StringComparer.Ordinal);

    private readonly Counter<long> _notified;
    private readonly Counter<long> _retries;

    public ReplicationMetrics(Meter meter)
    {
        _notified = meter.CreateCounter<long>("ownS3.replication.notified.total", unit: "{event}",
            description: "Исходы рассылки уведомлений соседям (accepted/backoff/retry)");
        _retries = meter.CreateCounter<long>("ownS3.replication.retries.total", unit: "{retry}",
            description: "Повторы рассылки уведомлений");
        meter.CreateObservableGauge<long>("ownS3.replication.link.online",
            () => _linkOnline.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("target", p.Key))),
            description: "1 — сосед онлайн по исходам notifier");
        meter.CreateObservableGauge<long>("ownS3.replication.queue.depth",
            () => _queueDepth.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("target", p.Key))),
            description: "Событий журнала после watermark приёма соседа");
        meter.CreateObservableGauge<double>("ownS3.replication.delivery.lag.seconds",
            () => _deliveryLagSeconds.Select(p => new Measurement<double>(p.Value, new KeyValuePair<string, object?>("target", p.Key))),
            unit: "s", description: "Возраст старейшего события, не подтверждённого приёмом (лаг уведомлений)");
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

    // Внутренние значения (снапшот статуса Task 14/юнит-ассерты).
    internal long LinkOnlineOf(string target) => _linkOnline.TryGetValue(target, out var v) ? v : 0;
    internal long QueueDepthOf(string target) => _queueDepth.TryGetValue(target, out var v) ? v : 0;
    internal double DeliveryLagOf(string target) => _deliveryLagSeconds.TryGetValue(target, out var v) ? v : 0;
}
