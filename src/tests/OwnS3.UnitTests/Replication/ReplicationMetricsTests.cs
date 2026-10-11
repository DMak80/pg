using System.Diagnostics.Metrics;
using OwnS3.Replication;

namespace OwnS3.UnitTests.Replication;

// Existing-регресс метрик репликации (t35, глава 07 §12): соседей нет → инструменты
// НЕ создаются — ни одной ownS3_replication_-серии в Meter (детерминированно, через
// MeterListener; WAF-scrape в общем процессе захватывает серии чужих хостов).
public class ReplicationMetricsDisabledTests
{
    [Fact]
    public void Disabled_NoInstrumentsCreated()
    {
        // Arrange — метрики неактивной репликации (соседей нет) на своём Meter
        using var meter = new Meter("owns3.test.disabled");
        var metrics = new ReplicationMetrics(meter, enabled: false);

        // Act — инкременты (все точки) + чтение observable-инструментов слушателем
        metrics.SetLinkOnline("peer-a", true);
        metrics.SetQueueDepth("peer-a", 5);
        metrics.SetDeliveryLagSeconds("peer-a", 10);
        metrics.SetFetchQueueDepth("peer-a", 3);
        metrics.SetFetchPaused("peer-a", true);
        metrics.SetReconcilePosition("peer-a", 7);
        metrics.Notified("peer-a", "accepted");
        metrics.Retried("peer-a");
        metrics.Fetched("peer-a");
        metrics.Applied("peer-a");
        metrics.Skipped("peer-a");
        metrics.Conflict("peer-a");

        var collected = new List<string>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, _) =>
        {
            if (instrument.Meter.Name == "owns3.test.disabled")
                collected.Add(instrument.Name);
        };
        listener.Start();

        // Assert — ни одного ownS3.replication_-инструмента в Meter
        collected.Should().NotContain(name => name.StartsWith("ownS3.replication_", StringComparison.Ordinal));
    }

    [Fact]
    public void Enabled_InstrumentsCreated()
    {
        // Arrange — активная репликация
        using var meter = new Meter("owns3.test.enabled");
        var metrics = new ReplicationMetrics(meter, enabled: true,
            outboxBytes: () => 42, outboxBudgetExceeded: () => 0);

        // Act — чтение observable-инструментов слушателем
        var collected = new List<string>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, _) =>
        {
            if (instrument.Meter.Name == "owns3.test.enabled")
                collected.Add(instrument.Name);
        };
        listener.Start();

        // Assert — инструменты словаря главы 07 §10 зарегистрированы (вкл. outbox-серии)
        collected.Should().Contain("ownS3.replication.outbox.bytes");
        collected.Should().Contain("ownS3.replication.outbox.budget.exceeded");
        collected.Should().Contain("ownS3.replication.queue.depth");
        collected.Should().Contain("ownS3.replication.applied.total");
    }
}
