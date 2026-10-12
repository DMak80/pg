using System.Diagnostics.Metrics;
using OwnS3.Replication;

namespace OwnS3.UnitTests.Replication;

// Метрики репликации (t35, глава 07 §10/§12): enabled=false — инкременты no-op и
// gauge-колбэки пусты (серии не экспортируются, Existing-регресс); SetEnabled(true)
// при hot-apply — серии появляются с первыми данными (A5/A9 без рестарта).
public class ReplicationMetricsTests
{
    private static (ReplicationMetrics Metrics, List<string> Published) Create(bool enabled)
    {
        var meter = new Meter("owns3.metrics." + Guid.NewGuid().ToString("N"));
        var metrics = new ReplicationMetrics(meter, enabled: enabled,
            outboxBytes: () => 42, outboxBudgetExceeded: () => 0);
        var published = new List<string>();
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, _) =>
        {
            if (instrument.Meter.Name == meter.Name)
                published.Add(instrument.Name);
        };
        listener.Start();
        return (metrics, published);
    }

    [Fact]
    public void Disabled_IncrementsNoop_GaugesEmpty()
    {
        // Arrange — старт без соседей (Existing-регресс)
        var (metrics, published) = Create(enabled: false);

        // Act — все точки инкремента
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

        // Assert — инкременты no-op (значения нулевые; counters без измерений
        // и gauge-колбэки с пустым списком не экспортируются)
        metrics.LinkOnlineOf("peer-a").Should().Be(0);
        metrics.QueueDepthOf("peer-a").Should().Be(0);
        metrics.FetchPausedOf("peer-a").Should().Be(0);
        metrics.ConflictsTotal.Should().Be(0);
    }

    [Fact]
    public void SetEnabled_AfterHotApply_IncrementsVisible()
    {
        // Arrange — старт без соседей; hot-apply состава (A5/A9)
        var (metrics, _) = Create(enabled: false);
        metrics.SetQueueDepth("peer-a", 3); // до активации — no-op
        metrics.SetEnabled(true);

        // Act
        metrics.SetQueueDepth("peer-a", 7);
        metrics.Conflict("peer-a");

        // Assert — после активации инкременты видны (серии появятся с данными)
        metrics.QueueDepthOf("peer-a").Should().Be(7);
        metrics.ConflictsTotal.Should().Be(1);
    }

    [Fact]
    public void Enabled_InstrumentsCreated()
    {
        // Arrange — активная репликация
        var (metrics, published) = Create(enabled: true);

        // Assert — инструменты словаря главы 07 §10 зарегистрированы (вкл. outbox-серии)
        published.Should().Contain("ownS3.replication.outbox.bytes");
        published.Should().Contain("ownS3.replication.outbox.budget.exceeded");
        published.Should().Contain("ownS3.replication.queue.depth");
        published.Should().Contain("ownS3.replication.applied.total");
    }
}
