using AdminPanel.Api.Inspection;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Хендлеры инспекции: 503-отказ «снапшота нет» и сборка DTO (spec §10.4).
public class InspectionQueryHandlerTests
{
    private readonly FixedTimeProvider _time = new();

    [Fact]
    public async Task OverviewHandle_NoSnapshot_ReturnsFailedSnapshotNotReady()
    {
        // Arrange: до первого тика Current = null (t03 §3.13).
        var handler = new OverviewQueryHandler(new SnapshotStore(), _time, Options.Create(new EtcdOptions()));

        // Act
        var result = await handler.Handle(new OverviewQuery(), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InspectionModule.SnapshotNotReadyException>();
    }

    [Fact]
    public async Task EtcdStatusHandle_NoSnapshot_ReturnsFailedSnapshotNotReady()
    {
        // Arrange
        var handler = new EtcdStatusQueryHandler(new SnapshotStore());

        // Act
        var result = await handler.Handle(new EtcdStatusQuery(), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InspectionModule.SnapshotNotReadyException>();
    }

    [Fact]
    public async Task AlertsHandle_NoSnapshot_ReturnsFailedSnapshotNotReady()
    {
        // Arrange
        var handler = new AlertsQueryHandler(new SnapshotStore());

        // Act
        var result = await handler.Handle(new AlertsQuery(null, null), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InspectionModule.SnapshotNotReadyException>();
    }

    [Fact]
    public async Task OverviewHandle_WithSnapshot_ReturnsDto()
    {
        // Arrange: BuiltAtUtc = фиксированное время теста → возраст 0.
        var store = new SnapshotStore();
        store.Replace(TestSnapshots.Healthy(_time.Utc) with
        {
            Alerts = [new Alert("a:etcd", AlertSeverity.Critical, "a", "etcd", "m", null, null)],
        });
        var handler = new OverviewQueryHandler(store, _time, Options.Create(new EtcdOptions()));

        // Act
        var result = await handler.Handle(new OverviewQuery(), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.AlertsCritical.Should().Be(1);
        result.Value.SnapshotAgeMs.Should().Be(0);
        result.Value.Stale.Should().BeFalse();
    }

    [Fact]
    public async Task AlertsHandler_AppliesFilters()
    {
        // Arrange
        var store = new SnapshotStore();
        store.Replace(TestSnapshots.Healthy(_time.Utc) with
        {
            Alerts =
            [
                new Alert("a:1", AlertSeverity.Critical, "a", "1", "m", null, null),
                new Alert("b:1", AlertSeverity.Warning, "b", "1", "m", null, null),
            ],
        });
        var handler = new AlertsQueryHandler(store);

        // Act
        var critical = await handler.Handle(new AlertsQuery(AlertSeverity.Critical, null), CancellationToken.None);
        var both = await handler.Handle(new AlertsQuery(AlertSeverity.Warning, "b"), CancellationToken.None);
        var none = await handler.Handle(new AlertsQuery(null, null), CancellationToken.None);

        // Assert
        critical.Value.Should().ContainSingle().Which.Kind.Should().Be("a");
        both.Value.Should().ContainSingle().Which.Kind.Should().Be("b");
        none.Value.Should().HaveCount(2);
    }
}
