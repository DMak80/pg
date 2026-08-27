using AdminPanel.Etcd;
using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace AdminPanel.UnitTests;

// EtcdHealthCheck — отражение состояния refresher'а (spec §7.3): старт Degraded, тик ок Healthy, отказ Unhealthy.
public class EtcdHealthCheckTests
{
    [Fact]
    public async Task Check_BeforeFirstTick_Degraded()
    {
        // Arrange
        var refresher = RefresherTestHarness.New(new FakeEtcdGateway(), new SnapshotStore(), "http://e1");
        var check = new EtcdHealthCheck(refresher);

        // Act
        var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        // Assert
        result.Status.Should().Be(HealthStatus.Degraded); // «service is starting»
    }

    [Fact]
    public async Task Check_AfterSuccessfulTick_Healthy()
    {
        // Arrange
        var gateway = new FakeEtcdGateway
        {
            ClustersKv = EtcdFixtures.LoadKv("clusters-full.json"),
        };
        var refresher = RefresherTestHarness.New(gateway, new SnapshotStore(), "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var check = new EtcdHealthCheck(refresher);

        // Act
        var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        // Assert
        result.Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task Check_AfterFailedTick_Unhealthy()
    {
        // Arrange
        var refresher = RefresherTestHarness.New(new FakeEtcdGateway(), new SnapshotStore());
        await refresher.RefreshOnceAsync(CancellationToken.None); // Endpoints пуст → отказ
        var check = new EtcdHealthCheck(refresher);

        // Act
        var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        // Assert
        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Exception.Should().NotBeNull();
    }
}
