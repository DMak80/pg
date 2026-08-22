using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Конструирование модуля напрямую (без attribute-DI/WAF): статический кеш сборок
// должен остаться чистым для Program-хостов t04+ (spec §3.15).
public static class EtcdTestHarness
{
    private sealed class RealTimeProvider : TimeProvider
    {
    }

    public static EtcdGateway NewGateway()
        => new(new HttpClient { Timeout = TimeSpan.FromSeconds(2) });

    public static SnapshotRefresher NewRefresher(ISnapshotStore store, params string[] endpoints)
        => new(
            NewGateway(),
            new AlertEngine(
            [
                new EtcdUnreachableRule(),
                new EtcdNoQuorumRule(),
                new EtcdEndpointDownRule(),
                new EtcdAlarmRule(),
                new SnapshotStaleRule(),
                new ClusterIncompleteRule(),
                new KeyMalformedRule(),
                new ShardNoMasterRule(),
                new MoveStaleRule(Options.Create(new AlertsOptions())),
                new MoveFrozenLongRule(Options.Create(new AlertsOptions())),
                new MoveAbortingRule(),
                new MoveFlippedStatusStuckRule(),
                new BucketLostRule(),
                new BucketNoRoutingRule(),
                new BucketOutOfRangeRule(),
            ]),
            store,
            Options.Create(new EtcdOptions { Endpoints = endpoints }),
            new RealTimeProvider(),
            NullLogger<SnapshotRefresher>.Instance);
}

// Gateway + refresher против живого etcd с сидом demo (spec §11.2).
public class EtcdSnapshotIntegrationTests(EtcdContainerFixture fixture) : IClassFixture<EtcdContainerFixture>
{
    [Fact]
    public async Task Gateway_Status_AgainstRealEtcd()
    {
        // Arrange
        var gateway = EtcdTestHarness.NewGateway();

        // Act
        var result = await gateway.StatusAsync(fixture.Endpoint, CancellationToken.None);

        // Assert — подтверждает фактические имена полей gateway (spec §3.17)
        result.IsSuccess.Should().BeTrue();
        result.Value.Version.Should().Be("3.5.21");
        result.Value.LeaderMemberId.Should().BeGreaterThan(0);
        result.Value.RaftTerm.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Gateway_MemberList_SingleMember()
    {
        // Arrange
        var gateway = EtcdTestHarness.NewGateway();

        // Act
        var result = await gateway.MemberListAsync(fixture.Endpoint, CancellationToken.None);

        // Assert
        var member = result.Value.Should().ContainSingle().Subject;
        member.Name.Should().Be("test");
        member.ClientUrls.Should().NotBeEmpty();
    }

    [Fact]
    public async Task Gateway_Alarm_Empty()
    {
        // Arrange
        var gateway = EtcdTestHarness.NewGateway();

        // Act
        var result = await gateway.AlarmAsync(fixture.Endpoint, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEmpty();
    }

    [Fact]
    public async Task Gateway_Range_ClustersPrefix_ReturnsSeededKvs()
    {
        // Arrange
        var gateway = EtcdTestHarness.NewGateway();

        // Act
        var result = await gateway.RangeAsync(fixture.Endpoint, "/clusters/", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Contain(kv => kv.Key == "/clusters/demo/config");
    }

    [Fact]
    public async Task Refresher_RefreshOnce_BuildsExpectedSnapshot()
    {
        // Arrange
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current.Should().NotBeNull();
        var snapshot = store.Current!;
        snapshot.Etcd.Reachable.Should().BeTrue();
        snapshot.Etcd.ActiveEndpoint.Should().Be(fixture.Endpoint);
        snapshot.Etcd.ConsecutiveFailures.Should().Be(0);
        snapshot.Etcd.QuorumSuspected.Should().BeFalse(); // одиночный etcd: leader валиден
        var demo = snapshot.Clusters.Should().ContainSingle(c => c.Name == "demo").Subject;
        demo.DbName.Should().Be("demo");
        demo.BucketsCount.Should().Be(16);
        demo.Buckets.Should().HaveCount(16);
        demo.Shards.Should().Contain(s => s.Name == "s1" && s.MasterAddress == "s1a:5432");
        demo.Buckets.Single(b => b.Id == 3).State.Should().Be(BucketState.Syncing);
        demo.Buckets.Single(b => b.Id == 7).State.Should().Be(BucketState.Aborting);
        demo.Buckets.Single(b => b.Id == 11).State.Should().Be(BucketState.Frozen);
        demo.Buckets.Single(b => b.Id == 0).State.Should().Be(BucketState.Active);
        demo.Heals.Should().ContainSingle(h => h.Bucket == "bucket_5");
        var scope = snapshot.HaScopes.Should().ContainSingle(s => s.Scope == "demo-s1").Subject;
        scope.Matched.Should().BeTrue();
        scope.LeaderName.Should().Be("s1a");
        scope.Members.Should().HaveCount(2);
        snapshot.StandNodes.Should().HaveCount(4);
        snapshot.Etcd.Members.Should().ContainSingle(m => m.Name == "test");
        // t05: сид demo несёт 3 статус-ключа с протухшими штампами → ровно 5 move-алертов (spec §3.15);
        // сортировка: critical (frozen-long) → warnings по kind/target (Ordinal).
        string.Join("|", snapshot.Alerts.Select(a => a.Id))
            .Should().Be("move-frozen-long:demo/bucket_11|move-aborting:demo/bucket_7|move-stale:demo/bucket_11|move-stale:demo/bucket_3|move-stale:demo/bucket_7");
        snapshot.Probes.Should().BeEmpty();
    }

    [Fact]
    public async Task Refresher_SecondTick_PicksUpChanges()
    {
        // Arrange
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Act — перевладение routing bucket_0 шарду s2
        await EtcdSeed.PutAsync(fixture.Endpoint, "/clusters/demo/buckets/routing/bucket_0", "s2", CancellationToken.None);
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        store.Current!.Clusters.Single().Buckets.Single(b => b.Id == 0).Owner.Should().Be("s2");
    }

    [Fact]
    public async Task Refresher_Failover_DeadFirstEndpoint()
    {
        // Arrange — localhost:1: connection refused мгновенен
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, "http://localhost:1", fixture.Endpoint);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current!.Etcd.ActiveEndpoint.Should().Be(fixture.Endpoint);
        store.Current.Etcd.Endpoints.Should().HaveCount(2);
        store.Current.Etcd.Endpoints[0].Reachable.Should().BeFalse();
    }

    [Fact]
    public async Task HealthCheck_ReflectsRefresherState()
    {
        // Arrange
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        var check = new EtcdHealthCheck(refresher);
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Act
        var result = await check.CheckHealthAsync(
            new Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckContext(),
            TestContext.Current.CancellationToken);

        // Assert
        result.Status.Should().Be(Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Healthy);
    }
}

// Сценарий отказа etcd: отдельный класс со СВОИМ контейнером — StopAsync ломает fixture,
// порядок тестов внутри коллекции не гарантирован бы (spec §11.1).
// Включает вторую половину HealthCheck-сценария §11.2: Unhealthy после остановки etcd.
public class EtcdFailureTests(EtcdContainerFixture fixture) : IClassFixture<EtcdContainerFixture>
{
    [Fact]
    public async Task Refresher_EtcdStopped_KeepsPreviousSnapshot()
    {
        // Arrange
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var builtAt = store.Current!.BuiltAtUtc;
        var clusters = store.Current.Clusters;
        await fixture.StopAsync();

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var health = await new EtcdHealthCheck(refresher)
            .CheckHealthAsync(
                new Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckContext(),
                TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeFalse();
        store.Current!.BuiltAtUtc.Should().Be(builtAt);     // данные прежние, возраст растёт (spec §3.9)
        store.Current.Clusters.Should().BeSameAs(clusters);
        store.Current.Etcd.Reachable.Should().BeFalse();
        store.Current.Etcd.ConsecutiveFailures.Should().Be(2);
        // t04: алерты вычислены и на отказном тике — unreachable на пороге 2 (spec §3.5).
        store.Current.Alerts.Should().Contain(a => a.Id == "etcd-unreachable:etcd");
        health.Status.Should().Be(Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Unhealthy);
    }
}
