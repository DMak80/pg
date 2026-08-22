using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Общая тест-обвязка refresher'а: FakeEtcdGateway + конструктор с любыми endpoints.
// Используется и EtcdHealthCheckTests (Task 10) — internal на сборку.
internal static class RefresherTestHarness
{
    public static SnapshotRefresher New(FakeEtcdGateway gateway, ISnapshotStore store, params string[] endpoints)
        => new(
            gateway,
            new AlertEngine(AlertTestRules.All()),
            store,
            Options.Create(new EtcdOptions { Endpoints = endpoints }),
            new FixedTimeProvider(),
            NullLogger<SnapshotRefresher>.Instance);
}

// Управляемый gateway: данные/отказы по endpoints, счётчики вызовов.
internal sealed class FakeEtcdGateway : IEtcdGateway
{
    public List<string> StatusFailEndpoints { get; } = [];

    public List<string> RangeFailEndpoints { get; } = [];

    public IReadOnlyList<Kv> ClustersKv { get; init; } = [];

    public IReadOnlyList<Kv> ServiceKv { get; init; } = [];

    public IReadOnlyList<Kv> NodesKv { get; init; } = [];

    public IReadOnlyList<EtcdMember> Members { get; init; } = [];

    public IReadOnlyList<EtcdAlarm> Alarms { get; init; } = [];

    public int RangeCalls { get; private set; }

    public int StatusCalls { get; private set; }

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
    {
        RangeCalls++;
        return Task.FromResult(RangeFailEndpoints.Contains(endpoint)
            ? Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException(endpoint))
            : Result<IReadOnlyList<Kv>>.Success(prefix switch
            {
                "/clusters/" => ClustersKv,
                "/service/" => ServiceKv,
                _ => NodesKv,
            }));
    }

    public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
    {
        StatusCalls++;
        return Task.FromResult(StatusFailEndpoints.Contains(endpoint)
            ? Result<EtcdStatusPayload>.Failed(new EtcdUnreachableException(endpoint))
            : Result<EtcdStatusPayload>.Success(new EtcdStatusPayload("3.5.21", 20480, 42, 17, 3)));
    }

    public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success(Members));

    public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success(Alarms));
}

// Refresher: живые/мёртвые endpoints, sticky-failover, отказ с сохранением данных (spec §10.9).
public class SnapshotRefresherTests
{
    private static FakeEtcdGateway DemoGateway() => new()
    {
        ClustersKv = EtcdFixtures.LoadKv("clusters-full.json"),
        ServiceKv = EtcdFixtures.LoadKv("service-full.json"),
        NodesKv = EtcdFixtures.LoadKv("stand-nodes.json"),
        Members = [new EtcdMember(42, "test", ["http://p"], ["http://c"])],
    };

    [Fact]
    public async Task Refresh_AllAlive_BuildsAndStoresSnapshot()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1", "http://e2");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current.Should().NotBeNull();
        var snapshot = store.Current!;
        snapshot.Etcd.Reachable.Should().BeTrue();
        snapshot.Etcd.Endpoints.Should().HaveCount(2);
        snapshot.Etcd.ActiveEndpoint.Should().Be("http://e1"); // sticky: первый по списку
        snapshot.Etcd.ConsecutiveFailures.Should().Be(0);
        snapshot.Clusters.Should().ContainSingle(c => c.Name == "demo");
        snapshot.Etcd.Members.Should().ContainSingle(m => m.Name == "test");
        gateway.StatusCalls.Should().Be(2); // персонально по всем endpoints (arch/02 §2.4)
        refresher.Working.Should().BeTrue();
        refresher.Inited.Should().BeTrue();
    }

    [Fact]
    public async Task Refresh_AllDead_PreservesDataAndCountsFailure()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1", "http://e2");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var builtAt = store.Current!.BuiltAtUtc;
        var clusters = store.Current.Clusters;
        gateway.StatusFailEndpoints.AddRange(["http://e1", "http://e2"]);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        store.Current!.BuiltAtUtc.Should().Be(builtAt);       // возраст данных растёт (spec §3.9)
        store.Current.Clusters.Should().BeSameAs(clusters);   // данные прежние
        store.Current.Etcd.Reachable.Should().BeFalse();
        store.Current.Etcd.ConsecutiveFailures.Should().Be(2);
        store.Current.Etcd.Endpoints.Should().OnlyContain(e => !e.Reachable);
        refresher.Working.Should().BeFalse();
        refresher.StatusError.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public async Task Refresh_Recovery_ResetsFailures()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.StatusFailEndpoints.Add("http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        store.Current!.Etcd.ConsecutiveFailures.Should().Be(1);

        // Act — endpoint ожил
        gateway.StatusFailEndpoints.Clear();
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current.Etcd.ConsecutiveFailures.Should().Be(0);
        store.Current.Etcd.Reachable.Should().BeTrue();
        refresher.Working.Should().BeTrue();
    }

    [Fact]
    public async Task Refresh_StickyFails_OverToNextAlive()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1", "http://e2");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.StatusFailEndpoints.Add("http://e1"); // активный умер между тиками

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current!.Etcd.ActiveEndpoint.Should().Be("http://e2");
        store.Current.Etcd.Endpoints.Single(e => e.Url == "http://e1").Reachable.Should().BeFalse();
    }

    [Fact]
    public async Task Refresh_MidTickFailure_FailsOverWithoutLosingTick()
    {
        // Arrange — статус жив, но KV-чтения на активном падают: failover внутри тика (spec §3.10)
        var gateway = DemoGateway();
        gateway.RangeFailEndpoints.Add("http://e1");
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1", "http://e2");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current!.Clusters.Should().ContainSingle(c => c.Name == "demo");
    }

    [Fact]
    public async Task Refresh_EmptyEndpoints_FailedTickWithEmptySnapshot()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        store.Current.Should().NotBeNull(); // пустой снапшот с Reachable=false (spec §3.12)
        store.Current!.Etcd.Reachable.Should().BeFalse();
        store.Current.Etcd.ConsecutiveFailures.Should().Be(1);
        store.Current.Clusters.Should().BeEmpty();
        refresher.Inited.Should().BeTrue();
        refresher.Working.Should().BeFalse();
    }

    [Fact]
    public async Task Refresh_AlertsStoredOnSuccessTick()
    {
        // Arrange: полный demo-сид + один битый статус-ключ → key-malformed (spec §10.2).
        var store = new SnapshotStore();
        var gateway = new FakeEtcdGateway
        {
            ClustersKv =
            [
                .. EtcdFixtures.LoadKv("clusters-full.json"),
                new Kv("/clusters/demo/buckets/status/bucket_9", "not json", 99),
            ],
            ServiceKv = EtcdFixtures.LoadKv("service-full.json"),
        };
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1");

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: key-malformed от битого ключа + 5 move-алертов сида demo (spec §3.15, §10.4).
        var alerts = store.Current!.Alerts;
        alerts.Should().HaveCount(6);
        alerts.Should().Contain(a => a.Id == "key-malformed:/clusters/demo/buckets/status/bucket_9");
        alerts.Should().Contain(a => a.Id == "move-stale:demo/bucket_3");
        alerts.Should().Contain(a => a.Id == "move-stale:demo/bucket_7");
        alerts.Should().Contain(a => a.Id == "move-stale:demo/bucket_11");
        alerts.Should().Contain(a => a.Id == "move-frozen-long:demo/bucket_11");
        alerts.Should().Contain(a => a.Id == "move-aborting:demo/bucket_7");
    }

    [Fact]
    public async Task Refresh_AlertsComputedOnFailTick()
    {
        // Arrange: первый тик собирает снапшот с incomplete-кластером; затем endpoints умирают.
        var store = new SnapshotStore();
        var gateway = new FakeEtcdGateway
        {
            ClustersKv = [new Kv("/clusters/ghost/shards/g1/dsn", "host=g1 port=5432", 1)],
        };
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.StatusFailEndpoints.Add("http://e1");

        // Act: два отказных тика — порог etcd-unreachable = 2 (spec §4.2).
        await refresher.RefreshOnceAsync(CancellationToken.None);
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: unreachable вспыхнул; data-алерт из прежних данных сохранён,
        // sinceUnix не рвётся (перенос null с первого тика — §3.4).
        var alerts = store.Current!.Alerts;
        alerts.Should().Contain(a => a.Id == "etcd-unreachable:etcd"
            && a.Severity == AlertSeverity.Critical);
        var incomplete = alerts.Single(a => a.Kind == "cluster-incomplete");
        incomplete.Target.Should().Be("ghost");
        incomplete.SinceUnix.Should().BeNull();
    }
}
