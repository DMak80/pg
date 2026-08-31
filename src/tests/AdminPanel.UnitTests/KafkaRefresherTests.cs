using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Core.Kafka.KafkaAlerting;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// KafkaSnapshotRefresher (arch/02 §10): тик-сборка на fake gateway, отказ
// транспорта роняет тик (неполный снапшот не публикуется), восстановление.
public class KafkaRefresherTests
{
    // Управляемый gateway kafka-домена: два префикса + точечные отказы.
    private sealed class KafkaFakeGateway : IEtcdGateway
    {
        public IReadOnlyList<Kv> ClustersKv { get; set; } = [];

        public IReadOnlyList<Kv> RotationsKv { get; set; } = [];

        public IReadOnlyList<Kv> RebalancesKv { get; set; } = [];

        public IReadOnlyList<Kv> ReassignmentsKv { get; set; } = [];

        public IReadOnlyList<Kv> WorkerApiKv { get; set; } = [];

        public List<string> FailEndpoints { get; } = [];

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(FailEndpoints.Contains(endpoint)
                ? Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException(endpoint))
                : Result<IReadOnlyList<Kv>>.Success(prefix switch
                {
                    "/kafka/clusters/" => ClustersKv,
                    "/kafkaworker/rotations/" => RotationsKv,
                    "/kafkaworker/rebalances/" => RebalancesKv,
                    "/kafkaworker/reassignments/" => ReassignmentsKv,
                    "/kafkaworker/api/" => WorkerApiKv,
                    _ => [],
                }));

        // Не используются kafka-тиком — заглушки ради интерфейса.
        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<EtcdStatusPayload>.Success(
                new EtcdStatusPayload("3.5.21", 1, 1, 1, 1)));

        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

        public Task<Result<TxnResult>> TxnAsync(
            string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)
            => Task.FromResult(Result<TxnResult>.Failed(new EtcdUnreachableException(endpoint)));

        public Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException(endpoint)));
    }

    private static KafkaSnapshotRefresher New(
        KafkaFakeGateway gateway, IKafkaSnapshotStore store, params string[] endpoints)
        => new(
            gateway,
            new KafkaAlertEngine(Options.Create(new KafkaAlertsOptions())),
            store,
            new KafkaSecretsStore(),
            Options.Create(new EtcdOptions { Endpoints = endpoints }),
            Options.Create(new KafkaPanelOptions()),
            new FixedTimeProvider(),
            NullLogger<KafkaSnapshotRefresher>.Instance);

    private static KafkaFakeGateway DemoGateway() => new()
    {
        ClustersKv = EtcdFixtures.LoadKv("Kafka/kafka-clusters-full.json"),
        RotationsKv = EtcdFixtures.LoadKv("Kafka/kafka-rotations.json"),
        RebalancesKv = EtcdFixtures.LoadKv("Kafka/kafka-rebalances.json"),
        ReassignmentsKv = EtcdFixtures.LoadKv("Kafka/kafka-reassignments.json"),
    };

    [Fact]
    public async Task Refresh_BuildsSnapshotWithAlerts()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new KafkaSnapshotStore();
        var refresher = New(gateway, store, "http://e1");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var snapshot = store.Current;
        snapshot.Should().NotBeNull();
        snapshot!.EtcdReachable.Should().BeTrue();
        snapshot.ConsecutiveFailures.Should().Be(0);
        snapshot.Clusters.Should().HaveCount(2);
        snapshot.Rotations.Should().HaveCount(2);
        snapshot.Rebalances.Should().HaveCount(2);
        snapshot.Reassignments.Should().HaveCount(2);
        snapshot.UnknownKeyCount.Should().Be(0);
        // Битые ключи новых префиксов — parseError (алерт kafka-key-malformed
        // по ним даст Task 12), тик не падает.
        snapshot.ParseErrors.Select(e => e.Key).Should()
            .Contain("/kafkaworker/rebalances/broken")
            .And.Contain("/kafkaworker/reassignments/broken");
        var progress = snapshot.Reassignments.Single(p => p.Cluster == "events");
        progress.Mode.Should().Be("drain");
        progress.PartitionsRemaining.Should().Be(5);
        // Алерты движка на собранном снапшоте: заявка ротации events + pending-кластер.
        snapshot.Alerts.Should().Contain(a => a.Id == "kafka-rotation-pending:events");
        snapshot.Alerts.Should().Contain(a => a.Id == "kafka-cluster-not-initialized:pending");
    }

    [Fact]
    public async Task Refresh_WithWorkerApiKeys_StoresWorkerEndpoints()
    {
        // Arrange: живой ключ доступа kafkaworker (arch/02 §2.3.2)
        var gateway = DemoGateway();
        gateway.WorkerApiKv =
        [
            new Kv("/kafkaworker/api/kw1", """{"url":"http://kafkaworker:8080","instance":"kw1","since_unix":1756000001}""", 9),
        ];
        var store = new KafkaSnapshotStore();

        // Act
        var result = await New(gateway, store, "http://e1").RefreshOnceAsync(CancellationToken.None);

        // Assert: ключ — в WorkerEndpoints снапшота (резолв мутаций панели)
        result.IsSuccess.Should().BeTrue();
        store.Current!.WorkerEndpoints.Should().ContainSingle().Which
            .Should().Be(new WorkerEndpoint("kw1", "http://kafkaworker:8080", 1756000001));
    }

    [Fact]
    public async Task Refresh_TransportFailure_PreservesPreviousSnapshot()
    {
        // Arrange: успешный тик, затем все endpoints умирают.
        var gateway = DemoGateway();
        var store = new KafkaSnapshotStore();
        var refresher = New(gateway, store, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var builtAt = store.Current!.BuiltAtUtc;
        var clusters = store.Current.Clusters;
        gateway.FailEndpoints.Add("http://e1");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: прежние данные, Reachable=false, счётчик отказов растёт.
        result.IsSuccess.Should().BeFalse();
        store.Current!.BuiltAtUtc.Should().Be(builtAt);
        store.Current.Clusters.Should().BeSameAs(clusters);
        store.Current.EtcdReachable.Should().BeFalse();
        store.Current.ConsecutiveFailures.Should().Be(1);
    }

    [Fact]
    public async Task Refresh_Recovery_ResetsFailures()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new KafkaSnapshotStore();
        var refresher = New(gateway, store, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.FailEndpoints.Add("http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Act — endpoint ожил
        gateway.FailEndpoints.Clear();
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current!.ConsecutiveFailures.Should().Be(0);
        store.Current.EtcdReachable.Should().BeTrue();
    }

    [Fact]
    public async Task Refresh_Failover_StickyDeadMovesToNext()
    {
        // Arrange: активный endpoint умирает между тиками — тик не теряется.
        var gateway = DemoGateway();
        var store = new KafkaSnapshotStore();
        var refresher = New(gateway, store, "http://e1", "http://e2");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.FailEndpoints.Add("http://e1");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: данные собраны через e2.
        result.IsSuccess.Should().BeTrue();
        store.Current!.Clusters.Should().HaveCount(2);
        store.Current.EtcdReachable.Should().BeTrue();
    }

    [Fact]
    public async Task Refresh_NoEndpoints_FailedTick()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new KafkaSnapshotStore();
        var refresher = New(gateway, store);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: пустой снапшот с Reachable=false.
        result.IsSuccess.Should().BeFalse();
        store.Current.Should().NotBeNull();
        store.Current!.EtcdReachable.Should().BeFalse();
        store.Current.Clusters.Should().BeEmpty();
        store.Current.ConsecutiveFailures.Should().Be(1);
    }
}

// Хранилище kafka-снапшота (порт SnapshotStore): volatile-ссылка.
public class KafkaSnapshotStoreTests
{
    [Fact]
    public void Store_CurrentIsNullBeforeFirstReplace()
    {
        // Arrange / Act
        var store = new KafkaSnapshotStore();

        // Assert
        store.Current.Should().BeNull();
    }

    [Fact]
    public void Store_ReplaceSwapsReferenceAtomically()
    {
        // Arrange
        var store = new KafkaSnapshotStore();
        var first = new KafkaSnapshot(
            new DateTimeOffset(2026, 8, 30, 0, 0, 0, TimeSpan.Zero), true, 0, [], [], [], [], [], [], [], [], 0);
        var second = first with { BuiltAtUtc = first.BuiltAtUtc.AddSeconds(3) };

        // Act
        store.Replace(first);
        var readBetween = store.Current;
        store.Replace(second);

        // Assert: читатели держат-consistent ссылку (замена атомарна).
        readBetween.Should().BeSameAs(first);
        store.Current.Should().BeSameAs(second);
    }
}
