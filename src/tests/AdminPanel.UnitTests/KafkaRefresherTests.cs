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
        IEtcdGateway gateway, IKafkaSnapshotStore store, params string[] endpoints)
        => New(gateway, store, new AdminPanel.Etcd.Workers.KafkaWorkerHealthStore(), endpoints);

    // Перегрузка New (рядом с существующей): refresher со стором health-проб.
    private static KafkaSnapshotRefresher New(
        IEtcdGateway gateway, IKafkaSnapshotStore store,
        AdminPanel.Etcd.Workers.KafkaWorkerHealthStore healthStore, params string[] endpoints)
        => New(gateway, store, healthStore, new KafkaSecretsStore(), endpoints);

    // Перегрузка New (t03): refresher с внешним стором кредов (проверка наполнения).
    private static KafkaSnapshotRefresher New(
        IEtcdGateway gateway, IKafkaSnapshotStore store,
        AdminPanel.Etcd.Workers.KafkaWorkerHealthStore healthStore,
        IKafkaSecretsStore secretsStore, params string[] endpoints)
        => new(
            gateway,
            new KafkaAlertEngine(Options.Create(new KafkaAlertsOptions())),
            store,
            secretsStore,
            Options.Create(new EtcdOptions { Endpoints = endpoints }),
            Options.Create(new KafkaPanelOptions()),
            new FixedTimeProvider(),
            NullLogger<KafkaSnapshotRefresher>.Instance,
            healthStore);

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

    [Fact]
    public async Task Refresh_AdminAndCaKeys_SecretsStoreCarriesAdminCreds()
    {
        // Arrange: кластерные kvs с admin_user/admin_password/ca_pem (t03, arch/15 §2).
        var gateway = DemoGateway();
        gateway.ClustersKv =
        [
            new Kv("/kafka/clusters/events/config", """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":1}""", 1),
            new Kv("/kafka/clusters/events/admin_user", "admin", 2),
            new Kv("/kafka/clusters/events/admin_password", "AdminSecret0123456789AAAAAAA", 3),
            new Kv("/kafka/clusters/events/ca_pem", "-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----", 4),
        ];
        var store = new KafkaSnapshotStore();
        var secrets = new KafkaSecretsStore();

        // Act
        var result = await New(gateway, store, new AdminPanel.Etcd.Workers.KafkaWorkerHealthStore(),
            secrets, "http://e1").RefreshOnceAsync(CancellationToken.None);

        // Assert: стор несёт admin-креды и CA (пробы SASL_SSL); app-креды панель не читает.
        result.IsSuccess.Should().BeTrue();
        var creds = secrets.Current["events"];
        creds.AdminUser.Should().Be("admin");
        creds.AdminPassword.Should().Be("AdminSecret0123456789AAAAAAA");
        creds.CaPem.Should().Contain("BEGIN CERTIFICATE");
    }

    [Fact]
    public async Task Refresh_BrokenCaPem_NotInStore_ParseErrorRecorded()
    {
        // Arrange: ca_pem — мусор (arch/15 §6: битый PEM → parseError, ключ пропускается).
        var gateway = DemoGateway();
        gateway.ClustersKv =
        [
            new Kv("/kafka/clusters/events/config", """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":1}""", 1),
            new Kv("/kafka/clusters/events/admin_user", "admin", 2),
            new Kv("/kafka/clusters/events/admin_password", "AdminSecret0123456789AAAAAAA", 3),
            new Kv("/kafka/clusters/events/ca_pem", "garbage", 4),
        ];
        var store = new KafkaSnapshotStore();
        var secrets = new KafkaSecretsStore();

        // Act
        var result = await New(gateway, store, new AdminPanel.Etcd.Workers.KafkaWorkerHealthStore(),
            secrets, "http://e1").RefreshOnceAsync(CancellationToken.None);

        // Assert: в стор не попал + parseErrors содержит запись (тик не падает).
        result.IsSuccess.Should().BeTrue();
        secrets.Current.Should().NotContainKey("events");
        store.Current!.ParseErrors.Should().Contain(e => e.Key.Contains("ca_pem", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Refresh_AdminRotationTicket_InSnapshot()
    {
        // Arrange: заявка ротации admin-пароля (t03, arch/15 §4).
        var gateway = DemoGateway();
        gateway.RotationsKv = EtcdFixtures.LoadKv("Kafka/kafka-rotations.json");
        gateway.WorkerApiKv = [];
        var store = new KafkaSnapshotStore();

        // Act: тик с заявкой в /kafkaworker/admin_rotations/.
        var adminRotations = new List<Kv>
        {
            new("/kafkaworker/admin_rotations/events", """{"requested_unix":1756500900,"requested_by":"admin"}""", 5),
        };
        var fakeWithAdmin = new RefresherGatewayWithAdminRotations(gateway, adminRotations);
        var refresher = New(fakeWithAdmin, store, "http://e1");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: заявка — в AdminRotations снапшота.
        result.IsSuccess.Should().BeTrue();
        store.Current!.AdminRotations.Should().ContainSingle()
            .Which.Should().Be(new KafkaRotationTicket("events", 1756500900, "admin"));
    }

    // Обёртка fake-gateway: добавляет чтение префикса /kafkaworker/admin_rotations/.
    private sealed class RefresherGatewayWithAdminRotations(KafkaFakeGateway inner, IReadOnlyList<Kv> adminRotations)
        : IEtcdGateway
    {
        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => prefix == "/kafkaworker/admin_rotations/"
                ? Task.FromResult(Result<IReadOnlyList<Kv>>.Success(adminRotations))
                : inner.RangeAsync(endpoint, prefix, ct);

        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => inner.StatusAsync(endpoint, ct);

        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => inner.MemberListAsync(endpoint, ct);

        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => inner.AlarmAsync(endpoint, ct);

        public Task<Result<TxnResult>> TxnAsync(
            string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)
            => inner.TxnAsync(endpoint, compares, puts, ct);

        public Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
            => inner.PutAsync(endpoint, key, value, ct);

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => inner.DeleteAsync(endpoint, keyOrPrefix, prefix, ct);
    }

    private static readonly DateTimeOffset HealthAt =
        new(2026, 9, 2, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Refresh_HealthStore_MergedIntoSnapshot()
    {
        // Arrange: стор поллера содержит Degraded-результат опроса /healthz воркера.
        var gateway = DemoGateway();
        var store = new KafkaSnapshotStore();
        var healthStore = new AdminPanel.Etcd.Workers.KafkaWorkerHealthStore();
        healthStore.Replace([new WorkerHealth("kw1", "http://kafkaworker:8080",
            WorkerHealthStatus.Degraded, HealthAt, "HTTP 503")]);

        // Act
        var result = await New(gateway, store, healthStore, "http://e1")
            .RefreshOnceAsync(CancellationToken.None);

        // Assert: успешный тик вносит свежее состояние поллера (arch/02 §2.3.2;
        // симметрия pg SnapshotRefresher.cs:156).
        result.IsSuccess.Should().BeTrue();
        store.Current!.WorkerHealth.Should().ContainSingle()
            .Which.Status.Should().Be(WorkerHealthStatus.Degraded);
    }

    [Fact]
    public async Task Refresh_EtcdFail_PreservesPreviousWorkerHealth()
    {
        // Arrange: успешный тик внёс Degraded из стора; затем все endpoints умирают
        // (FailEndpoints — механика соседних FailTick-тестов), а поллер записывает
        // в стор Healthy (воркер восстановился).
        var gateway = DemoGateway();
        var store = new KafkaSnapshotStore();
        var healthStore = new AdminPanel.Etcd.Workers.KafkaWorkerHealthStore();
        healthStore.Replace([new WorkerHealth("kw1", "http://kafkaworker:8080",
            WorkerHealthStatus.Degraded, HealthAt, "HTTP 503")]);
        var refresher = New(gateway, store, healthStore, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.FailEndpoints.Add("http://e1");
        healthStore.Replace([new WorkerHealth("kw1", "http://kafkaworker:8080",
            WorkerHealthStatus.Healthy, HealthAt.AddSeconds(5), null)]);

        // Act: отказный тик.
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: прежний WorkerHealth перенесён из previous (Degraded), свежий стор
        // НЕ мерджится на отказном тике (симметрия pg SnapshotRefresher.cs:225,
        // spec §3.4) — алерт worker-unhealthy загорается только первым УСПЕШНЫМ
        // тиком refresher'а после восстановления etcd.
        result.IsSuccess.Should().BeFalse();
        store.Current!.WorkerHealth.Should().ContainSingle()
            .Which.Status.Should().Be(WorkerHealthStatus.Degraded);
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
            new DateTimeOffset(2026, 8, 30, 0, 0, 0, TimeSpan.Zero), true, 0, [], [], [], [], [], [], [], [], [], [], 0);
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
