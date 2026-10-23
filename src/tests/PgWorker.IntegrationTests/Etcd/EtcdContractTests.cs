using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using Xunit;

namespace PgWorker.IntegrationTests.Etcd;

// Контракт форматов /clusters/ и /pgworker/* на реальном etcd (задача 13, критерий §11.8):
// сид панели → парсер; WorkJournal round-trip; portalloc round-trip.
[Collection(NonE2eCollection.Name)]
public class EtcdContractTests(EtcdFixture fixture)
{
    // DTO формата /pgworker/portalloc (spec §4.3): плоский {"host","pg","patroni","doorman"}.
    private sealed record PortallocEntry(
        [property: JsonPropertyName("host")] string Host,
        [property: JsonPropertyName("pg")] int Pg,
        [property: JsonPropertyName("patroni")] int Patroni,
        [property: JsonPropertyName("doorman")] int Doorman);

    private EtcdGateway Gateway => fixture.Gateway;

    private string Endpoint => fixture.Endpoint;

    // Per-class guid-тег (канон docs/e2e-isolation.md §1, инвариант
    // NonE2eCollection): имена кластеров в сидах /clusters/, /service/ и
    // /pgworker/* несут уникальный суффикс — литеральная коллизия с
    // соседними классами коллекции (парсеры читают общий /clusters/)
    // механически невозможна. Тесты чистят свои префиксы (assert чистоты).
    private static readonly string Tag = Guid.NewGuid().ToString("N")[..8];

    [Fact]
    public async Task PanelSeed_ClusterSnapshotParser_ProducesCorrectSnapshot()
    {
        // Arrange — сид кластера в стиле панели (02 §9.1): config NOT_INITIALIZED,
        // shards/replicas, nodes, routing/status NOT_INITIALIZED, request_*;
        // имя кластера с guid-тегом класса, dbname остаётся литеральным
        var cluster = $"ct1{Tag}";
        var ct = TestContext.Current.CancellationToken;
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/config",
            """{"buckets":6,"dbname":"shop","created_unix":1755800000,"state":"NOT_INITIALIZED"}""", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/replicas", "2", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/nodes/shard1a/state", "NOT_INITIALIZED", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/nodes/shard1b/state", "NOT_INITIALIZED", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard2/replicas", "2", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard2/nodes/shard2a/state", "NOT_INITIALIZED", null, ct);
        for (var i = 0; i < 6; i++)
        {
            await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i % 2 + 1}", null, ct);
            await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }

        await Gateway.PutAsync(Endpoint, $"/service/{cluster}-shard1/request_cpu", "2", null, ct);
        await Gateway.PutAsync(Endpoint, $"/service/{cluster}-shard1/request_mem", "2G", null, ct);

        // Act — читаем реальный range и парсим
        var range = await Gateway.RangeAsync(Endpoint, "/clusters/", ct);
        range.IsSuccess.Should().BeTrue();
        var result = ClusterSnapshotParser.ParseClusters(range.Value, out var errors);

        // Assert — снапшот соответствует сиду
        result.IsSuccess.Should().BeTrue();
        errors.Should().BeEmpty();
        var snap = result.Value.Should().ContainSingle(c => c.Config.Cluster == cluster).Subject;
        snap.Config.State.Should().Be(ClusterState.NotInitialized);
        snap.Config.DbName.Should().Be("shop");
        snap.Config.Buckets.Should().Be(6);
        snap.Shards.Should().HaveCount(2);
        snap.Shards.Should().Contain(s => s.Name == "shard1" && s.Replicas == 2 && s.Nodes.Count == 2);
        snap.Routing.Should().HaveCount(6);
        snap.Routing.Should().OnlyContain(r => r.Status == BucketMoveState.NotInitialized);

        var serviceRange = await Gateway.RangeAsync(Endpoint, "/service/", ct);
        serviceRange.IsSuccess.Should().BeTrue();
        var service = ClusterSnapshotParser.ParseService(serviceRange.Value);
        service.Should().Contain(s => s.Scope == $"{cluster}-shard1" && !s.Initialized && s.LeaderName == null);

        // Cleanup + assert чистоты: ключи теста не переживают тест
        await Gateway.DeleteAsync(Endpoint, $"/clusters/{cluster}/", prefix: true, ct);
        await Gateway.DeleteAsync(Endpoint, $"/service/{cluster}-shard1/", prefix: true, ct);
        var leftoverClusters = await Gateway.RangeAsync(Endpoint, $"/clusters/{cluster}/", ct);
        leftoverClusters.Value.Should().BeEmpty();
        var leftoverService = await Gateway.RangeAsync(Endpoint, $"/service/{cluster}-shard1/", ct);
        leftoverService.Value.Should().BeEmpty();
    }

    [Fact]
    public async Task WorkJournal_RoundTrip_AgainstRealEtcd()
    {
        // Arrange — имя кластера с guid-тегом класса
        var cluster = $"ct2{Tag}";
        var ct = TestContext.Current.CancellationToken;
        var journal = new WorkJournal("/pgworker", Gateway, [Endpoint]);

        // Act
        var write = await journal.WritePhaseAsync(cluster, "provision", "planned", "inst-1", null, ct);
        var read = await journal.ReadAsync(cluster, ct);

        // Assert — формат /pgworker/work/<C> переживает запись/чтение (§11.8)
        write.IsSuccess.Should().BeTrue();
        read.IsSuccess.Should().BeTrue();
        read.Value.Should().NotBeNull();
        read.Value!.Op.Should().Be("provision");
        read.Value.Phase.Should().Be("planned");
        read.Value.Instance.Should().Be("inst-1");
        read.Value.LastError.Should().BeNull();

        // Cleanup + assert чистоты
        await Gateway.DeleteAsync(Endpoint, $"/pgworker/work/{cluster}", prefix: false, ct);
        var leftover = await Gateway.GetAsync(Endpoint, $"/pgworker/work/{cluster}", ct);
        leftover.Value.Should().BeNull();
    }

    [Fact]
    public async Task WorkJournal_RetrySeries_RoundTrip_AgainstRealEtcd()
    {
        // Arrange — журнал с контекстом серии ретраев (arch/14 §3.3);
        // имя кластера с guid-тегом класса
        var cluster = $"ct3{Tag}";
        var ct = TestContext.Current.CancellationToken;
        var journal = new WorkJournal("/pgworker", Gateway, [Endpoint]);
        var series = new RetrySeries(FailCount: 3, FailFirstUnix: 1756005400, RetryNotBeforeUnix: 1756009215);

        // Act — фейл с серией, затем фаза прогресса с переносом той же серии.
        var fail = await journal.WritePhaseAsync(cluster, "provision", "shard-provision", "inst-1",
            "the Patroni of shard shop-shard1 did not come up within the 600 s budget", ct, series);
        var carried = await journal.WritePhaseAsync(
            cluster, "provision", "waiting-patroni", "inst-1", null, ct, series);
        var read = await journal.ReadAsync(cluster, ct);

        // Assert — поля серии переживают запись/чтение через реальный etcd.
        fail.IsSuccess.Should().BeTrue();
        carried.IsSuccess.Should().BeTrue();
        read.Value!.FailCount.Should().Be(3);
        read.Value.FailFirstUnix.Should().Be(1756005400);
        read.Value.RetryNotBeforeUnix.Should().Be(1756009215);

        // Cleanup + assert чистоты
        await Gateway.DeleteAsync(Endpoint, $"/pgworker/work/{cluster}", prefix: false, ct);
        var leftover = await Gateway.GetAsync(Endpoint, $"/pgworker/work/{cluster}", ct);
        leftover.Value.Should().BeNull();
    }

    [Fact]
    public async Task Portalloc_RoundTrip_StructureSurvivesRealEtcd()
    {
        // Arrange — закрепление портов нод (spec §4.3): {"<shard>/<node>":{host,pg,patroni,doorman}};
        // имя кластера с guid-тегом класса
        var cluster = $"ct4{Tag}";
        var ct = TestContext.Current.CancellationToken;
        const string json =
            """{"shard1/shard1a":{"host":"h1","pg":15432,"patroni":18008,"doorman":16432}}""";

        // Act
        var put = await Gateway.PutAsync(Endpoint, $"/pgworker/portalloc/{cluster}", json, lease: null, ct);
        var read = await Gateway.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);

        // Assert — структура десериализуется в исходные host/порты
        put.IsSuccess.Should().BeTrue();
        read.Value.Should().NotBeNull();
        var dict = JsonSerializer.Deserialize<Dictionary<string, PortallocEntry>>(read.Value!.Value);
        dict.Should().ContainKey("shard1/shard1a");
        var entry = dict!["shard1/shard1a"];
        entry.Host.Should().Be("h1");
        var addr = new NodeAddress(entry.Host, new NodePorts(entry.Pg, entry.Patroni, entry.Doorman));
        addr.Host.Should().Be("h1");
        addr.Ports.Pg.Should().Be(15432);
        addr.Ports.Patroni.Should().Be(18008);
        addr.Ports.Doorman.Should().Be(16432);

        // Cleanup + assert чистоты
        await Gateway.DeleteAsync(Endpoint, $"/pgworker/portalloc/{cluster}", prefix: false, ct);
        var leftover = await Gateway.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        leftover.Value.Should().BeNull();
    }
}
