using PgWorker.App.Loops;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.IntegrationTests.Etcd;

// Контракт scale-ключей на реальном etcd (t06 spec §8): сид панели → детекция
// ShardScaleClassifier → RemoveShardProcess реальными txn/del; идемпотентность
// конкурентных PUT маркера; демонтаж недоднятого add (Д5).
[Collection(EtcdCollection.Name)]
public class ShardScaleContractTests(EtcdFixture fixture)
{
    private EtcdGateway Gateway => fixture.Gateway;

    private string Endpoint => fixture.Endpoint;

    // Per-class guid-тег (канон docs/e2e-isolation.md §1: guid во всех именах):
    // имена кластеров несут уникальный суффикс — пересечение с соседними классами
    // EtcdCollection механически невозможно (инцидент t07: BackupSupervisor взял
    // имена sc1..sc3, клэйм sc3 жил 15с по TTL и ронял TryClaim этого класса).
    private static readonly string Tag = Guid.NewGuid().ToString("N")[..8];

    // Сид Active-кластера (уникальное имя на тест — общий etcd коллекции).
    private async Task SeedActiveClusterAsync(string cluster, int buckets)
    {
        var ct = TestContext.Current.CancellationToken;
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/config",
            $$"""{"buckets":{{buckets}},"dbname":"{{cluster}}","created_unix":1755900000}""", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/replicas", "2", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/nodes/shard1a/state", "RUNNING", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/nodes/shard1b/state", "RUNNING", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/dsn",
            "host=h1,h2 port=15000,15000 dbname=shop user=bucket_admin", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard2/replicas", "2", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard2/nodes/shard2a/state", "RUNNING", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard2/nodes/shard2b/state", "RUNNING", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard2/dsn",
            "host=h1,h2 port=15001,15001 dbname=shop user=bucket_admin", null, ct);
        for (var i = 0; i < buckets; i++)
            await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", "shard2", null, ct);
    }

    // Add-декларация в стиле панели (§4.1/§6.1): replicas + nodes + request_*.
    private async Task SeedAddDeclarationAsync(string cluster, string shard)
    {
        var ct = TestContext.Current.CancellationToken;
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
        await Gateway.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
        await Gateway.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "4Gi", null, ct);
        await Gateway.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_disk", "10Gi", null, ct);
    }

    private async Task<ClusterSnapshot> SnapshotAsync(string cluster)
    {
        var range = await Gateway.RangeAsync(Endpoint, "/clusters/", TestContext.Current.CancellationToken);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == cluster);
    }

    [Fact]
    public async Task PanelAddDeclaration_RealRange_ParserDetectsAddCandidate()
    {
        // Arrange — Active-кластер + add-декларация shard3 (сид в стиле панели)
        var cluster = $"sc1{Tag}";
        await SeedActiveClusterAsync(cluster, 6);
        await SeedAddDeclarationAsync(cluster, "shard3");

        // Act — реальный range → парсер → детекция scale-кандидатов
        var snap = await SnapshotAsync(cluster);
        var candidates = ShardScaleClassifier.Detect(snap);

        // Assert — только shard3 кандидат add; живые шарды не помечены
        candidates.Add.Should().Equal("shard3");
        candidates.Remove.Should().BeEmpty();
        snap.Shards.Where(s => s.Name is "shard1" or "shard2")
            .Should().OnlyContain(s => !s.ToRemove);
    }

    [Fact]
    public async Task Marker_RealRange_ParserDetectsRemoveCandidate()
    {
        // Arrange — маркер демонтажа через реальный PUT
        var cluster = $"sc2{Tag}";
        await SeedActiveClusterAsync(cluster, 6);
        var put = await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/state", "TO_REMOVE",
            null, TestContext.Current.CancellationToken);
        put.IsSuccess.Should().BeTrue();

        // Act
        var range = await Gateway.RangeAsync(Endpoint, "/clusters/", TestContext.Current.CancellationToken);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out var errors);
        var candidates = ShardScaleClassifier.Detect(parsed.Value.Single(c => c.Config.Cluster == cluster));

        // Assert — remove-кандидат найден; парсер не пишет parseErrors
        errors.Should().BeEmpty();
        candidates.Remove.Should().Equal("shard1");
    }

    [Fact]
    public async Task RemoveShardProcess_OnRealEtcd_CleansKeysWithRealTxnAndDel()
    {
        // Arrange — помеченный пустой шард (routing весь на shard2), portalloc
        // обоих шардов реальными Put, контейнеры в стаб-драйвере, клэйм наш
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"sc3{Tag}";
        await SeedActiveClusterAsync(cluster, 3);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/state", "TO_REMOVE", null, ct);
        await Gateway.PutAsync(Endpoint, $"/pgworker/portalloc/{cluster}", Portalloc.Serialize(
            new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
                ["shard1/shard1b"] = new("h2", new NodePorts(15000, 18000, 16500)),
                ["shard2/shard2a"] = new("h1", new NodePorts(15001, 18001, 16501)),
                ["shard2/shard2b"] = new("h2", new NodePorts(15001, 18001, 16501)),
            }), null, ct);
        await Gateway.PutAsync(Endpoint, $"/pgworker/evacuations/{cluster}/shard1",
            """{"buckets":{"0":"shard2"},"reason":"shard-dead","evacuated_unix":1,"state":"DONE","returned_unix":null}""",
            null, ct);
        var driver = new StubScaleDriver
        {
            NodeObjects = [$"pgw-{cluster}-shard1-shard1a", $"pgw-{cluster}-shard1-shard1b", $"pgw-{cluster}-shard2-shard2a"],
        };
        // Own-only предочистка клэйма (паттерн SeedAsync Backups-классов): тест не
        // зависит от таймингов TTL соседей по коллекции.
        await Gateway.DeleteAsync(Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        // Own-only teardown: DisposeAsync отзывает lease — ключ исчезает сразу, не по TTL 15с.
        await using var claims = new ClaimStore("/pgworker", [Endpoint], Gateway, TimeProvider.System);
        (await claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var process = new RemoveShardProcess(
            Gateway, [Endpoint], driver, claims, new WorkJournal("/pgworker", Gateway, [Endpoint]), snapshot: null);

        // Act — демонтаж на реальном etcd
        var outcome = await process.TickAsync(await SnapshotAsync(cluster), "shard1", ct);

        // Assert — ключи/порталы/журнал вычищены реальными del; сосед цел
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        driver.RemovedNodes.Should().BeEquivalentTo(["shard1/shard1a", "shard1/shard1b"]);
        var shardPrefix = await Gateway.RangeAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/", ct);
        shardPrefix.Value.Should().BeEmpty();
        var scopePrefix = await Gateway.RangeAsync(Endpoint, $"/service/{cluster}-shard1/", ct);
        scopePrefix.Value.Should().BeEmpty();
        var portalloc = await Gateway.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        portalloc.Value!.Value.Should().NotContain("shard1/");
        portalloc.Value.Value.Should().Contain("shard2/");
        var evacuation = await Gateway.GetAsync(Endpoint, $"/pgworker/evacuations/{cluster}/shard1", ct);
        evacuation.Value.Should().BeNull();
        var sibling = await Gateway.GetAsync(Endpoint, $"/clusters/{cluster}/shards/shard2/dsn", ct);
        sibling.Value.Should().NotBeNull();
    }

    [Fact]
    public async Task RemoveShard_останавливает_агента_WAL_и_чистит_ключ_бэкапов_AC6()
    {
        // Arrange — кластер Active, shard1 с TO_REMOVE-маркером; StubScaleDriver
        // видит объекты нод + живой агент (BackupAgentObjects); etcd-ключ
        // /pgworker/backups/<C>/shard1/wal посажен напрямую
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"sc6{Tag}";
        await SeedActiveClusterAsync(cluster, 3);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/state", "TO_REMOVE", null, ct);
        await Gateway.PutAsync(Endpoint, $"/pgworker/portalloc/{cluster}", Portalloc.Serialize(
            new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
                ["shard1/shard1b"] = new("h2", new NodePorts(15000, 18000, 16500)),
                ["shard2/shard2a"] = new("h1", new NodePorts(15001, 18001, 16501)),
                ["shard2/shard2b"] = new("h2", new NodePorts(15001, 18001, 16501)),
            }), null, ct);
        await Gateway.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/wal",
            """{"state":"ACTIVE","slot":"pgw_bkp_sc6_shard1","master_node":"shard1a","chain_start_segment":"000000010000000000000001","last_received_segment":"000000010000000000000002","last_uploaded_segment":"000000010000000000000002","last_uploaded_unix":1757500000,"lag_segments":1}""",
            null, ct);
        var driver = new StubScaleDriver
        {
            NodeObjects = [$"pgw-{cluster}-shard1-shard1a", $"pgw-{cluster}-shard1-shard1b", $"pgw-{cluster}-shard2-shard2a"],
            BackupAgentObjects =
            [
                new PgWorker.Docker.Engine.DockerContainer(
                    "id-agent", [$"/pgw-backup-wal-{cluster}-shard1"], "running", "bkp-img"),
            ],
        };
        // Own-only: предочистка клэйма + немедленный выпуск (await using).
        await Gateway.DeleteAsync(Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        await using var claims = new ClaimStore("/pgworker", [Endpoint], Gateway, TimeProvider.System);
        (await claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var process = new RemoveShardProcess(
            Gateway, [Endpoint], driver, claims, new WorkJournal("/pgworker", Gateway, [Endpoint]), snapshot: null);

        // Act — тик RemoveShardProcess
        var outcome = await process.TickAsync(await SnapshotAsync(cluster), "shard1", ct);

        // Assert — агент остановлен; docker-объектов шарда нет; ключ wal УДАЛЁН
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        driver.RemovedBackupAgents.Should().Contain($"pgw-backup-wal-{cluster}-shard1");
        driver.BackupAgentObjects.Should().NotContain(c => c.Names.Any(n => n.Contains(cluster)));
        var backupsPrefix = await Gateway.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/", ct);
        backupsPrefix.Value.Should().BeEmpty("ключ wal не переживает демонтаж (CleanKeysAsync)");
    }

    [Fact]
    public async Task ConcurrentMarkerPuts_ConvergeToSameValue()
    {
        // Arrange — конкурентные PUT одного маркера (идемпотентность §4.2)
        var cluster = $"sc4{Tag}";
        await SeedActiveClusterAsync(cluster, 2);
        var ct = TestContext.Current.CancellationToken;

        // Act — два параллельных PUT
        var puts = await Task.WhenAll(
            Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/state", "TO_REMOVE", null, ct),
            Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/state", "TO_REMOVE", null, ct));

        // Assert — оба успеха; значение ровно "TO_REMOVE"
        puts.Should().OnlyContain(p => p.IsSuccess);
        var read = await Gateway.GetAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/state", ct);
        read.Value!.Value.Should().Be("TO_REMOVE");
    }

    [Fact]
    public async Task AddShardDeclaration_ThenMarker_UndeclaredShardDismantledOnRealEtcd()
    {
        // Arrange — add-декларация shard3 (без dsn) + маркер: способ отменить
        // зависший add (Д5) — RemoveShardProcess вычищает декларацию
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"sc5{Tag}";
        await SeedActiveClusterAsync(cluster, 2);
        await SeedAddDeclarationAsync(cluster, "shard3");
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard3/state", "TO_REMOVE", null, ct);
        // Own-only: предочистка клэйма + немедленный выпуск (await using).
        await Gateway.DeleteAsync(Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        await using var claims = new ClaimStore("/pgworker", [Endpoint], Gateway, TimeProvider.System);
        (await claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var process = new RemoveShardProcess(
            Gateway, [Endpoint], new StubScaleDriver(), claims,
            new WorkJournal("/pgworker", Gateway, [Endpoint]), snapshot: null);

        // Act
        var outcome = await process.TickAsync(await SnapshotAsync(cluster), "shard3", ct);

        // Assert — декларация вычищена реальными del (контейнеров не было)
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        var shardPrefix = await Gateway.RangeAsync(Endpoint, $"/clusters/{cluster}/shards/shard3/", ct);
        shardPrefix.Value.Should().BeEmpty();
        var scopePrefix = await Gateway.RangeAsync(Endpoint, $"/service/{cluster}-shard3/", ct);
        scopePrefix.Value.Should().BeEmpty();
    }
}
