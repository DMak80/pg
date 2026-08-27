using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// POST /api/clusters/{c}/moves против реального etcd: постановка заявок с возрастающими
// requested_unix, идемпотентность повтора, конфликтная заявка, матрица 400/404/409,
// чтение очереди refresher'ом и отдача pendingMoves в деталях кластера
// (arch/02 §9.7, arch/03 §1.5; spec 2026-08-24 §7.4-§7.5).
[Collection("api")]
public class MovesApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
    : IClassFixture<EtcdContainerFixture>
{
    private readonly AuthWebFactory _factory = factory;

    // Снапшот «живого etcd» + кластер для пред-проверок (паттерн ShardsApiTests).
    private void SetLiveSnapshot(ClusterInfo? cluster = null)
    {
        var etcd = new EtcdStatus(
            true,
            [new EtcdEndpoint(fixture.Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
            [], [], fixture.Endpoint, false, _factory.Time.GetUtcNow(), 0);
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow()) with
        {
            Etcd = etcd,
            Clusters = cluster is null ? [] : [cluster],
        };
    }

    private async Task<HttpClient> LoginAsync() => await ApiTestLogin.LoginAsync(_factory);

    private async Task SeedAsync(params (string Key, string Value)[] kvs)
    {
        foreach (var (key, value) in kvs)
            await EtcdSeed.PutAsync(fixture.Endpoint, key, value, TestContext.Current.CancellationToken);
    }

    // Кластер Active с двумя шардами; имя уникально на тест — общий etcd коллекции
    // "api" не должен протекать между тестами (образец ShardsApiTests).
    private static ClusterInfo TwoShardCluster(string name, bool targetRemoving = false) => new(
        name, name, 6, 1755900000, ClusterState.Active,
        [
            new ShardInfo("shard1", $"host=shard1a port=5432 dbname={name} user=bucket_admin",
                ["shard1a"], 5432, name, "bucket_admin", 2, "shard1a:5432",
                [new NodeInfo("shard1a", "RUNNING")], null),
            new ShardInfo("shard2", $"host=shard2a port=5432 dbname={name} user=bucket_admin",
                ["shard2a"], 5432, name, "bucket_admin", 2, "shard2a:5432",
                [new NodeInfo("shard2a", "RUNNING")], null,
                targetRemoving ? ShardState.ToRemove : ShardState.Active),
        ],
        [.. Enumerable.Range(0, 6).Select(i => new BucketInfo(i, i % 2 == 0 ? "shard1" : "shard2", BucketState.Active, null))],
        []);

    // etcd-сид Active-кластера: config + 2 шарда + routing (0,2,4 — shard1).
    private async Task SeedShopAsync(string name)
    {
        var kvs = new List<(string, string)>
        {
            ($"/clusters/{name}/config", $$"""{"buckets":6,"dbname":"{{name}}","created_unix":1755900000}"""),
            ($"/clusters/{name}/shards/shard1/replicas", "2"),
            ($"/clusters/{name}/shards/shard1/nodes/shard1a/state", "RUNNING"),
            ($"/clusters/{name}/shards/shard2/replicas", "2"),
            ($"/clusters/{name}/shards/shard2/nodes/shard2a/state", "RUNNING"),
        };
        for (var i = 0; i < 6; i++)
            kvs.Add(($"/clusters/{name}/buckets/routing/bucket_{i}", i % 2 == 0 ? "shard1" : "shard2"));
        await SeedAsync([.. kvs]);
    }

    private async Task<Dictionary<string, string>> ReadMovesAsync(string name)
    {
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, $"/pgworker/moves/{name}/", TestContext.Current.CancellationToken);
        return range.Value.ToDictionary(kv => kv.Key, kv => kv.Value);
    }

    [Fact]
    public async Task Moves_WithoutCookie_Returns401()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvanon/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0 } },
            TestContext.Current.CancellationToken);

        // Assert: default-deny закрывает мутацию как все /api/*
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Moves_QueueBuckets_WritesAscendingUnixAndCanonicalBody()
    {
        // Arrange
        SetLiveSnapshot(TwoShardCluster("mvqueue"));
        await SeedShopAsync("mvqueue");
        using var client = await LoginAsync();

        // Act: порядок в массиве обратный — обработка всё равно по возрастанию id
        using var response = await client.PostAsJsonAsync("/api/clusters/mvqueue/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 4, 0, 2 } },
            TestContext.Current.CancellationToken);

        // Assert: 201; в etcd 3 заявки с строго возрастающими requested_unix (Д2/Д3)
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("queued").EnumerateArray().Select(e => e.GetInt32()).Should().BeInAscendingOrder();

        var moves = await ReadMovesAsync("mvqueue");
        moves.Keys.Should().BeEquivalentTo(
        [
            "/pgworker/moves/mvqueue/bucket_0", "/pgworker/moves/mvqueue/bucket_2", "/pgworker/moves/mvqueue/bucket_4",
        ]);
        var unixes = moves.Values
            .Select(v => JsonDocument.Parse(v).RootElement.GetProperty("requested_unix").GetInt64())
            .ToList();
        unixes.Should().OnlyHaveUniqueItems().And.BeInAscendingOrder();
        moves["/pgworker/moves/mvqueue/bucket_0"].Should().Contain("\"op\":\"move\"")
            .And.Contain("\"to\":\"shard2\"").And.Contain("\"requested_by\":\"admin\"");
    }

    [Fact]
    public async Task Moves_Repeat_IdempotentAllSkippedWithoutRewrite()
    {
        // Arrange: первый POST ставит заявки
        SetLiveSnapshot(TwoShardCluster("mvrepeat"));
        await SeedShopAsync("mvrepeat");
        using var client = await LoginAsync();
        using var first = await client.PostAsJsonAsync("/api/clusters/mvrepeat/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0, 2 } },
            TestContext.Current.CancellationToken);
        first.StatusCode.Should().Be(HttpStatusCode.Created);
        var before = (await ReadMovesAsync("mvrepeat"))["/pgworker/moves/mvrepeat/bucket_0"];

        // Act: повтор того же тела
        using var second = await client.PostAsJsonAsync("/api/clusters/mvrepeat/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0, 2 } },
            TestContext.Current.CancellationToken);

        // Assert: 201, всё в skipped; значение ключа НЕ перезаписано (Д6)
        second.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await second.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("queued").GetArrayLength().Should().Be(0);
        dto.GetProperty("skipped").EnumerateArray().Select(e => e.GetInt32())
            .Should().BeEquivalentTo([0, 2]);
        (await ReadMovesAsync("mvrepeat"))["/pgworker/moves/mvrepeat/bucket_0"].Should().Be(before);
    }

    [Fact]
    public async Task Moves_ConflictingExistingTicket_Returns409BeforeWrites()
    {
        // Arrange: на bucket_0 стоит иная заявка (to=shard9)
        SetLiveSnapshot(TwoShardCluster("mvconflict"));
        await SeedShopAsync("mvconflict");
        await SeedAsync(("/pgworker/moves/mvconflict/bucket_0",
            """{"op":"move","to":"shard9","requested_unix":10,"requested_by":"etcdctl"}"""));
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvconflict/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0, 2 } },
            TestContext.Current.CancellationToken);

        // Assert: 409; НИ одной новой заявки (Д7)
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await ReadMovesAsync("mvconflict")).Keys.Should().BeEquivalentTo(["/pgworker/moves/mvconflict/bucket_0"]);
    }

    [Fact]
    public async Task Moves_BucketNotOnSource_Returns409()
    {
        // Arrange: бакет 1 принадлежит shard2
        SetLiveSnapshot(TwoShardCluster("mvowner"));
        await SeedShopAsync("mvowner");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvowner/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 1 } },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await ReadMovesAsync("mvowner")).Should().BeEmpty();
    }

    [Fact]
    public async Task Moves_TargetToRemove_Returns409()
    {
        // Arrange: приёмник в демонтаже (Д9)
        SetLiveSnapshot(TwoShardCluster("mvtorm", targetRemoving: true));
        await SeedShopAsync("mvtorm");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvtorm/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0 } },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task Moves_EmptyBuckets_Returns400()
    {
        // Arrange
        SetLiveSnapshot(TwoShardCluster("mvempty"));
        await SeedShopAsync("mvempty");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvempty/moves",
            new { from = "shard1", to = "shard2", buckets = Array.Empty<int>() },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Moves_FromEqualsTo_Returns400()
    {
        // Arrange
        SetLiveSnapshot(TwoShardCluster("mvsame"));
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvsame/moves",
            new { from = "shard1", to = "shard1", buckets = new[] { 0 } },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Moves_EmptyObjectBody_Returns400()
    {
        // Arrange: тело {} — биндинг даёт null-поля; 400, а не 500-NRE
        SetLiveSnapshot(TwoShardCluster("mvnull"));
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvnull/moves",
            new { }, TestContext.Current.CancellationToken);

        // Assert: errors по from/to/buckets; в etcd ничего не записано
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await ReadMovesAsync("mvnull")).Should().BeEmpty();
    }

    [Fact]
    public async Task Moves_UnknownShard_Returns404()
    {
        // Arrange
        SetLiveSnapshot(TwoShardCluster("mvshard"));
        await SeedShopAsync("mvshard");
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/mvshard/moves",
            new { from = "shard1", to = "shard9", buckets = new[] { 0 } },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Moves_RefresherPicksUpQueueTickets()
    {
        // Arrange: сид demo уже содержит заявку bucket_13 (EtcdSeed)
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);

        // Act
        (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();

        // Assert: заявка в снапшоте (Д10)
        var ticket = store.Current!.MoveTickets.Single(t => t.Cluster == "demo");
        ticket.Bucket.Should().Be("bucket_13");
        ticket.Op.Should().Be("move");
        ticket.To.Should().Be("s1");
        ticket.RequestedBy.Should().Be("ops");

        // Assert-2: GET /api/clusters/{c} отдаёт pendingMoves (spec §7.5) —
        // снапшот refresher'а в API-стор, поля camelCase.
        _factory.Snapshot = store.Current;
        using var client = await LoginAsync();
        using var details = await client.GetAsync("/api/clusters/demo", TestContext.Current.CancellationToken);
        details.StatusCode.Should().Be(HttpStatusCode.OK);
        var json = await details.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        var pending = json.GetProperty("pendingMoves").EnumerateArray()
            .Single(t => t.GetProperty("bucket").GetString() == "bucket_13");
        pending.GetProperty("op").GetString().Should().Be("move");
        pending.GetProperty("to").GetString().Should().Be("s1");
        pending.GetProperty("bucketId").GetInt32().Should().Be(13);
        pending.GetProperty("requestedBy").GetString().Should().Be("ops");
    }
}
