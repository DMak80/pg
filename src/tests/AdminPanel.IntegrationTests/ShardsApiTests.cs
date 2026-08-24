using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// POST/DELETE /api/clusters/{c}/shards… против реального etcd: клэйм-гонки,
// компенсация-остатки, идемпотентность маркера, 404/409-матрица (arch/02 §9.5-§9.6, t06).
[Collection("api")]
public class ShardsApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
    : IClassFixture<EtcdContainerFixture>
{
    private readonly AuthWebFactory _factory = factory;

    // Снапшот «живого etcd» + перечисленные кластеры (пред-проверки DELETE — по снапшоту).
    private void SetLiveSnapshot(params ClusterInfo[] clusters)
    {
        var etcd = new EtcdStatus(
            true,
            [new EtcdEndpoint(fixture.Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
            [], [], fixture.Endpoint, false, _factory.Time.GetUtcNow(), 0);
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow()) with
        {
            Etcd = etcd,
            Clusters = [.. clusters],
        };
    }

    private async Task<HttpClient> LoginAsync()
        => await ApiTestLogin.LoginAsync(_factory);

    // Сид ключей кластера прямо в etcd (тем же транспортом, что и панель).
    private async Task SeedAsync(params (string Key, string Value)[] kvs)
    {
        foreach (var (key, value) in kvs)
            await EtcdSeed.PutAsync(fixture.Endpoint, key, value, TestContext.Current.CancellationToken);
    }

    // Кластер Active с двумя полными шардами (replicas + RUNNING-ноды) — база add-тестов.
    private static ClusterInfo TwoShardCluster(string name, int buckets, IReadOnlyList<string> owners)
        => new(
            name, name, buckets, 1755900000, ClusterState.Active,
            [
                new ShardInfo("shard1", "host=shard1a port=5432 dbname=" + name + " user=bucket_admin",
                    ["shard1a"], 5432, name, "bucket_admin", 2, "shard1a:5432",
                    [new NodeInfo("shard1a", "RUNNING"), new NodeInfo("shard1b", "RUNNING")], null),
                new ShardInfo("shard2", "host=shard2a port=5432 dbname=" + name + " user=bucket_admin",
                    ["shard2a"], 5432, name, "bucket_admin", 2, "shard2a:5432",
                    [new NodeInfo("shard2a", "RUNNING"), new NodeInfo("shard2b", "RUNNING")], null),
            ],
            [.. owners.Select((owner, i) => new BucketInfo(i, owner, BucketState.Active, null))],
            []);

    // etcd-сид Active-кластера с двумя шардами (config без state).
    private async Task SeedActiveClusterAsync(string name, int buckets, IReadOnlyList<string> owners)
    {
        var kvs = new List<(string, string)>
        {
            ($"/clusters/{name}/config", $$"""{"buckets":{{buckets}},"dbname":"{{name}}","created_unix":1755900000}"""),
            ("/clusters/" + name + "/shards/shard1/replicas", "2"),
            ("/clusters/" + name + "/shards/shard1/nodes/shard1a/state", "RUNNING"),
            ("/clusters/" + name + "/shards/shard1/nodes/shard1b/state", "RUNNING"),
            ("/clusters/" + name + "/shards/shard2/replicas", "2"),
            ("/clusters/" + name + "/shards/shard2/nodes/shard2a/state", "RUNNING"),
            ("/clusters/" + name + "/shards/shard2/nodes/shard2b/state", "RUNNING"),
        };
        for (var i = 0; i < owners.Count; i++)
            kvs.Add(($"/clusters/{name}/buckets/routing/bucket_{i}", owners[i]));
        await SeedAsync([.. kvs]);
    }

    [Fact]
    public async Task AddShard_WithoutCookie_Returns401()
    {
        // Arrange
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters/shop/shards",
            new { replicas = 2, requestCpu = 0.5m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: default-deny закрывает мутацию как все /api/*
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task AddShard_ActiveCluster_Returns201AndWritesContractKeys()
    {
        // Arrange: Active addshop, 2 шарда, routing 4 — ключи §9.5 1:1
        await SeedActiveClusterAsync("addshop", 4, ["shard1", "shard1", "shard2", "shard2"]);
        SetLiveSnapshot(TwoShardCluster("addshop", 4, ["shard1", "shard1", "shard2", "shard2"]));
        using var client = await LoginAsync();
        var gateway = EtcdTestHarness.NewGateway();
        var before = await gateway.RangeAsync(fixture.Endpoint, "/clusters/addshop/", TestContext.Current.CancellationToken);

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters/addshop/shards",
            new { replicas = 2, requestCpu = 0.5m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: 201 + DTO канона (§1.3): сгенерированное имя shard3
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("name").GetString().Should().Be("shard3");
        dto.GetProperty("cluster").GetString().Should().Be("addshop");
        dto.GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");
        dto.GetProperty("requestCpu").GetString().Should().Be("0.5");
        dto.GetProperty("requestMem").GetString().Should().Be("8Gi");
        dto.GetProperty("requestDisk").GetString().Should().Be("100Gi");

        // Реальный range: ключи §9.5 1:1 (replicas + 2 nodes + 3 request_*)
        var range = await gateway.RangeAsync(fixture.Endpoint, "/clusters/addshop/shards/shard3/", TestContext.Current.CancellationToken);
        range.Value.Select(kv => kv.Key).Should().BeEquivalentTo(
        [
            "/clusters/addshop/shards/shard3/replicas",
            "/clusters/addshop/shards/shard3/nodes/shard3a/state",
            "/clusters/addshop/shards/shard3/nodes/shard3b/state",
        ]);
        range.Value.Single(kv => kv.Key.EndsWith("shard3a/state")).Value.Should().Be("NOT_INITIALIZED");
        range.Value.Single(kv => kv.Key.EndsWith("replicas")).Value.Should().Be("2");
        var requests = await gateway.RangeAsync(fixture.Endpoint, "/service/addshop-shard3/", TestContext.Current.CancellationToken);
        requests.Value.Select(kv => (kv.Key, kv.Value)).Should().BeEquivalentTo(
        [
            ("/service/addshop-shard3/request_cpu", "0.5"),
            ("/service/addshop-shard3/request_mem", "8Gi"),
            ("/service/addshop-shard3/request_disk", "100Gi"),
        ]);

        // Граница §2.1: routing НЕ дописан, config не изменён (сравнение до/после)
        var after = await gateway.RangeAsync(fixture.Endpoint, "/clusters/addshop/", TestContext.Current.CancellationToken);
        after.Value.Where(kv => !kv.Key.Contains("/shard3/")).Select(kv => (kv.Key, kv.Value))
            .Should().BeEquivalentTo(before.Value.Select(kv => (kv.Key, kv.Value)));
    }

    [Fact]
    public async Task AddShard_ConcurrentPosts_One201Other409()
    {
        // Arrange: гонка клэйма — txn атомарен (spec §8)
        await SeedActiveClusterAsync("race", 2, ["shard1", "shard2"]);
        SetLiveSnapshot();
        using var client = await LoginAsync();
        var body = new { replicas = 2, requestCpu = 1m, requestMem = 4, requestDisk = 50 };

        // Act: два ПАРАЛЛЕЛЬНЫХ POST (одно и то же вычисленное имя shard3)
        var firstTask = client.PostAsJsonAsync("/api/clusters/race/shards", body, TestContext.Current.CancellationToken);
        var secondTask = client.PostAsJsonAsync("/api/clusters/race/shards", body, TestContext.Current.CancellationToken);
        using var first = await firstTask;
        using var second = await secondTask;

        // Assert: один 201, другой 409 Shard add rejected (порядок не важен)
        var codes = new[] { first.StatusCode, second.StatusCode }.OrderBy(c => (int)c).ToArray();
        codes.Should().Equal(HttpStatusCode.Created, HttpStatusCode.Conflict);
        var conflictResponse = first.StatusCode == HttpStatusCode.Conflict ? first : second;
        var conflict = await conflictResponse.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        conflict.GetProperty("title").GetString().Should().Be("Shard add rejected");
    }

    [Fact]
    public async Task AddShard_FailedCompensationLeftovers_RepeatGets409()
    {
        // Arrange: «провалившаяся компенсация» — выжил только replicas-ключ shard3
        // (частичная декларация §9.5); nodes/request_* нет
        await SeedActiveClusterAsync("part", 2, ["shard1", "shard2"]);
        await SeedAsync(("/clusters/part/shards/shard3/replicas", "2"));
        SetLiveSnapshot();
        using var client = await LoginAsync();

        // Act: повторный POST вычислит ТО ЖЕ имя (max+1) и проиграет клэйм
        using var response = await client.PostAsJsonAsync(
            "/api/clusters/part/shards",
            new { replicas = 2, requestCpu = 1m, requestMem = 4, requestDisk = 50 },
            TestContext.Current.CancellationToken);

        // Assert: 409 — молча создать «другой» шард повтор не может (остатки — etcdctl)
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, "/clusters/part/shards/shard3/", TestContext.Current.CancellationToken);
        range.Value.Should().ContainSingle(kv => kv.Key == "/clusters/part/shards/shard3/replicas");
    }

    [Fact]
    public async Task AddShard_ClusterNotInitialized_Returns409()
    {
        // Arrange: структура NOT_INITIALIZED-кластера исполняется provisioning'ом (Д9)
        await SeedAsync(
            ("/clusters/fresh/config", """{"buckets":2,"dbname":"fresh","created_unix":1755900000,"state":"NOT_INITIALIZED"}"""));
        SetLiveSnapshot();
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters/fresh/shards",
            new { replicas = 2, requestCpu = 1m, requestMem = 4, requestDisk = 50 },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Shard add rejected");
        problem.GetProperty("detail").GetString().Should().Contain("дождитесь инициализации");
    }

    [Fact]
    public async Task AddShard_ClusterNotFound_Returns404()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters/ghost/shards",
            new { replicas = 2, requestCpu = 1m, requestMem = 4, requestDisk = 50 },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task AddShard_InvalidBody_Returns400WithFieldErrors()
    {
        // Arrange: границы §9.3 — replicas 27, cpu 64.1
        await SeedActiveClusterAsync("valid", 2, ["shard1", "shard2"]);
        SetLiveSnapshot();
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters/valid/shards",
            new { replicas = 27, requestCpu = 64.1m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: ProblemDetails с errors по полям
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Validation failed");
        problem.GetProperty("errors").GetProperty("replicas").GetArrayLength().Should().Be(1);
        problem.GetProperty("errors").GetProperty("requestCpu").GetArrayLength().Should().Be(1);
    }

    [Fact]
    public async Task DeleteShard_WithoutCookie_Returns401()
    {
        // Arrange
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/shop/shards/shard1", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task DeleteShard_EmptyShard_PutsMarkerAndReturns204()
    {
        // Arrange: remshop — shard1 4 бакета, shard2 2, shard3 ПУСТ (routing 4+2)
        await SeedActiveClusterAsync("remshop", 4, ["shard1", "shard1", "shard1", "shard1"]);
        await SeedAsync(
            ("/clusters/remshop/shards/shard2/replicas", "2"),
            ("/clusters/remshop/shards/shard2/nodes/shard2a/state", "RUNNING"),
            ("/clusters/remshop/shards/shard2/nodes/shard2b/state", "RUNNING"),
            ("/clusters/remshop/buckets/routing/bucket_4", "shard2"),
            ("/clusters/remshop/buckets/routing/bucket_5", "shard2"),
            ("/clusters/remshop/shards/shard3/replicas", "2"),
            ("/clusters/remshop/shards/shard3/nodes/shard3a/state", "RUNNING"),
            ("/clusters/remshop/shards/shard3/nodes/shard3b/state", "RUNNING"));
        SetLiveSnapshot(new ClusterInfo(
            "remshop", "remshop", 6, 1755900000, ClusterState.Active,
            [
                new ShardInfo("shard1", "dsn1", ["shard1a"], 5432, "remshop", "bucket_admin", 2, "shard1a:5432", [], null),
                new ShardInfo("shard2", "dsn2", ["shard2a"], 5432, "remshop", "bucket_admin", 2, "shard2a:5432", [], null),
                new ShardInfo("shard3", "", [], null, null, null, 2, null, [], null),
            ],
            [
                new BucketInfo(0, "shard1", BucketState.Active, null),
                new BucketInfo(1, "shard1", BucketState.Active, null),
                new BucketInfo(2, "shard1", BucketState.Active, null),
                new BucketInfo(3, "shard1", BucketState.Active, null),
                new BucketInfo(4, "shard2", BucketState.Active, null),
                new BucketInfo(5, "shard2", BucketState.Active, null),
            ],
            []));
        using var client = await LoginAsync();

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/remshop/shards/shard3", TestContext.Current.CancellationToken);

        // Assert: 204; реальный get — маркер TO_REMOVE
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, "/clusters/remshop/shards/shard3/state", TestContext.Current.CancellationToken);
        range.Value.Should().ContainSingle().Which.Value.Should().Be("TO_REMOVE");

        // Читающий путь: парсер распознаёт маркер (ShardState.ToRemove)
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
        store.Current!.Clusters.Single(c => c.Name == "remshop").Shards.Single(s => s.Name == "shard3")
            .State.Should().Be(ShardState.ToRemove);
    }

    [Fact]
    public async Task DeleteShard_Idempotent_SecondCall204()
    {
        // Arrange: тот же сид, что EmptyShard, но на своём кластере rem2
        await SeedActiveClusterAsync("rem2", 2, ["shard1", "shard1"]);
        await SeedAsync(
            ("/clusters/rem2/shards/shard2/replicas", "2"),
            ("/clusters/rem2/shards/shard2/nodes/shard2a/state", "RUNNING"),
            ("/clusters/rem2/shards/shard2/nodes/shard2b/state", "RUNNING"),
            ("/clusters/rem2/shards/shard3/replicas", "2"),
            ("/clusters/rem2/shards/shard3/nodes/shard3a/state", "RUNNING"));
        SetLiveSnapshot(new ClusterInfo(
            "rem2", "rem2", 2, 1755900000, ClusterState.Active,
            [
                new ShardInfo("shard1", "dsn1", ["shard1a"], 5432, "rem2", "bucket_admin", 2, "shard1a:5432", [], null),
                new ShardInfo("shard2", "dsn2", ["shard2a"], 5432, "rem2", "bucket_admin", 2, "shard2a:5432", [], null),
                new ShardInfo("shard3", "", [], null, null, null, 2, null, [], null),
            ],
            [
                new BucketInfo(0, "shard1", BucketState.Active, null),
                new BucketInfo(1, "shard1", BucketState.Active, null),
            ],
            []));
        using var client = await LoginAsync();

        // Act: два DELETE подряд
        using var first = await client.DeleteAsync("/api/clusters/rem2/shards/shard3", TestContext.Current.CancellationToken);
        using var second = await client.DeleteAsync("/api/clusters/rem2/shards/shard3", TestContext.Current.CancellationToken);

        // Assert: оба 204; значение то же (повторная запись не нужна)
        first.StatusCode.Should().Be(HttpStatusCode.NoContent);
        second.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, "/clusters/rem2/shards/shard3/state", TestContext.Current.CancellationToken);
        range.Value.Should().ContainSingle().Which.Value.Should().Be("TO_REMOVE");
    }

    [Fact]
    public async Task DeleteShard_ShardWithBuckets_Returns409WithCount()
    {
        // Arrange: rem3 — все 4 бакета на shard1 (Д4: быстрая проверка до записи)
        await SeedActiveClusterAsync("rem3", 4, ["shard1", "shard1", "shard1", "shard1"]);
        await SeedAsync(
            ("/clusters/rem3/shards/shard2/replicas", "2"),
            ("/clusters/rem3/shards/shard2/nodes/shard2a/state", "RUNNING"));
        SetLiveSnapshot(new ClusterInfo(
            "rem3", "rem3", 4, 1755900000, ClusterState.Active,
            [
                new ShardInfo("shard1", "dsn1", ["shard1a"], 5432, "rem3", "bucket_admin", 2, "shard1a:5432", [], null),
                new ShardInfo("shard2", "dsn2", ["shard2a"], 5432, "rem3", "bucket_admin", 2, "shard2a:5432", [], null),
            ],
            [.. Enumerable.Range(0, 4).Select(i => new BucketInfo(i, "shard1", BucketState.Active, null))],
            []));
        using var client = await LoginAsync();

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/rem3/shards/shard1", TestContext.Current.CancellationToken);

        // Assert: 409 ProblemDetails с числом и подсказкой; маркер НЕ писался
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Shard remove rejected");
        problem.GetProperty("detail").GetString().Should().Contain("4").And.Contain("перевезите");
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, "/clusters/rem3/shards/shard1/", TestContext.Current.CancellationToken);
        range.Value.Should().NotContain(kv => kv.Key == "/clusters/rem3/shards/shard1/state");
    }

    [Fact]
    public async Task DeleteShard_LastShard_Returns409()
    {
        // Arrange: solo — единственный шард (G7)
        await SeedActiveClusterAsync("solo", 2, ["shard1", "shard1"]);
        SetLiveSnapshot(new ClusterInfo(
            "solo", "solo", 2, 1755900000, ClusterState.Active,
            [new ShardInfo("shard1", "dsn1", ["shard1a"], 5432, "solo", "bucket_admin", 2, "shard1a:5432", [], null)],
            [],
            []));
        using var client = await LoginAsync();

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/solo/shards/shard1", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("detail").GetString().Should().Contain("последний шард");
    }

    [Fact]
    public async Task DeleteShard_UnknownShard_Returns404()
    {
        // Arrange: шард ghost не заявлен (replicas-ключа нет)
        await SeedActiveClusterAsync("rem4", 2, ["shard1", "shard2"]);
        SetLiveSnapshot(TwoShardCluster("rem4", 2, ["shard1", "shard2"]));
        using var client = await LoginAsync();

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/rem4/shards/ghost", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DeleteShard_ClusterNotActive_Returns409()
    {
        // Arrange: кластер удаляется — шардный демонтаж запрещён (§9.6 п.2)
        await SeedAsync(
            ("/clusters/dying/config", """{"buckets":2,"dbname":"dying","created_unix":1755900000,"state":"TO_REMOVE"}"""),
            ("/clusters/dying/shards/shard1/replicas", "2"),
            ("/clusters/dying/shards/shard2/replicas", "2"));
        SetLiveSnapshot();
        using var client = await LoginAsync();

        // Act
        using var response = await client.DeleteAsync(
            "/api/clusters/dying/shards/shard1", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("detail").GetString().Should().Contain("удаляется");
    }
}
