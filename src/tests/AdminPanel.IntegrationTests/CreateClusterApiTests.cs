using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// POST /api/clusters против реального etcd: свой контейнер (мутация сида —
// прецедент InspectionSeededAnomaliesApiTests), снапшот хоста указывает на него.
[Collection("api")]
public class CreateClusterApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
    : IClassFixture<EtcdContainerFixture>
{
    private readonly AuthWebFactory _factory = factory;

    // Снапшот «живого etcd»: единственный endpoint = контейнер, ActiveEndpoint на него.
    private void SetLiveSnapshot()
    {
        var etcd = new EtcdStatus(
            true,
            [new EtcdEndpoint(fixture.Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
            [], [], fixture.Endpoint, false, _factory.Time.GetUtcNow(), 0);
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow()) with { Etcd = etcd };
    }

    [Fact]
    public async Task Create_WithoutCookie_Returns401()
    {
        // Arrange
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters", new { name = "x", buckets = 1, shards = 1, replicas = 1, requestCpu = 1, requestMem = 1, requestDisk = 1 },
            TestContext.Current.CancellationToken);

        // Assert: default-deny guard закрывает мутацию как все /api/*
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Create_Valid_WritesContractKeysToEtcd()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "shop", buckets = 4, shards = 2, replicas = 2, requestCpu = 0.5m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: 201 + Location + DTO канона (arch/03 §1.1)
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Headers.Location!.ToString().Should().Be("/api/clusters/shop");
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");
        // Тело без sharded — обратная совместимость: ответ трактует как sharded=true
        dto.GetProperty("sharded").GetBoolean().Should().BeTrue();
        dto.GetProperty("requestCpu").GetString().Should().Be("0.5");
        dto.GetProperty("requestMem").GetString().Should().Be("8Gi");

        // Ключи в etcd — ровно контракт arch/02 §9.1 (через реальный gateway)
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, "/clusters/shop/", TestContext.Current.CancellationToken);
        range.Value.Select(kv => kv.Key).Should().BeEquivalentTo(
        [
            "/clusters/shop/config",
            "/clusters/shop/shards/shard1/replicas",
            "/clusters/shop/shards/shard1/nodes/shard1a/state",
            "/clusters/shop/shards/shard1/nodes/shard1b/state",
            "/clusters/shop/shards/shard2/replicas",
            "/clusters/shop/shards/shard2/nodes/shard2a/state",
            "/clusters/shop/shards/shard2/nodes/shard2b/state",
            "/clusters/shop/buckets/routing/bucket_0", "/clusters/shop/buckets/routing/bucket_1",
            "/clusters/shop/buckets/routing/bucket_2", "/clusters/shop/buckets/routing/bucket_3",
            "/clusters/shop/buckets/status/bucket_0", "/clusters/shop/buckets/status/bucket_1",
            "/clusters/shop/buckets/status/bucket_2", "/clusters/shop/buckets/status/bucket_3",
        ]);
        range.Value.Single(kv => kv.Key == "/clusters/shop/config").Value.Should().Contain("\"state\":\"NOT_INITIALIZED\"");
        var requests = await gateway.RangeAsync(fixture.Endpoint, "/service/shop-", TestContext.Current.CancellationToken);
        requests.Value.Select(kv => kv.Key).Should().BeEquivalentTo(
        [
            "/service/shop-shard1/request_cpu", "/service/shop-shard1/request_mem", "/service/shop-shard1/request_disk",
            "/service/shop-shard2/request_cpu", "/service/shop-shard2/request_mem", "/service/shop-shard2/request_disk",
        ]);

        // routing — блочное распределение (arch/02 §9.1.1): 4×2 → 0,1=shard1; 2,3=shard2
        // (порядок ключей bucket_0..3 лексикографичен — одна разрядность)
        var routing = range.Value
            .Where(kv => kv.Key.StartsWith("/clusters/shop/buckets/routing/"))
            .OrderBy(kv => kv.Key)
            .Select(kv => kv.Value).ToArray();
        routing.Should().Equal("shard1", "shard1", "shard2", "shard2");
    }

    [Fact]
    public async Task Create_Duplicate_Returns409()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);
        var body = new { name = "dup", buckets = 1, shards = 1, replicas = 1, requestCpu = 1m, requestMem = 1, requestDisk = 1 };

        // Act
        using var first = await client.PostAsJsonAsync("/api/clusters", body, TestContext.Current.CancellationToken);
        using var second = await client.PostAsJsonAsync("/api/clusters", body, TestContext.Current.CancellationToken);

        // Assert: клэйм атомарен — второй POST не прошёл
        first.StatusCode.Should().Be(HttpStatusCode.Created);
        second.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await second.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Cluster already exists");
    }

    [Fact]
    public async Task Create_CanonicalTenByThree_WritesBlockRouting()
    {
        // Arrange: канон spec §2.1 — 10 бакетов × 3 шарда, остаток среднему шарду
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "canon10", buckets = 10, shards = 3, replicas = 2, requestCpu = 0.5m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: через реальный gateway — блоки 3+4+3 (arch/02 §9.1.1)
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(
            fixture.Endpoint, "/clusters/canon10/buckets/routing/", TestContext.Current.CancellationToken);
        var routing = range.Value.OrderBy(kv => kv.Key).Select(kv => kv.Value).ToArray();
        routing.Should().Equal(
            "shard1", "shard1", "shard1",
            "shard2", "shard2", "shard2", "shard2",
            "shard3", "shard3", "shard3");
    }

    [Theory]
    [InlineData("Bad-Name", 4, 2, 2, 0.5, 8, 100, "name")]
    [InlineData("ok", 0, 1, 2, 0.5, 8, 100, "buckets")]
    [InlineData("ok", 4, 8, 2, 0.5, 8, 100, "shards")]      // шардов больше бакетов
    [InlineData("ok", 4, 2, 0, 0.5, 8, 100, "replicas")]
    [InlineData("ok", 4, 2, 2, 0.001, 8, 100, "requestCpu")]
    [InlineData("ok", 4, 2, 2, 0.5, 0, 100, "requestMem")]
    [InlineData("ok", 4, 2, 2, 0.5, 8, 0, "requestDisk")]
    public async Task Create_Invalid_Returns400WithFieldErrors(
        string name, int buckets, int shards, int replicas, decimal cpu, int mem, int disk, string field)
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name, buckets, shards, replicas, requestCpu = cpu, requestMem = mem, requestDisk = disk },
            TestContext.Current.CancellationToken);

        // Assert: ProblemDetails 400, errors содержит провалившееся поле (arch/03 §1.1)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("errors").GetProperty(field).GetArrayLength().Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Create_NoSnapshot_Returns503()
    {
        // Arrange
        _factory.Snapshot = null;
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "x", buckets = 1, shards = 1, replicas = 1, requestCpu = 1m, requestMem = 1, requestDisk = 1 },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    [Fact]
    public async Task Create_RefresherNextTick_PicksUpNewCluster()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);
        using var created = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "fresh", buckets = 2, shards = 1, replicas = 2, requestCpu = 2m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);
        created.StatusCode.Should().Be(HttpStatusCode.Created);

        // Act: «следующий тик» — RefreshOnce реального refresher'а (spec t12 §3.10)
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();

        // Assert: кластер распознан (NOT_INITIALIZED), заявки видны в scope
        var cluster = store.Current!.Clusters.Single(c => c.Name == "fresh");
        cluster.State.Should().Be(ClusterState.NotInitialized);
        cluster.Shards.Single().Nodes.Should().HaveCount(2);
        cluster.Buckets.Should().OnlyContain(b => b.State == BucketState.NotInitialized);
        var scope = store.Current.HaScopes.Single(s => s.Scope == "fresh-shard1");
        scope.RequestCpu.Should().Be("2");
        scope.RequestMem.Should().Be("8Gi");
        scope.RequestDisk.Should().Be("100Gi");
    }

    [Fact]
    public async Task Create_SingleWithoutBucketsShards_WritesDegenerateStructure()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act: нешардированная — buckets/shards в теле отсутствуют вовсе (arch/03 §1.1)
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "solo", sharded = false, replicas = 2, requestCpu = 0.5m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: 201 + вырожденный DTO (sharded=false, 1/1)
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("sharded").GetBoolean().Should().BeFalse();
        dto.GetProperty("bucketsCount").GetInt32().Should().Be(1);
        dto.GetProperty("shardsTotal").GetInt32().Should().Be(1);

        // Ключи в etcd — ровно вырожденная структура arch/02 §9.1
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, "/clusters/solo/", TestContext.Current.CancellationToken);
        range.Value.Select(kv => kv.Key).Should().BeEquivalentTo(
        [
            "/clusters/solo/config",
            "/clusters/solo/shards/shard1/replicas",
            "/clusters/solo/shards/shard1/nodes/shard1a/state",
            "/clusters/solo/shards/shard1/nodes/shard1b/state",
            "/clusters/solo/buckets/routing/bucket_0",
            "/clusters/solo/buckets/status/bucket_0",
        ]);
        range.Value.Single(kv => kv.Key == "/clusters/solo/config").Value.Should().Contain("\"buckets\":1");
        var requests = await gateway.RangeAsync(fixture.Endpoint, "/service/solo-", TestContext.Current.CancellationToken);
        requests.Value.Select(kv => kv.Key).Should().BeEquivalentTo(
        [
            "/service/solo-shard1/request_cpu",
            "/service/solo-shard1/request_mem",
            "/service/solo-shard1/request_disk",
        ]);
    }

    [Fact]
    public async Task Create_SingleWithGarbageBuckets_IgnoresAndNormalizes()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act: sharded=false + невалидные buckets/shards — сервер игнорирует
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "solo2", sharded = false, buckets = 99999, shards = -3, replicas = 2, requestCpu = 1m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: 201 (не 400) и вырожденная структура — без bucket_1/shard2
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("bucketsCount").GetInt32().Should().Be(1);
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, "/clusters/solo2/", TestContext.Current.CancellationToken);
        range.Value.Select(kv => kv.Key).Where(k => k.Contains("bucket_1") || k.Contains("shard2"))
            .Should().BeEmpty();
    }
}
