using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// POST /api/ha/{scope}/nodes/{node}/recreate против реального etcd:
// постановка маркера TO_RECREATE, идемпотентность, guard'ы (последняя нода,
// все остальные уже пересоздаются), матрица 401/503/404/409.
[Collection("api")]
public class RecreateNodeApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
    : IClassFixture<EtcdContainerFixture>
{
    private readonly AuthWebFactory _factory = factory;

    private void SetLiveSnapshot(ClusterInfo? cluster = null, IReadOnlyList<HaMember>? haMembers = null)
    {
        var clusterName = cluster?.Name ?? "rc";
        var etcd = new EtcdStatus(
            true,
            [new EtcdEndpoint(fixture.Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
            [], [], fixture.Endpoint, false, _factory.Time.GetUtcNow(), 0);
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow()) with
        {
            Etcd = etcd,
            Clusters = cluster is null ? [] : [cluster],
            HaScopes =
            [
                new HaScope(
                    $"{clusterName}-s1", clusterName, "s1", true, "s1a", null, true, null, null, null,
                    haMembers ??
                    [
                        new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, _factory.Time.GetUtcNow(), null, "RUNNING"),
                        new HaMember("s1b", "s1b", 5432, "replica", "streaming", 1L, 0L, _factory.Time.GetUtcNow(), null, "RUNNING"),
                    ],
                    null),
            ],
        };
    }

    private async Task<HttpClient> LoginAsync() => await ApiTestLogin.LoginAsync(_factory);

    private async Task SeedAsync(params (string Key, string Value)[] kvs)
    {
        foreach (var (key, value) in kvs)
            await EtcdSeed.PutAsync(fixture.Endpoint, key, value, TestContext.Current.CancellationToken);
    }

    // Кластер rc с одним шардом s1 и двумя нодами (s1a=leader, s1b=replica).
    private static ClusterInfo TwoNodeCluster(string name, string? nodeBState = "RUNNING") => new(
        name, name, 6, 1755900000, ClusterState.Active,
        [
            new ShardInfo("s1", $"host=s1a,s1b port=5432 dbname={name} user=bucket_admin",
                ["s1a", "s1b"], 5432, name, "bucket_admin", 2, "s1a:5432",
                [
                    new NodeInfo("s1a", "RUNNING"),
                    new NodeInfo("s1b", nodeBState),
                ], null),
        ],
        [.. Enumerable.Range(0, 6).Select(i => new BucketInfo(i, "s1", BucketState.Active, null))],
        []);

    // etcd-сид Active-кластера с двумя нодами.
    private async Task SeedClusterAsync(string name)
    {
        await SeedAsync(
            ($"/clusters/{name}/config", $$"""{"buckets":6,"dbname":"{{name}}","created_unix":1755900000}"""),
            ($"/clusters/{name}/shards/s1/replicas", "2"),
            ($"/clusters/{name}/shards/s1/nodes/s1a/state", "RUNNING"),
            ($"/clusters/{name}/shards/s1/nodes/s1b/state", "RUNNING"));
    }

    private async Task<string?> ReadNodeStateAsync(string name, string node)
    {
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, $"/clusters/{name}/shards/s1/nodes/{node}/state", TestContext.Current.CancellationToken);
        return range.Value.FirstOrDefault(kv => kv.Key == $"/clusters/{name}/shards/s1/nodes/{node}/state")?.Value;
    }

    private async Task<string?> ReadNodeModeAsync(string name, string node)
    {
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, $"/clusters/{name}/shards/s1/nodes/{node}/recreate", TestContext.Current.CancellationToken);
        return range.Value.FirstOrDefault(kv => kv.Key == $"/clusters/{name}/shards/s1/nodes/{node}/recreate")?.Value;
    }

    [Fact]
    public async Task Recreate_WithoutCookie_Returns401()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.PostAsync("/api/ha/rc-s1/nodes/s1b/recreate", null, TestContext.Current.CancellationToken);

        // Assert: default-deny закрывает мутацию без auth
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Recreate_NoSnapshot_Returns503()
    {
        // Arrange
        _factory.Snapshot = null;
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync("/api/ha/rc-s1/nodes/s1b/recreate", null, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    [Fact]
    public async Task Recreate_ValidNode_WritesToRecreateMarker()
    {
        // Arrange
        var cluster = "rcok";
        SetLiveSnapshot(TwoNodeCluster(cluster));
        await SeedClusterAsync(cluster);
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync($"/api/ha/{cluster}-s1/nodes/s1b/recreate", null, TestContext.Current.CancellationToken);

        // Assert: 201 + DTO; в etcd записан TO_RECREATE + режим soft по умолчанию
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("scope").GetString().Should().Be($"{cluster}-s1");
        dto.GetProperty("node").GetString().Should().Be("s1b");
        dto.GetProperty("state").GetString().Should().Be("TO_RECREATE");
        dto.GetProperty("mode").GetString().Should().Be("soft");

        var state = await ReadNodeStateAsync(cluster, "s1b");
        state.Should().Be("TO_RECREATE");
        (await ReadNodeModeAsync(cluster, "s1b")).Should().Be("soft");
    }

    [Fact]
    public async Task Recreate_HardMode_WritesHardMarker()
    {
        // Arrange — оператор выбрал «грубо»: лидер сносится сразу, failover — Patroni
        var cluster = "rchard";
        SetLiveSnapshot(TwoNodeCluster(cluster));
        await SeedClusterAsync(cluster);
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync(
            $"/api/ha/{cluster}-s1/nodes/s1a/recreate", new { mode = "hard" }, TestContext.Current.CancellationToken);

        // Assert: 201; в etcd TO_RECREATE + recreate=hard (режим для лидера)
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("mode").GetString().Should().Be("hard");
        (await ReadNodeStateAsync(cluster, "s1a")).Should().Be("TO_RECREATE");
        (await ReadNodeModeAsync(cluster, "s1a")).Should().Be("hard");
    }

    [Fact]
    public async Task Recreate_InvalidMode_Returns400()
    {
        // Arrange — режим обязан быть soft|hard
        var cluster = "rcbad";
        SetLiveSnapshot(TwoNodeCluster(cluster));
        await SeedClusterAsync(cluster);
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync(
            $"/api/ha/{cluster}-s1/nodes/s1b/recreate", new { mode = "sideways" }, TestContext.Current.CancellationToken);

        // Assert: 400, маркеры в etcd не записаны (сид-состояние RUNNING не тронуто)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Invalid mode");
        (await ReadNodeStateAsync(cluster, "s1b")).Should().Be("RUNNING");
        (await ReadNodeModeAsync(cluster, "s1b")).Should().BeNull();
    }

    [Fact]
    public async Task Recreate_AlreadyToRecreate_ModeRewritten()
    {
        // Arrange: маркер уже стоит (soft); оператор передумал — грубо
        var cluster = "rcidem";
        SetLiveSnapshot(TwoNodeCluster(cluster));
        await SeedClusterAsync(cluster);
        await SeedAsync(
            ($"/clusters/{cluster}/shards/s1/nodes/s1b/state", "TO_RECREATE"),
            ($"/clusters/{cluster}/shards/s1/nodes/s1b/recreate", "soft"));
        using var client = await LoginAsync();

        // Act — повторный POST со сменой режима на висящем маркере
        using var response = await client.PostAsJsonAsync(
            $"/api/ha/{cluster}-s1/nodes/s1b/recreate", new { mode = "hard" }, TestContext.Current.CancellationToken);

        // Assert: 201 (идемпотентность state), режим перезаписан на hard
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("state").GetString().Should().Be("TO_RECREATE");
        dto.GetProperty("mode").GetString().Should().Be("hard");
        (await ReadNodeStateAsync(cluster, "s1b")).Should().Be("TO_RECREATE");
        (await ReadNodeModeAsync(cluster, "s1b")).Should().Be("hard");
    }

    [Fact]
    public async Task Recreate_AlreadyToRecreate_IdempotentNoRewrite()
    {
        // Arrange: маркер уже стоит
        var cluster = "rcidem";
        SetLiveSnapshot(TwoNodeCluster(cluster));
        await SeedClusterAsync(cluster);
        await SeedAsync(($"/clusters/{cluster}/shards/s1/nodes/s1b/state", "TO_RECREATE"));
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync($"/api/ha/{cluster}-s1/nodes/s1b/recreate", null, TestContext.Current.CancellationToken);

        // Assert: 201 (идемпотентность), значение не перезаписано
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("state").GetString().Should().Be("TO_RECREATE");
        (await ReadNodeStateAsync(cluster, "s1b")).Should().Be("TO_RECREATE");
    }

    [Fact]
    public async Task Recreate_LastNode_Returns409()
    {
        // Arrange: только одна нода в шарде
        var cluster = "rclast";
        var singleNodeCluster = new ClusterInfo(
            cluster, cluster, 6, 1755900000, ClusterState.Active,
            [
                new ShardInfo("s1", $"host=s1a port=5432 dbname={cluster} user=bucket_admin",
                    ["s1a"], 5432, cluster, "bucket_admin", 1, "s1a:5432",
                    [new NodeInfo("s1a", "RUNNING")], null),
            ],
            [.. Enumerable.Range(0, 6).Select(i => new BucketInfo(i, "s1", BucketState.Active, null))],
            []);
        SetLiveSnapshot(singleNodeCluster,
            [new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, _factory.Time.GetUtcNow(), null, "RUNNING")]);
        await SeedAsync(
            ($"/clusters/{cluster}/config", $$"""{"buckets":6,"dbname":"{{cluster}}","created_unix":1755900000}"""),
            ($"/clusters/{cluster}/shards/s1/replicas", "1"),
            ($"/clusters/{cluster}/shards/s1/nodes/s1a/state", "RUNNING"));
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync($"/api/ha/{cluster}-s1/nodes/s1a/recreate", null, TestContext.Current.CancellationToken);

        // Assert: 409 — последняя нода, нет источника basebackup
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Recreate rejected");
    }

    [Fact]
    public async Task Recreate_AllOthersRebuilding_Returns409()
    {
        // Arrange: другая нода уже REBUILDING
        var cluster = "rcbusy";
        SetLiveSnapshot(TwoNodeCluster(cluster, nodeBState: "REBUILDING"));
        await SeedClusterAsync(cluster);
        await SeedAsync(($"/clusters/{cluster}/shards/s1/nodes/s1b/state", "REBUILDING"));
        using var client = await LoginAsync();

        // Act: пересоздаём s1a — единственная другая нода уже в rebuild
        using var response = await client.PostAsync($"/api/ha/{cluster}-s1/nodes/s1a/recreate", null, TestContext.Current.CancellationToken);

        // Assert: 409 — все остальные ноды уже пересоздаются
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Recreate rejected");
    }

    [Fact]
    public async Task Recreate_UnknownScope_Returns404()
    {
        // Arrange
        SetLiveSnapshot(TwoNodeCluster("rcscope"));
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync("/api/ha/nope-s1/nodes/s1a/recreate", null, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Not found");
    }

    [Fact]
    public async Task Recreate_UnknownNode_Returns404()
    {
        // Arrange
        var cluster = "rcnode";
        SetLiveSnapshot(TwoNodeCluster(cluster));
        await SeedClusterAsync(cluster);
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync($"/api/ha/{cluster}-s1/nodes/s9z/recreate", null, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Not found");
    }

    [Fact]
    public async Task Recreate_ClusterNotActive_Returns409()
    {
        // Arrange: config с state=removing
        var cluster = "rcnotactive";
        SetLiveSnapshot(TwoNodeCluster(cluster));
        await SeedAsync(
            ($"/clusters/{cluster}/config", $$"""{"buckets":6,"dbname":"{{cluster}}","created_unix":1755900000,"state":"removing"}"""),
            ($"/clusters/{cluster}/shards/s1/replicas", "2"),
            ($"/clusters/{cluster}/shards/s1/nodes/s1a/state", "RUNNING"),
            ($"/clusters/{cluster}/shards/s1/nodes/s1b/state", "RUNNING"));
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsync($"/api/ha/{cluster}-s1/nodes/s1b/recreate", null, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Cluster not active");
    }
}
