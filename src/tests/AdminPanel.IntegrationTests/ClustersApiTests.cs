using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit;

namespace AdminPanel.IntegrationTests;

// HTTP-контракт кластерных эндпоинтов: 401/503/200/404/400/фильтры (spec §9.2).
[Collection("api")]
public class ClustersApiTests
{
    private readonly AuthWebFactory _factory;

    public ClustersApiTests(AuthWebFactory factory) => _factory = factory;

    private Task<HttpClient> LoginAsync() => ApiTestLogin.LoginAsync(_factory);

    private async Task<JsonElement> GetJsonAsync(HttpClient client, string url)
    {
        using var response = await client.GetAsync(url, TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        return await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
    }

    // Порядок Arrange: сначала логин (+61 c окна лимитера), затем снапшот по текущему времени
    // фабрики — ageSec/snapshotAgeMs считаются от factory.Time (прецедент t04).
    private void SetClusteredSnapshot()
        => _factory.Snapshot = InspectionSnapshots.Clustered(_factory.Time.Utc, _factory.Time.Utc);

    [Fact]
    public async Task Clusters_WithoutCookie_Return401()
    {
        // Arrange
        using var client = _factory.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

        // Act
        var list = await client.GetAsync("/api/clusters", TestContext.Current.CancellationToken);
        var details = await client.GetAsync("/api/clusters/demo", TestContext.Current.CancellationToken);

        // Assert: default-deny guard закрыл новые эндпоинты без правок auth.
        list.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        details.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Clusters_NoSnapshot_Return503ProblemDetails()
    {
        // Arrange
        _factory.Snapshot = null;
        using var client = await LoginAsync();

        // Act
        var list = await client.GetAsync("/api/clusters", TestContext.Current.CancellationToken);
        var details = await client.GetAsync("/api/clusters/demo", TestContext.Current.CancellationToken);

        // Assert
        list.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        list.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
        var body = await list.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Snapshot not ready");
        details.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    [Fact]
    public async Task Clusters_WithSnapshot_ReturnSummaries()
    {
        // Arrange
        using var client = await LoginAsync();
        SetClusteredSnapshot();

        // Act
        var clusters = await GetJsonAsync(client, "/api/clusters");

        // Assert: сводка кластера фикстуры (spec §9.2).
        clusters.GetArrayLength().Should().Be(1);
        var summary = clusters[0];
        summary.GetProperty("name").GetString().Should().Be("demo");
        summary.GetProperty("dbName").GetString().Should().Be("demo");
        summary.GetProperty("bucketsCount").GetInt32().Should().Be(16);
        summary.GetProperty("shardsTotal").GetInt32().Should().Be(2);
        summary.GetProperty("shardsWithMaster").GetInt32().Should().Be(1);
        summary.GetProperty("activeMoves").GetInt32().Should().Be(3);
        summary.GetProperty("incomplete").GetBoolean().Should().BeFalse();
    }

    [Fact]
    public async Task ClusterDetails_ReturnsConfigShardsBucketsHeals()
    {
        // Arrange
        using var client = await LoginAsync();
        SetClusteredSnapshot();

        // Act
        var dto = await GetJsonAsync(client, "/api/clusters/demo");

        // Assert
        dto.GetProperty("name").GetString().Should().Be("demo");
        dto.GetProperty("createdUnix").GetInt64().Should().Be(1755800000);
        var shards = dto.GetProperty("shards");
        shards.GetArrayLength().Should().Be(2);
        shards[0].GetProperty("hosts").GetArrayLength().Should().Be(2);
        shards[0].GetProperty("masterLeaseAlive").GetBoolean().Should().BeTrue();
        shards[1].GetProperty("masterLeaseAlive").GetBoolean().Should().BeFalse();
        shards[1].GetProperty("masterAddress").ValueKind.Should().Be(JsonValueKind.Null);
        shards[0].GetProperty("runtime").ValueKind.Should().Be(JsonValueKind.Null); // данные — t06 (spec §3.14)
        dto.GetProperty("buckets").GetArrayLength().Should().Be(16);
        var heals = dto.GetProperty("heals");
        heals.GetArrayLength().Should().Be(2);
        heals[0].GetProperty("bucket").GetString().Should().Be("bucket_5"); // новые сверху (spec §3.3)
        var standNodes = dto.GetProperty("standNodes"); // стендовая топология (t08 spec §8)
        standNodes.GetArrayLength().Should().Be(2);
        standNodes[0].GetProperty("name").GetString().Should().Be("node1");
        standNodes[0].GetProperty("address").GetString().Should().Be("10.0.0.5");
        standNodes[1].GetProperty("address").ValueKind.Should().Be(JsonValueKind.Null);
    }

    [Fact]
    public async Task ClusterDetails_AgeSec_ForNonActiveBuckets()
    {
        // Arrange
        using var client = await LoginAsync();
        SetClusteredSnapshot();

        // Act
        var dto = await GetJsonAsync(client, "/api/clusters/demo");

        // Assert: возраст не-ACTIVE от updated_unix; ACTIVE — move/ageSec null (spec §3.7).
        var buckets = dto.GetProperty("buckets");
        buckets[1].GetProperty("state").GetString().Should().Be("SYNCING");
        buckets[1].GetProperty("ageSec").GetInt64().Should().Be(30);
        buckets[1].GetProperty("move").GetProperty("target").GetString().Should().Be("s2");
        buckets[2].GetProperty("state").GetString().Should().Be("FROZEN");
        buckets[2].GetProperty("ageSec").GetInt64().Should().Be(10);
        buckets[3].GetProperty("state").GetString().Should().Be("ABORTING");
        buckets[3].GetProperty("move").GetProperty("lastError").GetString().Should().Be("receiver went away");
        buckets[0].GetProperty("state").GetString().Should().Be("ACTIVE");
        buckets[0].GetProperty("ageSec").ValueKind.Should().Be(JsonValueKind.Null);
        buckets[0].GetProperty("move").ValueKind.Should().Be(JsonValueKind.Null);
    }

    [Fact]
    public async Task ClusterDetails_OwnerFilter_ReturnsOnlyMatching()
    {
        // Arrange
        using var client = await LoginAsync();
        SetClusteredSnapshot();

        // Act
        var dto = await GetJsonAsync(client, "/api/clusters/demo?owner=s2");

        // Assert: 7 бакетов s2 (6 routing + ABORTING bucket_3); shards/heals не фильтруются (spec §3.9).
        dto.GetProperty("buckets").GetArrayLength().Should().Be(7);
        dto.GetProperty("shards").GetArrayLength().Should().Be(2);
        dto.GetProperty("heals").GetArrayLength().Should().Be(2);
    }

    [Fact]
    public async Task ClusterDetails_StateFilter_ActiveIncluded()
    {
        // Arrange
        using var client = await LoginAsync();
        SetClusteredSnapshot();

        // Act
        var active = await GetJsonAsync(client, "/api/clusters/demo?state=ACTIVE");
        var syncing = await GetJsonAsync(client, "/api/clusters/demo?state=SYNCING");
        var both = await GetJsonAsync(client, "/api/clusters/demo?owner=s2&state=ABORTING");

        // Assert: ACTIVE входит в фильтр (roadmap t05); фильтры сочетаются (AND).
        active.GetProperty("buckets").GetArrayLength().Should().Be(13);
        syncing.GetProperty("buckets").GetArrayLength().Should().Be(1);
        both.GetProperty("buckets").GetArrayLength().Should().Be(1);
    }

    [Fact]
    public async Task ClusterDetails_UnknownOwner_ReturnsEmptyBuckets()
    {
        // Arrange
        using var client = await LoginAsync();
        SetClusteredSnapshot();

        // Act
        var dto = await GetJsonAsync(client, "/api/clusters/demo?owner=nope");

        // Assert: пустой buckets и 200 — имена шардов эволюционируют (spec §3.9).
        dto.GetProperty("buckets").GetArrayLength().Should().Be(0);
    }

    [Fact]
    public async Task ClusterDetails_UnknownCluster_Returns404ProblemDetails()
    {
        // Arrange
        using var client = await LoginAsync();
        SetClusteredSnapshot();

        // Act
        using var response = await client.GetAsync("/api/clusters/ghost", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Cluster not found");
    }

    [Fact]
    public async Task ClusterDetails_InvalidState_Returns400ProblemDetails()
    {
        // Arrange
        using var client = await LoginAsync();
        SetClusteredSnapshot();

        // Act
        using var response = await client.GetAsync(
            "/api/clusters/demo?state=bogus", TestContext.Current.CancellationToken);

        // Assert: опечатка фронта ловится сразу, а не пустым списком (spec §3.9).
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
    }

    [Fact]
    public async Task Clusters_IncompleteCluster_Flagged()
    {
        // Arrange: ghost без config — incomplete=true, dbname null (spec §9.2).
        using var client = await LoginAsync();
        var clustered = InspectionSnapshots.Clustered(_factory.Time.Utc, _factory.Time.Utc);
        _factory.Snapshot = clustered with
        {
            Clusters =
            [
                .. clustered.Clusters,
                new ClusterInfo("ghost", null, 0, null, ClusterState.Active, [], [], []),
            ],
        };

        // Act
        var clusters = await GetJsonAsync(client, "/api/clusters");

        // Assert
        clusters.GetArrayLength().Should().Be(2);
        clusters[1].GetProperty("name").GetString().Should().Be("ghost");
        clusters[1].GetProperty("incomplete").GetBoolean().Should().BeTrue();
        clusters[1].GetProperty("dbName").ValueKind.Should().Be(JsonValueKind.Null);
    }

    [Fact]
    public async Task Clusters_NotInitializedCluster_FlaggedInSummaryAndDetails()
    {
        // Arrange: fresh — 1 шард (nodes a/b), бакеты NOT_INITIALIZED, scope с заявкой
        using var client = await LoginAsync();
        var unix = _factory.Time.Utc.ToUnixTimeSeconds();
        var cluster = new ClusterInfo("fresh", "fresh", 2, 1755900000, ClusterState.NotInitialized,
            [new ShardInfo("shard1", "", [], null, null, null, 2, null,
                [new NodeInfo("shard1a", "NOT_INITIALIZED"), new NodeInfo("shard1b", "NOT_INITIALIZED")], null)],
            [
                new BucketInfo(0, "shard1", BucketState.NotInitialized,
                    new MoveInfo("shard1", null, null, unix - 100, null, null)),
                new BucketInfo(1, "shard1", BucketState.NotInitialized,
                    new MoveInfo("shard1", null, null, unix - 100, null, null)),
            ], []);
        var scope = new AdminPanel.Core.HaScope("fresh-shard1", "fresh", "shard1", true, null, null, false,
            "2", "8Gi", "100Gi", [], null);
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc) with
        {
            Clusters = [cluster],
            HaScopes = [scope],
        };

        // Act
        var summary = await GetJsonAsync(client, "/api/clusters");
        var details = await GetJsonAsync(client, "/api/clusters/fresh");
        var filtered = await GetJsonAsync(client, "/api/clusters/fresh?state=NOT_INITIALIZED");

        // Assert: сводка (notInitialized, activeMoves=0), детали (state/nodes/requests), фильтр
        summary[0].GetProperty("notInitialized").GetBoolean().Should().BeTrue();
        summary[0].GetProperty("activeMoves").GetInt32().Should().Be(0);
        details.GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");
        var shard = details.GetProperty("shards")[0];
        shard.GetProperty("nodes").GetArrayLength().Should().Be(2);
        shard.GetProperty("requests").GetProperty("cpu").GetString().Should().Be("2");
        details.GetProperty("buckets")[0].GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");
        filtered.GetProperty("buckets").GetArrayLength().Should().Be(2);
    }

    [Fact]
    public async Task ClusterDetails_ShardedFlag_SingleFalse_MultiTrue()
    {
        // Arrange: lone — нешардированная 1×1 (arch/02 §9.1); mini — 2×1; паттерн
        // Clusters_NotInitializedCluster_FlaggedInSummaryAndDetails (поверх Fixture)
        using var client = await LoginAsync();
        var lone = new ClusterInfo("lone", "lone", 1, 1755900000, ClusterState.NotInitialized,
            [new ShardInfo("shard1", "", [], null, null, null, 2, null,
                [new NodeInfo("shard1a", "NOT_INITIALIZED"), new NodeInfo("shard1b", "NOT_INITIALIZED")], null)],
            [new BucketInfo(0, "shard1", BucketState.NotInitialized, null)], []);
        var mini = new ClusterInfo("mini", "mini", 2, 1755900000, ClusterState.NotInitialized,
            [new ShardInfo("shard1", "", [], null, null, null, 2, null,
                [new NodeInfo("shard1a", "NOT_INITIALIZED"), new NodeInfo("shard1b", "NOT_INITIALIZED")], null)],
            [
                new BucketInfo(0, "shard1", BucketState.NotInitialized, null),
                new BucketInfo(1, "shard1", BucketState.NotInitialized, null),
            ], []);
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc) with { Clusters = [lone, mini] };

        // Act
        var loneDto = await GetJsonAsync(client, "/api/clusters/lone");
        var miniDto = await GetJsonAsync(client, "/api/clusters/mini");

        // Assert: sharded=false ⟺ 1 бакет и ≤1 шард (arch/03 §2)
        loneDto.GetProperty("sharded").GetBoolean().Should().BeFalse();
        miniDto.GetProperty("sharded").GetBoolean().Should().BeTrue();
    }
}
