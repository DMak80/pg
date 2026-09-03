using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// POST /api/clusters/{c}/moves/rollback|finalize|abort + DELETE .../moves/{bucket}
// (t07, arch/02 §9.7.2–§9.7.5): постановка заявок op≠move и отмена стоящих.
[Collection(PgApiCollection.Name)]
public class MoveOpsApiTests(PgApiFixture fixture)
{
    private HttpClient Client => fixture.Factory.CreateClient();

    private EtcdFixture Etcd => fixture.Etcd;

    // ===== rollback (§9.7.2) =====

    // AAA: rollback-заявки ставятся по одному ключу на бакет (op=rollback,
    // requested_by из X-Requested-By, requested_unix в конец очереди).
    [Fact]
    public async Task Rollback_QueuesTickets_WithOperatorAndOrder()
    {
        // Arrange — 4×2, бакеты 0,1 на shard1; в очереди чужая заявка unix=100
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rb", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await ApiTestSeed.SeedTicketAsync(Etcd, "rb", 3, "move", to: "shard2", unix: 100);
        var client = Client;
        client.DefaultRequestHeaders.Add("X-Requested-By", "opsuser");

        // Act
        var resp = await client.PostAsJsonAsync("/api/clusters/rb/moves/rollback",
            new { buckets = new[] { 1, 0 } }, ct);

        // Assert — 201: queued по возрастанию id; ключ op=rollback без to;
        // requested_by из заголовка; requested_unix > сида (в конец очереди).
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("queued").EnumerateArray().Select(v => v.GetInt32()).Should().Equal(0, 1);
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/moves/rb/bucket_0", ct);
        ticket.Value!.Value.Should().Contain("\"op\":\"rollback\"")
            .And.Contain("\"requested_by\":\"opsuser\"")
            .And.NotContain("\"to\"");
        using var doc = JsonDocument.Parse(ticket.Value.Value);
        doc.RootElement.GetProperty("requested_unix").GetInt64().Should().BeGreaterThan(100);
    }

    // AAA: повтор идентичной rollback-заявки → skipped (без перезаписи, Д6).
    [Fact]
    public async Task Rollback_Repeat_AllSkipped()
    {
        // Arrange — живая op=rollback заявка на bucket_0
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rbrpt", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await ApiTestSeed.SeedTicketAsync(Etcd, "rbrpt", 0, "rollback");

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rbrpt/moves/rollback",
            new { buckets = new[] { 0 } }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("skipped").EnumerateArray().Select(v => v.GetInt32()).Should().Equal(0);
        body.GetProperty("queued").GetArrayLength().Should().Be(0);
    }

    // AAA: живая иная заявка на бакете → 409 (панель не перезаписывает чужие).
    [Fact]
    public async Task Rollback_ConflictingMoveTicket_409()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rbcf", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await ApiTestSeed.SeedTicketAsync(Etcd, "rbcf", 0, "move", to: "shard2");

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rbcf/moves/rollback",
            new { buckets = new[] { 0 } }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Move ops rejected");
    }

    // AAA: не-ACTIVE бакет (SYNCING) → 409 «возможен только из ACTIVE».
    [Fact]
    public async Task Rollback_SyncingBucket_409()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rbsync", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await ApiTestSeed.SeedBucketStatusAsync(Etcd, "rbsync", 0, "SYNCING", "shard1", "shard2",
            DateTimeOffset.UtcNow.AddSeconds(-300).ToUnixTimeSeconds());

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rbsync/moves/rollback",
            new { buckets = new[] { 0 } }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("detail").GetString().Should().Contain("только из ACTIVE");
    }

    // AAA: пустой массив → 400; нешардированный кластер → 409.
    [Fact]
    public async Task Rollback_EmptyBuckets_400()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rb400", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rb400/moves/rollback",
            new { buckets = Array.Empty<int>() }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Rollback_NonSharded_409()
    {
        // Arrange — вырожденный 1×1
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rb1x1", buckets: 1, shards: 1);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rb1x1/moves/rollback",
            new { buckets = new[] { 0 } }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}
