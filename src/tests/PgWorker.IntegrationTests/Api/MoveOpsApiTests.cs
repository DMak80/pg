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

    // ===== finalize (§9.7.3) =====

    // AAA: finalize-заявка ставится с old_shard; ключ каноничен.
    [Fact]
    public async Task Finalize_QueuesTicket_WithOldShard()
    {
        // Arrange — 4×2, bucket_0 на shard1; убираем артефакты на shard2
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "fin", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/fin/moves/finalize",
            new { bucket = 0, oldShard = "shard2" }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("oldShard").GetString().Should().Be("shard2");
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/moves/fin/bucket_0", ct);
        ticket.Value!.Value.Should().Contain("\"op\":\"finalize\"")
            .And.Contain("\"old_shard\":\"shard2\"");
    }

    // AAA: oldShard = текущему владельцу → 409 «убирать нечего».
    [Fact]
    public async Task Finalize_OldShardIsOwner_409()
    {
        // Arrange — bucket_0 на shard1
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "finown", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/finown/moves/finalize",
            new { bucket = 0, oldShard = "shard1" }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("detail").GetString().Should().Contain("убирать нечего");
    }

    // AAA: oldShard не существует → 404; TO_REMOVE-приёмник допустим → 201.
    [Fact]
    public async Task Finalize_UnknownShard_404()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "fin404", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/fin404/moves/finalize",
            new { bucket = 0, oldShard = "shard9" }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Finalize_OldShardToRemove_201()
    {
        // Arrange — shard2 в демонтаже: финализация перед удалением допустима
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "finrm", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/clusters/finrm/shards/shard2/state",
            "TO_REMOVE", null, ct);

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/finrm/moves/finalize",
            new { bucket = 0, oldShard = "shard2" }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    // ===== abort (§9.7.4) =====

    // AAA: abort ставит заявку с force:true только при force; иначе force в JSON нет.
    [Fact]
    public async Task Abort_QueuesTicket_ForceOnlyWhenTrue()
    {
        // Arrange — зависший SYNCING-статус (несвежий: updated_unix = now-300)
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "ab", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await ApiTestSeed.SeedBucketStatusAsync(Etcd, "ab", 0, "SYNCING", "shard1", "shard2",
            DateTimeOffset.UtcNow.AddSeconds(-300).ToUnixTimeSeconds());

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/ab/moves/abort",
            new { bucket = 0 }, ct);

        // Assert — force не пишется (null-поле опускается, канон §4.2)
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("force").GetBoolean().Should().BeFalse();
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/moves/ab/bucket_0", ct);
        ticket.Value!.Value.Should().Contain("\"op\":\"abort\"").And.NotContain("\"force\"");
    }

    // AAA: свежий статус без force → 409 (текст AbortMinAgeSec); с force → 201.
    [Fact]
    public async Task Abort_FreshStatus_409ThenForce_201()
    {
        // Arrange — SYNCING, updated_unix = now (свежий)
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "abfr", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await ApiTestSeed.SeedBucketStatusAsync(Etcd, "abfr", 0, "SYNCING", "shard1", "shard2",
            DateTimeOffset.UtcNow.ToUnixTimeSeconds());

        // Act
        var noForce = await Client.PostAsJsonAsync("/api/clusters/abfr/moves/abort",
            new { bucket = 0 }, ct);

        // Assert — 409, текст — порт процесса
        noForce.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await noForce.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("detail").GetString().Should().Contain("AbortMinAgeSec").And.Contain("force");

        // Act — с force
        var forced = await Client.PostAsJsonAsync("/api/clusters/abfr/moves/abort",
            new { bucket = 0, force = true }, ct);

        // Assert — 201, в ключе force:true
        forced.StatusCode.Should().Be(HttpStatusCode.Created);
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/moves/abfr/bucket_0", ct);
        ticket.Value!.Value.Should().Contain("\"force\":true");
    }

    // AAA: routing==target без force → 409 «осознанно: force»; с force → 201.
    [Fact]
    public async Task Abort_RoutingEqualsTarget_409ThenForce_201()
    {
        // Arrange — SYNCING, владелец shard1 == target (flip прошёл, статус завис)
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "abfl", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await ApiTestSeed.SeedBucketStatusAsync(Etcd, "abfl", 0, "SYNCING", "shard1", "shard1",
            DateTimeOffset.UtcNow.AddSeconds(-300).ToUnixTimeSeconds());

        // Act
        var noForce = await Client.PostAsJsonAsync("/api/clusters/abfl/moves/abort",
            new { bucket = 0 }, ct);

        // Assert
        noForce.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await noForce.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("detail").GetString().Should().Contain("осознанно");

        // Act / Assert — с force
        var forced = await Client.PostAsJsonAsync("/api/clusters/abfl/moves/abort",
            new { bucket = 0, force = true }, ct);
        forced.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    // AAA: ACTIVE-бакет (нет статуса) → 409 «пост-flip артефакты убирает finalize»;
    // NOT_INITIALIZED → 409 «не переезд».
    [Fact]
    public async Task Abort_ActiveBucket_409FinalizeHint()
    {
        // Arrange — bucket_0 без статус-ключа = ACTIVE
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "abact", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/abact/moves/abort",
            new { bucket = 0 }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("detail").GetString().Should().Contain("finalize");
    }

    [Fact]
    public async Task Abort_NotInitializedBucket_409NotAMove()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "abni", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        // target для NOT_INITIALIZED не важен — ветка «не переезд» раньше проверок target
        await ApiTestSeed.SeedBucketStatusAsync(Etcd, "abni", 0, "NOT_INITIALIZED", "shard1", "",
            DateTimeOffset.UtcNow.AddSeconds(-300).ToUnixTimeSeconds());

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/abni/moves/abort",
            new { bucket = 0 }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("detail").GetString().Should().Contain("не переезд");
    }
}
