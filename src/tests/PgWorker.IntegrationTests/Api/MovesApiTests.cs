using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// POST /api/clusters/{c}/moves (task etcd-via-worker-api): порт панельных
// MovesApiTests на WAF-хост воркера + идентичность оператора (spec §3.7).
[Collection(PgApiCollection.Name)]
public class MovesApiTests(PgApiFixture fixture)
{
    private HttpClient Client => fixture.Factory.CreateClient();

    private EtcdFixture Etcd => fixture.Etcd;

    // AAA: заявки переездов ставятся в очередь (txn-клэйм per key) с
    // requested_by из заголовка X-Requested-By (инвариант spec §3.7).
    [Fact]
    public async Task Moves_WithRequestedByHeader_TicketCarriesOperator()
    {
        // Arrange — 4×2: бакеты 0,1 на shard1; один клиент несёт заголовок оператора
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "mv", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        var client = Client;
        client.DefaultRequestHeaders.Add("X-Requested-By", "opsuser");

        // Act
        var resp = await client.PostAsJsonAsync("/api/clusters/mv/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0, 1 } }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("queued").EnumerateArray().Select(v => v.GetInt32()).Should().Equal(0, 1);
        body.GetProperty("skipped").GetArrayLength().Should().Be(0);
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/moves/mv/bucket_0", ct);
        ticket.Value!.Value.Should().Contain("\"op\":\"move\"")
            .And.Contain("\"to\":\"shard2\"")
            .And.Contain("\"requested_by\":\"opsuser\"");
    }

    // AAA: без заголовка заявка получает requested_by="api" (fallback воркера —
    // тот же источник, что ClaimsPrincipal у панели; значения etcd не меняются).
    [Fact]
    public async Task Moves_WithoutHeader_TicketRequestedByApi()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "nohdr", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/nohdr/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0 } }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/moves/nohdr/bucket_0", ct);
        ticket.Value!.Value.Should().Contain("\"requested_by\":\"api\"");
    }

    // AAA: идентичная живая заявка → skipped (без записи); иная живая → 409.
    [Fact]
    public async Task Moves_ExistingTicket_SkippedOrConflict()
    {
        // Arrange — 4×2: бакет 0 на shard1; живая заявка bucket_0 → shard2
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "cf", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await ApiTestSeed.SeedMoveTicketAsync(Etcd, "cf", 0, "shard2");

        // Act — идентичная заявка
        var same = await Client.PostAsJsonAsync("/api/clusters/cf/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0 } }, ct);

        // Assert — 201 со skipped
        same.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await same.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("skipped").EnumerateArray().Select(v => v.GetInt32()).Should().Equal(0);
        body.GetProperty("queued").GetArrayLength().Should().Be(0);

        // Act — иная заявка на тот же бакет
        var conflict = await Client.PostAsJsonAsync("/api/clusters/cf/moves",
            new { from = "shard1", to = "shard1", buckets = new[] { 0 } }, ct);

        // Assert — 400 (from==to ловит валидатор раньше), поэтому конфликт проверяем
        // через 3-шардовый кластер: заявка to=shard2, POST to=shard3
        conflict.StatusCode.Should().Be(HttpStatusCode.BadRequest); // from==to — 400
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "cf3", buckets: 4, shards: 3);
        await ApiTestSeed.SeedMoveTicketAsync(Etcd, "cf3", 0, "shard2");
        var clash = await Client.PostAsJsonAsync("/api/clusters/cf3/moves",
            new { from = "shard1", to = "shard3", buckets = new[] { 0 } }, ct);
        clash.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await clash.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Moves rejected");
    }

    // AAA: бакет не на источнике (routing у другого шарда) — 409.
    [Fact]
    public async Task Moves_BucketNotOnSource_409()
    {
        // Arrange — 4×2: бакет 0 на shard1
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "own", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act — заявка «с шарда 2», бакет лежит на shard1
        var resp = await Client.PostAsJsonAsync("/api/clusters/own/moves",
            new { from = "shard2", to = "shard1", buckets = new[] { 0 } }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("detail").GetString().Should().Contain("не доступен для переезда");
    }
}
