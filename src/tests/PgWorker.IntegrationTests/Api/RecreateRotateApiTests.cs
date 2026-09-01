using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// POST /api/clusters/{c}/app-password/rotate + POST /api/ha/{scope}/nodes/{node}/recreate
// (task etcd-via-worker-api): порт панельных RotateAppPasswordApiTests/RecreateNodeApiTests.
[Collection(PgApiCollection.Name)]
public class RecreateRotateApiTests(PgApiFixture fixture)
{
    private HttpClient Client => fixture.Factory.CreateClient();

    private EtcdFixture Etcd => fixture.Etcd;

    // AAA: заявка ротации ставится txn-клэймом с requested_by из X-Requested-By.
    [Fact]
    public async Task Rotate_WithRequestedByHeader_TicketCarriesOperator()
    {
        // Arrange — один клиент несёт заголовок оператора
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rot", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        var client = Client;
        client.DefaultRequestHeaders.Add("X-Requested-By", "opsuser");

        // Act
        var resp = await client.PostAsync("/api/clusters/rot/app-password/rotate", null, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("cluster").GetString().Should().Be("rot");
        body.GetProperty("requestedBy").GetString().Should().Be("opsuser");
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/rotations/rot", ct);
        ticket.Value!.Value.Should().Contain("\"requested_by\":\"opsuser\"")
            .And.Contain("\"requested_unix\":");
    }

    // AAA: без заголовка заявка получает requested_by="api" (fallback воркера).
    [Fact]
    public async Task Rotate_WithoutHeader_TicketRequestedByApi()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rotnh", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsync("/api/clusters/rotnh/app-password/rotate", null, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/rotations/rotnh", ct);
        ticket.Value!.Value.Should().Contain("\"requested_by\":\"api\"");
    }

    // AAA: живая заявка уже стоит — повторный POST 409 (перезапись запрещена).
    [Fact]
    public async Task Rotate_AlreadyRequested_409()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rot2", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/pgworker/rotations/rot2",
            """{"requested_unix":100,"requested_by":"seed"}""", null, ct);

        // Act
        var resp = await Client.PostAsync("/api/clusters/rot2/app-password/rotate", null, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Rotation rejected");
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/rotations/rot2", ct);
        ticket.Value!.Value.Should().Contain("\"requested_by\":\"seed\""); // не перезаписана
    }

    // AAA: recreate ноды — маркеры state=TO_RECREATE + recreate=hard (тело).
    [Fact]
    public async Task Recreate_Node_PutsMarkers_201()
    {
        // Arrange — скоп rc-s1: две ноды в декларации (replicas=2) + /service/<scope>/members
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rc", buckets: 4, shards: 2, replicas: 2);
        var ct = TestContext.Current.CancellationToken;
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/service/rc-shard1/members/shard1a", "{}", null, ct);

        // Act
        var resp = await Client.PostAsJsonAsync("/api/ha/rc-shard1/nodes/shard1a/recreate",
            new { mode = "hard" }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("state").GetString().Should().Be("TO_RECREATE");
        body.GetProperty("mode").GetString().Should().Be("hard");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/rc/shards/shard1/nodes/shard1a/state", ct))
            .Value!.Value.Should().Be("TO_RECREATE");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/rc/shards/shard1/nodes/shard1a/recreate", ct))
            .Value!.Value.Should().Be("hard");
    }

    // AAA: ноды нет в декларации шарда — 404.
    [Fact]
    public async Task Recreate_UnknownNode_404()
    {
        // Arrange — скоп жив (members), но ноды shard1z в декларации нет
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rc404", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/service/rc404-shard1/members/shard1a", "{}", null, ct);

        // Act
        var resp = await Client.PostAsync("/api/ha/rc404-shard1/nodes/shard1z/recreate", null, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Not found");
    }

    // AAA: последняя нода скопа — пересоздание невозможно (409).
    [Fact]
    public async Task Recreate_LastNode_409()
    {
        // Arrange — шард с одной нодой (replicas=1)
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "solo", buckets: 2, shards: 2, replicas: 1);
        var ct = TestContext.Current.CancellationToken;
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/service/solo-shard1/members/shard1a", "{}", null, ct);

        // Act
        var resp = await Client.PostAsync("/api/ha/solo-shard1/nodes/shard1a/recreate", null, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Recreate rejected");
        problem.GetProperty("detail").GetString().Should().Contain("последняя");
    }
}
