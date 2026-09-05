using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// POST /api/clusters + DELETE /api/clusters/{c} на WAF-хосте воркера (task
// etcd-via-worker-api): контракт 1:1 панельному CreateClusterApiTests, но
// пишет сам PgWorker. Claim-txn гонки — spec §6.
[Collection(PgApiCollection.Name)]
public class CreateClusterApiTests(PgApiFixture fixture)
{
    private HttpClient Client => fixture.Factory.CreateClient();

    private EtcdFixture Etcd => fixture.Etcd;

    // AAA: POST декларации пишет канонический набор ключей (arch/02 §9.1):
    // config NOT_INITIALIZED, nodes-декларации, request_*, routing блоками §9.1.1.
    [Fact]
    public async Task PostCluster_WritesCanonicalKeySet()
    {
        // Arrange
        var client = Client;

        // Act
        var resp = await client.PostAsJsonAsync("/api/clusters",
            new { name = "smoke", buckets = 4, shards = 2, replicas = 2,
                  requestCpu = 0.5, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");
        body.GetProperty("name").GetString().Should().Be("smoke");
        resp.Headers.Location.Should().BeNull(); // Location строит панель, не воркер

        var ct = TestContext.Current.CancellationToken;
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/smoke/config", ct))
            .Value!.Value.Should().Contain("\"state\":\"NOT_INITIALIZED\"");
        var routing = await Etcd.Gateway.RangeAsync(Etcd.Endpoint,
            "/clusters/smoke/buckets/routing/", ct);
        string.Join(" ", routing.Value.OrderBy(k => k.Key).Select(k => k.Value))
            .Should().Be("shard1 shard1 shard2 shard2"); // блоки 4×2 (§9.1.1)
    }

    // AAA: повторный POST того же имени — 409 (claim-txn не сошёлся).
    [Fact]
    public async Task PostCluster_SecondPost_SameName_409()
    {
        // Arrange — первый POST занимает имя
        await Client.PostAsJsonAsync("/api/clusters",
            new { name = "dup", buckets = 2, shards = 1, replicas = 1,
                  requestCpu = 1, requestMem = 1, requestDisk = 1 },
            TestContext.Current.CancellationToken);

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters",
            new { name = "dup", buckets = 2, shards = 1, replicas = 1,
                  requestCpu = 1, requestMem = 1, requestDisk = 1 },
            TestContext.Current.CancellationToken);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Cluster already exists");
    }

    // AAA: гонка claim-txn (spec §6) — два параллельных POST одного имени:
    // ровно один 201 и один 409, в etcd один набор ключей.
    [Fact]
    public async Task PostCluster_ConcurrentPosts_ExactlyOneWins()
    {
        // Arrange
        var payload = new { name = "race", buckets = 4, shards = 2, replicas = 1,
                            requestCpu = 1, requestMem = 1, requestDisk = 1 };
        var ct = TestContext.Current.CancellationToken;

        // Act
        var responses = await Task.WhenAll(
            Client.PostAsJsonAsync("/api/clusters", payload, ct),
            Client.PostAsJsonAsync("/api/clusters", payload, ct));

        // Assert
        responses.Select(r => r.StatusCode).Should().Contain(HttpStatusCode.Created);
        responses.Select(r => r.StatusCode).Should().Contain(HttpStatusCode.Conflict);
        responses.Count(r => r.StatusCode == HttpStatusCode.Created).Should().Be(1);
        responses.Count(r => r.StatusCode == HttpStatusCode.Conflict).Should().Be(1);

        var configs = await Etcd.Gateway.RangeAsync(Etcd.Endpoint, "/clusters/race/config", ct);
        configs.Value.Should().ContainSingle(); // один набор ключей
    }

    // AAA: buckets=0 — 400 ProblemDetails с errors-массивом по полю.
    [Fact]
    public async Task PostCluster_InvalidBuckets_400WithErrorsArray()
    {
        // Arrange / Act
        var resp = await Client.PostAsJsonAsync("/api/clusters",
            new { name = "bad", buckets = 0, shards = 1, replicas = 1,
                  requestCpu = 1, requestMem = 1, requestDisk = 1 },
            TestContext.Current.CancellationToken);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Validation failed");
        problem.GetProperty("errors").GetProperty("buckets").GetArrayLength().Should().BeGreaterThan(0);
    }

    // AAA: DELETE существующего кластера — 204 + config.state=TO_REMOVE;
    // повторный DELETE — идемпотентный 204.
    [Fact]
    public async Task DeleteCluster_204AndIdempotent()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await Client.PostAsJsonAsync("/api/clusters",
            new { name = "gone", buckets = 2, shards = 1, replicas = 1,
                  requestCpu = 1, requestMem = 1, requestDisk = 1 }, ct);

        // Act
        var resp = await Client.DeleteAsync("/api/clusters/gone", ct);
        var repeat = await Client.DeleteAsync("/api/clusters/gone", ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NoContent);
        repeat.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var config = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/gone/config", ct);
        config.Value!.Value.Should().Contain("\"state\":\"TO_REMOVE\"");
    }

    // AAA: DELETE несуществующего — 404.
    [Fact]
    public async Task DeleteCluster_NotFound_404()
    {
        // Arrange / Act
        var resp = await Client.DeleteAsync("/api/clusters/nosuch", TestContext.Current.CancellationToken);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Cluster not found");
    }
}
