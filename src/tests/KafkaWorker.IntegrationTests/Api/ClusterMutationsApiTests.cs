using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using KafkaWorker.IntegrationTests.Etcd;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace KafkaWorker.IntegrationTests.Api;

// Кластерные мутации KafkaWorker API (task etcd-via-worker-api, arch/02
// §10.2-1..5/8/13/14): контракт 1:1 панельному KafkaOperationsModule, но пишет
// сам KafkaWorker. Claim-txn гонки — spec §6; requested_by — заголовок
// X-Requested-By (панель шлёт оператора), fallback "api" (значения etcd не
// меняются при переходе на прокси, spec §3.7).
[Collection(KafkaApiCollection.Name)]
public class ClusterMutationsApiTests(KafkaApiFixture fixture)
{
    private HttpClient Client => fixture.Factory.CreateClient();

    private EtcdFixture Etcd => fixture.Etcd;

    // AAA: POST декларации пишет канонический набор ключей (arch/02 §10.2-1):
    // config NOT_INITIALIZED + state/resources на каждого брокера.
    [Fact]
    public async Task PostCluster_WritesCanonicalKeySet()
    {
        // Arrange
        var client = Client;
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await client.PostAsJsonAsync("/api/kafka/clusters",
            new { name = "smoke" }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("name").GetString().Should().Be("smoke");
        body.GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");
        body.GetProperty("brokers").GetInt32().Should().Be(3);
        resp.Headers.Location.Should().BeNull(); // Location строит панель, не воркер

        var config = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/smoke/config", ct);
        config.Value!.Value.Should().Contain("\"state\":\"NOT_INITIALIZED\"");
        for (var k = 1; k <= 3; k++)
        {
            var state = await Etcd.Gateway.GetAsync(Etcd.Endpoint,
                $"/kafka/clusters/smoke/brokers/broker{k}/state", ct);
            state.Value!.Value.Should().Be("NOT_INITIALIZED");
            var resources = await Etcd.Gateway.GetAsync(Etcd.Endpoint,
                $"/kafka/clusters/smoke/brokers/broker{k}/resources", ct);
            resources.Value!.Value.Should().Contain("\"cpu\":\"2\"").And.Contain("\"disk\":\"20Gi\"");
        }
    }

    // AAA: повторный POST того же имени — 409 (claim-txn не сошёлся).
    [Fact]
    public async Task PostCluster_SecondPost_SameName_409()
    {
        // Arrange — первый POST занимает имя
        await Client.PostAsJsonAsync("/api/kafka/clusters", new { name = "dup" },
            TestContext.Current.CancellationToken);

        // Act
        var resp = await Client.PostAsJsonAsync("/api/kafka/clusters", new { name = "dup" },
            TestContext.Current.CancellationToken);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Cluster already exists");
    }

    // AAA: гонка claim-txn (spec §6) — два параллельных POST одного имени:
    // ровно один 201 и один 409, в etcd один config.
    [Fact]
    public async Task PostCluster_ConcurrentPosts_ExactlyOneWins()
    {
        // Arrange
        var payload = new { name = "race" };
        var ct = TestContext.Current.CancellationToken;

        // Act
        var responses = await Task.WhenAll(
            Client.PostAsJsonAsync("/api/kafka/clusters", payload, ct),
            Client.PostAsJsonAsync("/api/kafka/clusters", payload, ct));

        // Assert
        responses.Count(r => r.StatusCode == HttpStatusCode.Created).Should().Be(1);
        responses.Count(r => r.StatusCode == HttpStatusCode.Conflict).Should().Be(1);

        var configs = await Etcd.Gateway.RangeAsync(Etcd.Endpoint, "/kafka/clusters/race/config", ct);
        configs.Value.Should().ContainSingle();
    }

    // AAA: невалидное тело (RF > brokers) — 400 ProblemDetails с errors-массивом.
    [Fact]
    public async Task PostCluster_InvalidBody_400WithErrorsArray()
    {
        // Arrange / Act
        var resp = await Client.PostAsJsonAsync("/api/kafka/clusters",
            new { name = "bad", brokers = 1, replicationFactor = 3 },
            TestContext.Current.CancellationToken);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Validation failed");
        problem.GetProperty("errors").GetProperty("replicationFactor").GetArrayLength().Should().BeGreaterThan(0);
    }

    // AAA: DELETE кластера — 204 + config.state=TO_REMOVE с сохранением полей;
    // повторный DELETE — идемпотентный 204 без изменения значения.
    [Fact]
    public async Task DeleteCluster_204AndIdempotent()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await Client.PostAsJsonAsync("/api/kafka/clusters", new { name = "gone" }, ct);

        // Act
        var resp = await Client.DeleteAsync("/api/kafka/clusters/gone", ct);
        var repeat = await Client.DeleteAsync("/api/kafka/clusters/gone", ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NoContent);
        repeat.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var config = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/gone/config", ct);
        config.Value!.Value.Should().Contain("\"state\":\"TO_REMOVE\"")
            .And.Contain("\"brokers\":3").And.Contain("\"created_unix\":");
    }

    // AAA: DELETE несуществующего — 404.
    [Fact]
    public async Task DeleteCluster_NotFound_404()
    {
        // Arrange / Act
        var resp = await Client.DeleteAsync("/api/kafka/clusters/nosuch",
            TestContext.Current.CancellationToken);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Cluster not found");
    }

    // AAA: PUT config — 200 с DTO обновлённых полей; значение в etcd изменилось
    // (чек 50 шаг 7: панель отвечает 200, а не 204 — код 1:1 панельному модулю).
    [Fact]
    public async Task PutConfig_200AndValueInEtcdChanged()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events");

        // Act
        var resp = await Client.PutAsJsonAsync("/api/kafka/clusters/events/config",
            new { defaultRetentionMs = 86400000 }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("defaultRetentionMs").GetInt64().Should().Be(86400000);
        body.GetProperty("replicationFactor").GetInt32().Should().Be(3); // не тронуто
        var config = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/events/config", ct);
        config.Value!.Value.Should().Contain("\"default_retention_ms\":86400000")
            .And.Contain("\"created_unix\":").And.NotContain("\"state\"");
    }

    // AAA: PUT config пустым телом (ни одного поля) — 400.
    [Fact]
    public async Task PutConfig_EmptyUpdate_400()
    {
        // Arrange
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events2");

        // Act
        var resp = await Client.PutAsJsonAsync("/api/kafka/clusters/events2/config",
            new { }, TestContext.Current.CancellationToken);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Validation failed");
    }

    // AAA: POST brokers на 3-брочном кластере — 201 broker4 (имя генерит сервер,
    // чек 50 шаг 8), state NOT_INITIALIZED + resources в etcd.
    [Fact]
    public async Task PostBroker_201GeneratesBroker4()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events");

        // Act
        var resp = await Client.PostAsJsonAsync("/api/kafka/clusters/events/brokers",
            new { cpu = 1, memGi = 2, diskGi = 20 }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("name").GetString().Should().Be("broker4");
        body.GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");
        var state = await Etcd.Gateway.GetAsync(Etcd.Endpoint,
            "/kafka/clusters/events/brokers/broker4/state", ct);
        state.Value!.Value.Should().Be("NOT_INITIALIZED");
        var resources = await Etcd.Gateway.GetAsync(Etcd.Endpoint,
            "/kafka/clusters/events/brokers/broker4/resources", ct);
        resources.Value!.Value.Should().Contain("\"cpu\":\"1\"");
    }

    // AAA: DELETE брокера-broker (не controller) — 204 + state=TO_REMOVE;
    // DELETE controller — 409 (роль фиксируется навсегда; чек 50 шаги 9–10).
    [Fact]
    public async Task DeleteBroker_BrokerOnly_204_Controller_409()
    {
        // Arrange: 4 брокера, broker4 — broker-only, broker1 — controller.
        var ct = TestContext.Current.CancellationToken;
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events", brokers: 4);

        // Act
        var removed = await Client.DeleteAsync("/api/kafka/clusters/events/brokers/broker4", ct);
        var controller = await Client.DeleteAsync("/api/kafka/clusters/events/brokers/broker1", ct);

        // Assert
        removed.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var state = await Etcd.Gateway.GetAsync(Etcd.Endpoint,
            "/kafka/clusters/events/brokers/broker4/state", ct);
        state.Value!.Value.Should().Be("TO_REMOVE");
        controller.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await controller.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Broker remove rejected");
    }

    // AAA: DELETE несуществующего брокера — 404.
    [Fact]
    public async Task DeleteBroker_Unknown_404()
    {
        // Arrange
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events");

        // Act
        var resp = await Client.DeleteAsync("/api/kafka/clusters/events/brokers/broker9",
            TestContext.Current.CancellationToken);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // AAA: POST rotate при живой заявке сида — 409, заявка не перезаписана
    // (чек 50 шаг 11). Идентичность оператора (spec §3.7): rotate с заголовком
    // X-Requested-By на чистом кластере пишет "requested_by":"opsuser", без
    // заголовка — fallback "api".
    [Fact]
    public async Task Rotate_LiveTicket_409_AndRequestedByFromHeader()
    {
        // Arrange: events — живая заявка ротации, rotme — чистый кластер.
        var ct = TestContext.Current.CancellationToken;
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events");
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/kafkaworker/rotations/events",
            """{"requested_unix":1750000200,"requested_by":"ops"}""", null, ct);
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "rotme");

        // Act
        var rejected = await Client.PostAsync("/api/kafka/clusters/events/app-password/rotate", null, ct);
        using var withBy = new HttpRequestMessage(HttpMethod.Post,
            "/api/kafka/clusters/rotme/app-password/rotate");
        withBy.Headers.Add("X-Requested-By", "opsuser");
        var accepted = await Client.SendAsync(withBy, ct);
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "rotme2");
        var noBy = await Client.PostAsync("/api/kafka/clusters/rotme2/app-password/rotate", null, ct);

        // Assert
        rejected.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafkaworker/rotations/events", ct);
        ticket.Value!.Value.Should().Contain("ops"); // не перезаписана
        accepted.StatusCode.Should().Be(HttpStatusCode.Created);
        var acceptedBody = await accepted.Content.ReadFromJsonAsync<JsonElement>(ct);
        acceptedBody.GetProperty("requestedBy").GetString().Should().Be("opsuser");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafkaworker/rotations/rotme", ct))
            .Value!.Value.Should().Contain("\"requested_by\":\"opsuser\"");
        noBy.StatusCode.Should().Be(HttpStatusCode.Created);
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafkaworker/rotations/rotme2", ct))
            .Value!.Value.Should().Contain("\"requested_by\":\"api\"");
    }

    // AAA: rebalance при живой заявке — 409; DELETE — 204; повторный DELETE — 404;
    // POST несуществующему кластеру — 404 (чек 50 шаги 12–15). requested_by
    // заявки — заголовок X-Requested-By, fallback "api".
    [Fact]
    public async Task Rebalance_Live409_Delete204_Repeat404_NoCluster404()
    {
        // Arrange: events — живая заявка ребалансировки, reb — чистый.
        var ct = TestContext.Current.CancellationToken;
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events");
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/kafkaworker/rebalances/events",
            """{"requested_unix":1756505100,"requested_by":"ops"}""", null, ct);
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "reb");

        // Act
        var rejected = await Client.PostAsync("/api/kafka/clusters/events/rebalance", null, ct);
        using var withBy = new HttpRequestMessage(HttpMethod.Post, "/api/kafka/clusters/reb/rebalance");
        withBy.Headers.Add("X-Requested-By", "opsuser");
        var accepted = await Client.SendAsync(withBy, ct);
        var acceptedTicket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafkaworker/rebalances/reb", ct);
        var cancelled = await Client.DeleteAsync("/api/kafka/clusters/reb/rebalance", ct);
        var repeatCancel = await Client.DeleteAsync("/api/kafka/clusters/reb/rebalance", ct);
        var noCluster = await Client.PostAsync("/api/kafka/clusters/nope/rebalance", null, ct);

        // Assert
        rejected.StatusCode.Should().Be(HttpStatusCode.Conflict);
        accepted.StatusCode.Should().Be(HttpStatusCode.Created);
        acceptedTicket.Value!.Value.Should().Contain("\"requested_by\":\"opsuser\"");
        cancelled.StatusCode.Should().Be(HttpStatusCode.NoContent);
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafkaworker/rebalances/reb", ct))
            .Value.Should().BeNull(); // заявка снята
        repeatCancel.StatusCode.Should().Be(HttpStatusCode.NotFound);
        noCluster.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // AAA: с заданным ApiKey все /api/* требуют X-Api-Key: без заголовка — 401,
    // с корректным — мутация проходит (201).
    [Fact]
    public async Task ApiKey_WithoutHeader_401_WithHeader_201()
    {
        // Arrange — отдельный хост с KafkaWorker:Api:ApiKey=test
        using var factory = fixture.Factory.WithWebHostBuilder(b =>
            b.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(
                new Dictionary<string, string?> { ["KafkaWorker:Api:ApiKey"] = "test" })));
        var client = factory.CreateClient();
        var payload = new { name = "authdemo" };
        var ct = TestContext.Current.CancellationToken;

        // Act
        var rejected = await client.PostAsJsonAsync("/api/kafka/clusters", payload, ct);
        client.DefaultRequestHeaders.Add("X-Api-Key", "test");
        var accepted = await client.PostAsJsonAsync("/api/kafka/clusters", payload, ct);

        // Assert
        rejected.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        var problem = await rejected.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Unauthorized");
        accepted.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}
