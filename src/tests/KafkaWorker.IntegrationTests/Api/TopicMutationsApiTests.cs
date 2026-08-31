using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Xunit;

namespace KafkaWorker.IntegrationTests.Api;

// Топиковые мутации KafkaWorker API (task etcd-via-worker-api, arch/02
// §10.2-6,7,9,10,11,12): контракт 1:1 панельному KafkaOperationsModule,
// пишет сам KafkaWorker (guards по прямым чтениям etcd). Негативы — порт
// чека dev-stand/adminpanel/checks/50-kafka-api.sh шагов 4–5 и панельных
// TopicDesired/TopicLifecycleCommandTests. desired_by/requested_by — заголовок
// X-Requested-By (панель шлёт оператора), fallback "api" (значения etcd не
// меняются при переходе на прокси, spec §3.7).
[Collection(KafkaApiCollection.Name)]
public class TopicMutationsApiTests(KafkaApiFixture fixture)
{
    private HttpClient Client => fixture.Factory.CreateClient();

    private Etcd.EtcdFixture Etcd => fixture.Etcd;

    // AAA: PUT desired — 200 с DTO (панель отвечает Ok, чек 50); в etcd ключ
    // topics/<t> получает desired + desired_unix/desired_by (RMW, факт не тронут).
    [Fact]
    public async Task PutDesired_200_WritesDesiredFields()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events");
        await KafkaApiTestSeed.SeedTopicKeyAsync(Etcd, "events", "orders", partitions: 12);
        using var req = new HttpRequestMessage(HttpMethod.Put, "/api/kafka/clusters/events/topics/orders")
        {
            Content = JsonContent.Create(new { retentionMs = 86400000L }),
        };
        req.Headers.Add("X-Requested-By", "opsuser");

        // Act
        var resp = await Client.SendAsync(req, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("cluster").GetString().Should().Be("events");
        body.GetProperty("topic").GetString().Should().Be("orders");
        body.GetProperty("retentionMs").GetInt64().Should().Be(86400000L);
        var key = await Etcd.Gateway.GetAsync(Etcd.Endpoint,
            "/kafka/clusters/events/topics/orders", ct);
        key.Value!.Value.Should().Contain("\"partitions\":12") // факт не тронут
            .And.Contain("\"desired\":").And.Contain("\"retention.ms\":\"86400000\"")
            .And.Contain("\"desired_unix\":").And.Contain("\"desired_by\":\"opsuser\"");
    }

    // AAA: PUT desired уменьшением partitions — 400 (Kafka не умеет уменьшение,
    // arch/02 §10.2-7); desired в etcd НЕ появился.
    [Fact]
    public async Task PutDesired_PartitionsDecrease_400_NoDesiredWritten()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events");
        await KafkaApiTestSeed.SeedTopicKeyAsync(Etcd, "events", "orders", partitions: 12);

        // Act
        var resp = await Client.PutAsJsonAsync("/api/kafka/clusters/events/topics/orders",
            new { partitions = 6 }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Validation failed");
        problem.GetProperty("errors").GetProperty("partitions").GetArrayLength().Should().BeGreaterThan(0);
        var key = await Etcd.Gateway.GetAsync(Etcd.Endpoint,
            "/kafka/clusters/events/topics/orders", ct);
        key.Value!.Value.Should().NotContain("\"desired\":");
    }

    // AAA: DELETE desired — 204 и desired-поля сняты RMW (факт сохранён);
    // без заявки — 404 (панельные кейсы CancelTopicDesired).
    [Fact]
    public async Task DeleteDesired_204_RemovesDesired_NoDesired_404()
    {
        // Arrange: orders — живая desired-заявка, payments — без.
        var ct = TestContext.Current.CancellationToken;
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events");
        await KafkaApiTestSeed.SeedTopicKeyAsync(Etcd, "events", "orders", partitions: 12,
            "\"desired\":{\"partitions\":16},\"desired_unix\":1756501000,\"desired_by\":\"seed\"");
        await KafkaApiTestSeed.SeedTopicKeyAsync(Etcd, "events", "payments", partitions: 12);

        // Act
        var cancelled = await Client.DeleteAsync("/api/kafka/clusters/events/topics/orders/desired", ct);
        var noDesired = await Client.DeleteAsync("/api/kafka/clusters/events/topics/payments/desired", ct);

        // Assert
        cancelled.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var key = await Etcd.Gateway.GetAsync(Etcd.Endpoint,
            "/kafka/clusters/events/topics/orders", ct);
        key.Value!.Value.Should().Contain("\"partitions\":12").And.NotContain("\"desired\":")
            .And.NotContain("\"desired_unix\":").And.NotContain("\"desired_by\":");
        noDesired.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // AAA: POST topics — 201 (панель строит Location; воркер — без) и ключ
    // desired.create с каноническим JSON (дефолты config кластера развёрнуты);
    // desired_by — заголовок X-Requested-By, fallback "api".
    [Fact]
    public async Task PostTopic_201_ClaimsCreateTicket()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events");

        // Act
        using var req = new HttpRequestMessage(HttpMethod.Post, "/api/kafka/clusters/events/topics")
        {
            Content = JsonContent.Create(new { name = "audit", retentionMs = 86400000L }),
        };
        req.Headers.Add("X-Requested-By", "opsuser");
        var resp = await Client.SendAsync(req, ct);
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events2");
        var noBy = await Client.PostAsJsonAsync("/api/kafka/clusters/events2/topics",
            new { name = "audit" }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        resp.Headers.Location.Should().BeNull(); // Location строит панель
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("topic").GetString().Should().Be("audit");
        body.GetProperty("partitions").GetInt32().Should().Be(12); // дефолт config
        body.GetProperty("replicationFactor").GetInt32().Should().Be(3);
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint,
            "/kafka/clusters/events/topics/audit/desired.create", ct);
        ticket.Value!.Value.Should().Contain("\"partitions\":12")
            .And.Contain("\"replication_factor\":3")
            .And.Contain("\"retention.ms\":\"86400000\"")
            .And.Contain("\"requested_unix\":").And.Contain("\"requested_by\":\"opsuser\"");
        noBy.StatusCode.Should().Be(HttpStatusCode.Created);
        var noByTicket = await Etcd.Gateway.GetAsync(Etcd.Endpoint,
            "/kafka/clusters/events2/topics/audit/desired.create", ct);
        noByTicket.Value!.Value.Should().Contain("\"requested_by\":\"api\"");
    }

    // AAA: повторный create при живой заявке desired.create — 409 (чек 50 шаг 4);
    // create существующего не-missing топика — 409; missing-топик с живым
    // desired — 409; RF выше brokers — 400 (чек 50 шаг 5). Missing-топик без
    // desired create разрешён (панельный Handle_MissingTopicAllowed → 201).
    [Fact]
    public async Task PostTopic_LiveTicket409_Exists409_MissingDesired409_Rf400()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events");
        await KafkaApiTestSeed.SeedLifecycleTicketAsync(Etcd, "events", "payments", "create");
        await KafkaApiTestSeed.SeedTopicKeyAsync(Etcd, "events", "orders", partitions: 12);
        await KafkaApiTestSeed.SeedTopicKeyAsync(Etcd, "events", "frozen", partitions: 12,
            "\"desired\":{\"partitions\":16}", missing: true);

        // Act
        var liveTicket = await Client.PostAsJsonAsync("/api/kafka/clusters/events/topics",
            new { name = "payments" }, ct);
        var exists = await Client.PostAsJsonAsync("/api/kafka/clusters/events/topics",
            new { name = "orders" }, ct);
        var missingDesired = await Client.PostAsJsonAsync("/api/kafka/clusters/events/topics",
            new { name = "frozen" }, ct);
        var badRf = await Client.PostAsJsonAsync("/api/kafka/clusters/events/topics",
            new { name = "x", replicationFactor = 10 }, ct);

        // Assert
        liveTicket.StatusCode.Should().Be(HttpStatusCode.Conflict);
        exists.StatusCode.Should().Be(HttpStatusCode.Conflict);
        missingDesired.StatusCode.Should().Be(HttpStatusCode.Conflict);
        badRf.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await badRf.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("errors").GetProperty("replicationFactor").GetArrayLength()
            .Should().BeGreaterThan(0);
    }

    // AAA: DELETE topic — 204 + идемпотентный повтор 204 (живая delete-заявка);
    // missing-топик — 404; живая desired — 409; живая create-заявка на НЕ-missing
    // топике — 409 (порядок панельных guard'ов: missing отсекается раньше) —
    // чек 50 шаги 4–6 + панельные кейсы.
    [Fact]
    public async Task DeleteTopic_204Idempotent_Missing404_DesiredPending409_CreatePending409()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events");
        await KafkaApiTestSeed.SeedTopicKeyAsync(Etcd, "events", "orders", partitions: 12);
        await KafkaApiTestSeed.SeedTopicKeyAsync(Etcd, "events", "ghost", partitions: 12,
            missing: true);
        await KafkaApiTestSeed.SeedTopicKeyAsync(Etcd, "events", "payments", partitions: 12,
            "\"desired\":{\"partitions\":16}");
        await KafkaApiTestSeed.SeedTopicKeyAsync(Etcd, "events", "audit", partitions: 12);
        await KafkaApiTestSeed.SeedLifecycleTicketAsync(Etcd, "events", "audit", "create");

        // Act
        var removed = await Client.DeleteAsync("/api/kafka/clusters/events/topics/orders", ct);
        var repeat = await Client.DeleteAsync("/api/kafka/clusters/events/topics/orders", ct);
        var missingTopic = await Client.DeleteAsync("/api/kafka/clusters/events/topics/ghost", ct);
        var desiredPending = await Client.DeleteAsync("/api/kafka/clusters/events/topics/payments", ct);
        var createPending = await Client.DeleteAsync("/api/kafka/clusters/events/topics/audit", ct);

        // Assert
        removed.StatusCode.Should().Be(HttpStatusCode.NoContent);
        repeat.StatusCode.Should().Be(HttpStatusCode.NoContent); // идемпотентность
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint,
            "/kafka/clusters/events/topics/orders/desired.delete", ct);
        ticket.Value!.Value.Should().Contain("\"requested_by\":\"api\""); // fallback без заголовка
        missingTopic.StatusCode.Should().Be(HttpStatusCode.NotFound);
        desiredPending.StatusCode.Should().Be(HttpStatusCode.Conflict);
        createPending.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    // AAA: cancel create/delete — 204 со снятием ключа; без заявки — 404
    // (чек 50 шаги 5–6 + панельные CancelTopicLifecycle).
    [Fact]
    public async Task CancelLifecycle_204RemovesTicket_NoTicket404()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await KafkaApiTestSeed.SeedActiveClusterAsync(Etcd, "events");
        await KafkaApiTestSeed.SeedLifecycleTicketAsync(Etcd, "events", "audit", "create");
        await KafkaApiTestSeed.SeedTopicKeyAsync(Etcd, "events", "orders", partitions: 12);

        // Act
        var cancelled = await Client.DeleteAsync(
            "/api/kafka/clusters/events/topics/audit/desired.create", ct);
        var repeat = await Client.DeleteAsync(
            "/api/kafka/clusters/events/topics/audit/desired.create", ct);
        var noDelete = await Client.DeleteAsync(
            "/api/kafka/clusters/events/topics/orders/desired.delete", ct);

        // Assert
        cancelled.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint,
            "/kafka/clusters/events/topics/audit/desired.create", ct);
        ticket.Value.Should().BeNull(); // ключ снят
        repeat.StatusCode.Should().Be(HttpStatusCode.NotFound);
        noDelete.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }
}
