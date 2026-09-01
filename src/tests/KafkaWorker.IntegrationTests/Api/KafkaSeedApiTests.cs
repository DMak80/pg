using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace KafkaWorker.IntegrationTests.Api;

// POST /api/seed/demo (task etcd-via-worker-api): демо-сид kafka-домена через
// API воркера — перенос dev-stand/adminpanel/kafka-seed.sh 1:1; идемпотентен
// по живому /kafka/clusters/events/config; за флагом EnableSeedEndpoint
// (выключен → 404). Проверки — перечень самопроверки скрипта.
[Collection(KafkaApiCollection.Name)]
public class KafkaSeedApiTests(KafkaApiFixture fixture)
{
    private HttpClient Client => fixture.Factory.CreateClient();

    private Etcd.EtcdFixture Etcd => fixture.Etcd;

    // Фабрика с ВЫКЛЮЧЕННЫМ seed-эндпоинтом: оверрайд поверх KafkaApiFactory
    // (последний источник конфигурации выигрывает).
    private sealed class SeedDisabledFactory(Etcd.EtcdFixture etcd) : KafkaApiFactory(etcd)
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(
                new Dictionary<string, string?> { ["KafkaWorker:Api:EnableSeedEndpoint"] = "false" }));
        }
    }

    // AAA: наливка на «пустом» (относительно сида) etcd — 200 {"seeded":true}
    // и канонический набор ключей kafka-seed.sh: events config/брокеры/endpoints/
    // app-креды, topics-архетипы (desired/missing), lifecycle-заявки audit/orders,
    // живые ротация (now)/ребалансировка/reassignment, pending NOT_INITIALIZED.
    [Fact]
    public async Task SeedDemo_EmptyEtcd_SeedsCanonicalKeySet()
    {
        // Arrange — гарантируем отсутствие kafka-деклараций (порядок кейсов
        // в коллекции не гарантирован; чистим все префиксы сида)
        var ct = TestContext.Current.CancellationToken;
        foreach (var prefix in new[] { "/kafka/clusters/events/", "/kafka/clusters/pending/",
            "/kafkaworker/rotations/", "/kafkaworker/rebalances/", "/kafkaworker/reassignments/" })
            await Etcd.Gateway.DeleteAsync(Etcd.Endpoint, prefix, prefix: true, ct);

        // Act
        var resp = await Client.PostAsync("/api/seed/demo", null, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("seeded").GetBoolean().Should().BeTrue();
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/events/config", ct))
            .Value!.Value.Should().Contain("\"brokers\":3").And.NotContain("\"state\"");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/events/brokers/broker2/state", ct))
            .Value!.Value.Should().Be("RUNNING");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/events/brokers/broker2/role", ct))
            .Value!.Value.Should().Be("controller");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/events/endpoints", ct))
            .Value!.Value.Should().Be("host.docker.internal:16001,host.docker.internal:16002,host.docker.internal:16003");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/events/app_password", ct))
            .Value!.Value.Should().Be("SeEdPaSsWoRd0123456789AbCdEf");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/events/topics/payments", ct))
            .Value!.Value.Should().Contain("\"desired\":").And.Contain("\"desired_by\":\"ops\"");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/events/topics/ghost", ct))
            .Value!.Value.Should().Contain("\"missing\":true");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint,
            "/kafka/clusters/events/topics/audit/desired.create", ct))
            .Value!.Value.Should().Contain("\"partitions\":12").And.Contain("\"requested_by\":\"seed\"");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint,
            "/kafka/clusters/events/topics/orders/desired.delete", ct))
            .Value!.Value.Should().Contain("\"requested_by\":\"seed\"");
        var rotation = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafkaworker/rotations/events", ct);
        rotation.Value!.Value.Should().Contain("\"requested_by\":\"seed\"")
            .And.Contain("\"requested_unix\":"); // now — единственное динамическое
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafkaworker/rebalances/events", ct))
            .Value!.Value.Should().Contain("\"requested_unix\":1756500123");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafkaworker/reassignments/events", ct))
            .Value!.Value.Should().Contain("\"drain_broker\":\"broker2\"")
            .And.Contain("\"partitions_remaining\":3");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/pending/config", ct))
            .Value!.Value.Should().Contain("\"state\":\"NOT_INITIALIZED\"");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/pending/brokers/broker3/resources", ct))
            .Value!.Value.Should().Contain("\"mem\":\"2Gi\"").And.Contain("\"disk\":\"20Gi\"");
    }

    // AAA: повторный вызов при живом /kafka/clusters/events/config — 200
    // {"seeded":false}, значения НЕ перезаписаны (идемпотентность образца
    // kafka-seed.sh: «не портим»).
    [Fact]
    public async Task SeedDemo_AlreadySeeded_NoOp()
    {
        // Arrange — наливаем (если ещё не налито предыдущим кейсом) и фиксируем значения
        var ct = TestContext.Current.CancellationToken;
        await Client.PostAsync("/api/seed/demo", null, ct);
        var config = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/events/config", ct);
        var rotation = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafkaworker/rotations/events", ct);

        // Act
        var resp = await Client.PostAsync("/api/seed/demo", null, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("seeded").GetBoolean().Should().BeFalse();
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafka/clusters/events/config", ct))
            .Value!.Value.Should().Be(config.Value!.Value);
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/kafkaworker/rotations/events", ct))
            .Value!.Value.Should().Be(rotation.Value!.Value);
    }

    // AAA: EnableSeedEndpoint=false — 404 ProblemDetails (флаг проверяется до
    // идемпотентности/записей — в etcd ничего не меняется).
    [Fact]
    public async Task SeedDemo_DisabledFlag_404()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        using var factory = new SeedDisabledFactory(Etcd);
        var client = factory.CreateClient();

        // Act
        var resp = await client.PostAsync("/api/seed/demo", null, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Not found");
    }
}
