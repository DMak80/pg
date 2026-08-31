using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// POST /api/seed/demo (task etcd-via-worker-api): демо-сид pg-контура через API
// воркера — перенос dev-stand/adminpanel/seed.sh 1:1; идемпотентен по живому
// /clusters/demo/config; за флагом EnableSeedEndpoint (выключен → 404).
[Collection(PgApiCollection.Name)]
public class SeedApiTests(PgApiFixture fixture)
{
    private HttpClient Client => fixture.Factory.CreateClient();

    private EtcdFixture Etcd => fixture.Etcd;

    // Фабрика с ВЫКЛЮЧЕННЫМ seed-эндпоинтом (дефолт ApiOptions): оверрайд
    // поверх PgWorkerApiFactory (последний источник конфигурации выигрывает).
    private sealed class SeedDisabledFactory(EtcdFixture etcd) : PgWorkerApiFactory(etcd)
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(
                new Dictionary<string, string?> { ["PgWorker:Api:EnableSeedEndpoint"] = "false" }));
        }
    }

    // AAA: наливка на «пустом» (относительно demo) etcd — 200 {"seeded":true}
    // и канонический набор ключей seed.sh: config, routing bucket_0→s1/bucket_15→s2,
    // живая заявка bucket_13, статус bucket_3 SYNCING, HA-скоп demo-s1.
    [Fact]
    public async Task SeedDemo_EmptyEtcd_SeedsCanonicalKeySet()
    {
        // Arrange — гарантируем отсутствие demo-деклараций (порядок кейсов
        // в коллекции не гарантирован; чистим все префиксы сида)
        var ct = TestContext.Current.CancellationToken;
        foreach (var prefix in new[] { "/clusters/demo/", "/pgworker/moves/demo/",
            "/service/demo-s1/", "/service/demo-s2/", "/cluster/nodes/" })
            await Etcd.Gateway.DeleteAsync(Etcd.Endpoint, prefix, prefix: true, ct);

        // Act
        var resp = await Client.PostAsync("/api/seed/demo", null, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("seeded").GetBoolean().Should().BeTrue();
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/demo/config", ct))
            .Value!.Value.Should().Contain("\"buckets\":16").And.Contain("\"dbname\":\"demo\"");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/demo/buckets/routing/bucket_0", ct))
            .Value!.Value.Should().Be("s1");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/demo/buckets/routing/bucket_15", ct))
            .Value!.Value.Should().Be("s2");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/moves/demo/bucket_13", ct))
            .Value!.Value.Should().Contain("\"op\":\"move\"").And.Contain("\"to\":\"s1\"");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/demo/buckets/status/bucket_3", ct))
            .Value!.Value.Should().Contain("\"state\":\"SYNCING\"");
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/service/demo-s1/leader", ct))
            .Value!.Value.Should().Contain("\"name\":\"s1a\"");
    }

    // AAA: повторный вызов при живом /clusters/demo/config — 200 {"seeded":false},
    // значения НЕ перезаписаны (идемпотентность образца seed.sh: «не портим»).
    [Fact]
    public async Task SeedDemo_AlreadySeeded_NoOp()
    {
        // Arrange — наливаем (если ещё не налито предыдущим кейсом) и фиксируем значения
        var ct = TestContext.Current.CancellationToken;
        await Client.PostAsync("/api/seed/demo", null, ct);
        var config = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/demo/config", ct);
        var status = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/demo/buckets/status/bucket_3", ct);

        // Act
        var resp = await Client.PostAsync("/api/seed/demo", null, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("seeded").GetBoolean().Should().BeFalse();
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/demo/config", ct))
            .Value!.Value.Should().Be(config.Value!.Value);
        (await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/clusters/demo/buckets/status/bucket_3", ct))
            .Value!.Value.Should().Be(status.Value!.Value);
    }

    // AAA: EnableSeedEndpoint=false (дефолт ApiOptions) — 404 ProblemDetails
    // (флаг проверяется до идемпотентности/записей — в etcd ничего не меняется).
    [Fact]
    public async Task SeedDemo_DisabledFlag_404()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await using var factory = new SeedDisabledFactory(Etcd);
        var client = factory.CreateClient();

        // Act
        var resp = await client.PostAsync("/api/seed/demo", null, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Not found");
    }
}
