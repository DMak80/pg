using System.Net;
using System.Net.Http.Json;
using System.Text.RegularExpressions;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using ValkeyWorker.IntegrationTests.Etcd;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Api;

// WAF-мутации valkey-домена (spec §4.7): create 201/409/400 (все правила),
// delete 202/404/идемпотентность, config 200/404/400, resources 200/404/400,
// rotate 202/409/404 + СЕКРЕТНОСТЬ ответов (§9.2: пароли не отдаются).
[Collection(ValkeyApiCollection.Name)]
public class ClusterMutationsApiTests(ValkeyApiFixture fx)
{
    private HttpClient Client => fx.Factory.CreateClient();

    private static string Cluster(string name) => $"{name}{Guid.NewGuid().ToString("N")[..6]}";

    private static readonly Regex PasswordLike = new("^[A-Za-z0-9]{32}$", RegexOptions.Compiled);

    // §9.2: сериализованный ответ не содержит ключей *password* и значений-паролей.
    private static void AssertNoSecrets(string body)
    {
        body.Should().NotContain("password");
        foreach (var match in Regex.Matches(body, "[A-Za-z0-9]{32}"))
            PasswordLike.IsMatch(match.ToString() ?? "").Should().BeFalse(
                "32-символьное значение в ответе похоже на пароль: {0}", match);
    }

    [Fact]
    public async Task Create_201_ДискавериКлючиБезПаролей()
    {
        // Arrange: валидная заявка.
        var cluster = Cluster("mk");
        using var client = Client;

        // Act
        using var response = await client.PostAsJsonAsync("/api/valkey/clusters",
            new { name = cluster, nodes = 1, maxmemoryBytes = 536870912, maxmemoryPolicy = "allkeys-lru",
                resources = new { cpu = 1m, memGi = 1, diskGi = 10 } },
            TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 201 и тело без секретов; config в etcd NOT_INITIALIZED.
        response.StatusCode.Should().Be(HttpStatusCode.Created, body);
        AssertNoSecrets(body);
        body.Should().Contain($"\"name\":\"{cluster}\"");
        var config = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/valkey/clusters/{cluster}/config", TestContext.Current.CancellationToken);
        config.Value!.Value.Should().Contain("NOT_INITIALIZED");
    }

    [Fact]
    public async Task Create_Повтор_409()
    {
        // Arrange: кластер создан первым POST.
        var cluster = Cluster("dup");
        using var client = Client;
        var request = new { name = cluster, nodes = 1, maxmemoryBytes = 536870912,
            maxmemoryPolicy = "allkeys-lru", resources = new { cpu = 1m, memGi = 1, diskGi = 10 } };
        (await client.PostAsJsonAsync("/api/valkey/clusters", request, TestContext.Current.CancellationToken))
            .StatusCode.Should().Be(HttpStatusCode.Created);

        // Act: повтор.
        using var response = await client.PostAsJsonAsync("/api/valkey/clusters", request,
            TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 409.
        response.StatusCode.Should().Be(HttpStatusCode.Conflict, body);
    }

    [Theory]
    [InlineData(2, 536870912, "allkeys-lru", 1, 1, 10, "nodes")]
    [InlineData(1, 0, "allkeys-lru", 1, 1, 10, "maxmemoryBytes")]
    [InlineData(1, 536870912, "lru", 1, 1, 10, "maxmemoryPolicy")]
    [InlineData(1, 536870912, "allkeys-lru", 0.001, 1, 10, "cpu")]
    [InlineData(1, 536870912, "allkeys-lru", 1, 0, 10, "memGi")]
    [InlineData(1, 1073741824, "allkeys-lru", 1, 1, 10, "maxmemoryBytes")] // инвариант R3
    public async Task Create_Невалидная_400СПолем(
        int nodes, long maxmemory, string policy, decimal cpu, int memGi, int diskGi, string field)
    {
        // Arrange/Act
        var cluster = Cluster("bad");
        using var client = Client;
        using var response = await client.PostAsJsonAsync("/api/valkey/clusters",
            new { name = cluster, nodes, maxmemoryBytes = maxmemory, maxmemoryPolicy = policy,
                resources = new { cpu, memGi, diskGi } },
            TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 400 с errors.<field>.
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest, body);
        body.Should().Contain($"\"{field}\"");
        // Компенсации нет — кластер не создан.
        var config = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/valkey/clusters/{cluster}/config", TestContext.Current.CancellationToken);
        config.Value.Should().BeNull();
    }

    [Fact]
    public async Task Delete_202_404_Идемпотентность()
    {
        // Arrange: создан кластер.
        var cluster = Cluster("del");
        using var client = Client;
        (await client.PostAsJsonAsync("/api/valkey/clusters",
            new { name = cluster, nodes = 1, maxmemoryBytes = 536870912,
                resources = new { cpu = 1m, memGi = 1, diskGi = 10 } },
            TestContext.Current.CancellationToken)).StatusCode.Should().Be(HttpStatusCode.Created);

        // Act 1: перевод в TO_REMOVE.
        using var first = await client.DeleteAsync($"/api/valkey/clusters/{cluster}",
            TestContext.Current.CancellationToken);
        // Act 2: повтор (уже TO_REMOVE).
        using var second = await client.DeleteAsync($"/api/valkey/clusters/{cluster}",
            TestContext.Current.CancellationToken);
        // Act 3: несуществующий.
        using var missing = await client.DeleteAsync(
            $"/api/valkey/clusters/{Cluster("nope")}", TestContext.Current.CancellationToken);

        // Assert
        first.StatusCode.Should().Be(HttpStatusCode.Accepted);
        second.StatusCode.Should().Be(HttpStatusCode.Accepted, "повтор идемпотентен");
        missing.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var config = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/valkey/clusters/{cluster}/config", TestContext.Current.CancellationToken);
        config.Value!.Value.Should().Contain("TO_REMOVE");
    }

    [Fact]
    public async Task Config_200_404_400_ИСекретность()
    {
        // Arrange: Active-кластер (state снят — имитируем прогнанный provisioning).
        var cluster = Cluster("cfg");
        using var client = Client;
        (await client.PostAsJsonAsync("/api/valkey/clusters",
            new { name = cluster, nodes = 1, maxmemoryBytes = 536870912,
                resources = new { cpu = 1m, memGi = 1, diskGi = 10 } },
            TestContext.Current.CancellationToken)).StatusCode.Should().Be(HttpStatusCode.Created);
        var activeConfig = """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""";
        await fx.Etcd.Gateway.PutAsync(fx.Etcd.Endpoint, $"/valkey/clusters/{cluster}/config",
            activeConfig, null, TestContext.Current.CancellationToken);

        // Act 1: валидная мутация.
        using var ok = await client.PutAsJsonAsync($"/api/valkey/clusters/{cluster}/config",
            new { maxmemoryBytes = 268435456, maxmemoryPolicy = "noeviction" },
            TestContext.Current.CancellationToken);
        var okBody = await ok.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Act 2: инвариант R3 нарушен (maxmemory == mem).
        using var invariant = await client.PutAsJsonAsync($"/api/valkey/clusters/{cluster}/config",
            new { maxmemoryBytes = 1073741824 }, TestContext.Current.CancellationToken);

        // Act 3: неизвестный policy.
        using var badPolicy = await client.PutAsJsonAsync($"/api/valkey/clusters/{cluster}/config",
            new { maxmemoryPolicy = "lru" }, TestContext.Current.CancellationToken);
        // Act 4: несуществующий кластер.
        using var gone = await client.PutAsJsonAsync($"/api/valkey/clusters/{Cluster("nope")}/config",
            new { maxmemoryBytes = 1 }, TestContext.Current.CancellationToken);

        // Assert
        ok.StatusCode.Should().Be(HttpStatusCode.OK, okBody);
        AssertNoSecrets(okBody);
        invariant.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        badPolicy.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        gone.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Resources_200_404_400()
    {
        // Arrange: кластер с node1.
        var cluster = Cluster("res");
        using var client = Client;
        (await client.PostAsJsonAsync("/api/valkey/clusters",
            new { name = cluster, nodes = 1, maxmemoryBytes = 536870912,
                resources = new { cpu = 1m, memGi = 1, diskGi = 10 } },
            TestContext.Current.CancellationToken)).StatusCode.Should().Be(HttpStatusCode.Created);

        // Act 1: мутация лимитов node1.
        using var ok = await client.PutAsJsonAsync($"/api/valkey/clusters/{cluster}/nodes/node1/resources",
            new { cpu = 2m, memGi = 2, diskGi = 20 }, TestContext.Current.CancellationToken);
        var okBody = await ok.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        // Act 2: node2 — 404 (v1 standalone).
        using var node2 = await client.PutAsJsonAsync($"/api/valkey/clusters/{cluster}/nodes/node2/resources",
            new { cpu = 2m }, TestContext.Current.CancellationToken);
        // Act 3: граница cpu.
        using var bad = await client.PutAsJsonAsync($"/api/valkey/clusters/{cluster}/nodes/node1/resources",
            new { cpu = 100m }, TestContext.Current.CancellationToken);

        // Assert
        ok.StatusCode.Should().Be(HttpStatusCode.OK, okBody);
        okBody.Should().Contain("\"2\"");
        node2.StatusCode.Should().Be(HttpStatusCode.NotFound);
        bad.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var resources = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/valkey/clusters/{cluster}/nodes/node1/resources", TestContext.Current.CancellationToken);
        resources.Value!.Value.Should().Be("""{"cpu":"2","mem":"2Gi","disk":"20Gi"}""");
    }

    [Fact]
    public async Task Rotate_202_409_400_404_ИСекретность()
    {
        // Arrange: Active-кластер.
        var cluster = Cluster("rot");
        using var client = Client;
        (await client.PostAsJsonAsync("/api/valkey/clusters",
            new { name = cluster, nodes = 1, maxmemoryBytes = 536870912,
                resources = new { cpu = 1m, memGi = 1, diskGi = 10 } },
            TestContext.Current.CancellationToken)).StatusCode.Should().Be(HttpStatusCode.Created);

        // Act 1: заявка app-ротации.
        using var first = await client.PostAsJsonAsync($"/api/valkey/clusters/{cluster}/password/rotate",
            new { role = "app" }, TestContext.Current.CancellationToken);
        var firstBody = await first.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        // Act 2: повтор — 409 (заявка жива).
        using var second = await client.PostAsJsonAsync($"/api/valkey/clusters/{cluster}/password/rotate",
            new { role = "app" }, TestContext.Current.CancellationToken);
        // Act 2b: роль вне канона — валидация 400 с errors.role (t03-фикс, 02 §11.2).
        using var wrongRole = await client.PostAsJsonAsync($"/api/valkey/clusters/{cluster}/password/rotate",
            new { role = "wrong" }, TestContext.Current.CancellationToken);
        var wrongBody = await wrongRole.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        // Act 3: несуществующий кластер.
        using var gone = await client.PostAsJsonAsync($"/api/valkey/clusters/{Cluster("nope")}/password/rotate",
            new { role = "app" }, TestContext.Current.CancellationToken);

        // Assert
        first.StatusCode.Should().Be(HttpStatusCode.Accepted, firstBody);
        AssertNoSecrets(firstBody);
        firstBody.Should().Contain("\"role\":\"app\"");
        second.StatusCode.Should().Be(HttpStatusCode.Conflict);
        // Валидация роли — 400 с полем errors.role (канон create/config/resources).
        wrongRole.StatusCode.Should().Be(HttpStatusCode.BadRequest, wrongBody);
        wrongBody.Should().Contain("\"role\"");
        gone.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var ticket = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/valkeyworker/rotations/{cluster}", TestContext.Current.CancellationToken);
        ticket.Value!.Value.Should().Contain("\"role\":\"app\"");
    }

    [Fact]
    public async Task Seed_ВыключенныйФлаг_404()
    {
        // Arrange: фабрика с EnableSeedEndpoint=false.
        var factory = new SeedOffFactory(fx.Etcd);
        using var client = factory.CreateClient();

        // Act
        using var response = await client.PostAsync("/api/seed/demo", null,
            TestContext.Current.CancellationToken);

        // Assert: псевдо-404 до любых чтений.
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        await factory.DisposeAsync();
    }

    private sealed class SeedOffFactory(ValkeyEtcdFixture etcd) : ValkeyApiFactory(etcd)
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(
                new Dictionary<string, string?> { ["ValkeyWorker:Api:EnableSeedEndpoint"] = "false" }));
        }
    }
}
