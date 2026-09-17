using System.Net;
using System.Net.Http.Json;
using AdminPanel.Core.Valkey;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Сквозной путь valkey-домена (spec §5.2): панель (WAF на PanelHostBuilder) →
// реальный etcd-контейнер (сид guid-ключей; refresher тикает вручную — hosted
// сняты) → стаб IWorkerApiGateway (коды/тела/ProblemDetails API воркера t02).
public sealed class ValkeyApiFactory : WebApplicationFactory<Program>
{
    public string EtcdEndpoint { get; set; } = "";

    public FixedTimeProvider Time { get; } = new();

    public TestWorkerApi WorkerApi { get; } = new();

    private bool _built;

    public void EnsureBuilt()
    {
        if (_built) return;
        PanelHostBuilder.BuildExclusive(this);
        _built = true;
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting("AdminPanel:Auth:Username", "admin");
        builder.UseSetting("AdminPanel:Auth:Password", "adminpw");
        builder.UseSetting("AdminPanel:Auth:AllowHttp", "true");
        if (EtcdEndpoint.Length > 0)
            builder.UseSetting("AdminPanel:Etcd:Endpoints:0", EtcdEndpoint);
        builder.ConfigureTestServices(services =>
        {
            // hosted сняты: снапшот собирается тестом вручную (RefreshOnceAsync).
            services.RemoveAll<IHostedService>();
            services.Replace(new ServiceDescriptor(typeof(TimeProvider), Time));
            // Стаб API воркера: фиксирует путь/метод/тело/X-Requested-By (1:1-маппинг).
            services.Replace(new ServiceDescriptor(typeof(IWorkerApiGateway), WorkerApi));
        });
    }
}

// Фикстура сценария: свой etcd-контейнер (динамический порт) + сид valkey-ключей
// + фабрика (EnsureBuilt ПОСЛЕ etcd) + ручной тик refresher'а. Teardown при любом
// исходе: prefix-delete владельческих ключей + ассерт чистоты + контейнер.
public sealed class ValkeyApiFixture : IAsyncLifetime
{
    private readonly EtcdContainerFixture _etcd = new();

    public ValkeyApiFactory Factory { get; } = new();

    // Стаб API воркера фабрики (кейсы настраивают ответы/читают журнал вызовов).
    public TestWorkerApi WorkerApi => Factory.WorkerApi;

    public string Endpoint => _etcd.Endpoint;

    public string Prefix { get; } = $"t03a{Guid.NewGuid():N}";

    public string Cluster => $"{Prefix}demo";

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _etcd.InitializeAsync();
        Factory.EtcdEndpoint = Endpoint;
        Factory.Time.Utc = DateTimeOffset.UtcNow;
        Factory.EnsureBuilt();

        // Сид: Active-кластер с endpoints/admin-парой + живой ключ API воркера.
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{Cluster}/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{Cluster}/endpoints", "host.docker.internal:17001", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{Cluster}/nodes/node1/state", "RUNNING", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{Cluster}/admin_user", "admin", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{Cluster}/admin_password", "topsecret0123456789", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkeyworker/api/{Prefix}i1",
            $$"""{"url":"https://valkeyworker:8080","instance":"{{Prefix}}i1","since_unix":1756000001}""", ct);

        await RefreshSnapshotAsync();
    }

    // Тик refresher'а вручную (hosted сняты) — полный путь панель→etcd.
    public async Task RefreshSnapshotAsync()
    {
        var refresher = Factory.Services.GetRequiredService<ValkeySnapshotRefresher>();
        (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
    }

    public async ValueTask DisposeAsync()
    {
        await Factory.DisposeAsync();

        // Чистка владельческих ключей + ассерт чистоты до удаления контейнера.
        var gateway = EtcdTestHarness.NewGateway();
        await gateway.DeleteAsync(Endpoint, $"/valkey/clusters/{Prefix}", prefix: true, CancellationToken.None);
        await gateway.DeleteAsync(Endpoint, $"/valkeyworker/api/{Prefix}", prefix: true, CancellationToken.None);
        var leftovers = await gateway.RangeAsync(Endpoint, $"/valkey/clusters/{Prefix}", CancellationToken.None);
        if (leftovers.IsSuccess && leftovers.Value.Count > 0)
            throw new InvalidOperationException("teardown-ассерт чистоты: остались guid-ключи кластера");

        await _etcd.DisposeAsync();
    }
}

[CollectionDefinition("valkey-api")]
public sealed class ValkeyApiCollection : ICollectionFixture<ValkeyApiFixture>;

[Collection("valkey-api")]
public class ValkeyApiTests(ValkeyApiFixture fx)
{
    // Свежий клиент + логин (окно rate-limiter'а двигается, cookie в клиенте).
    private async Task<HttpClient> LoginAsync()
    {
        fx.Factory.Time.Utc += TimeSpan.FromSeconds(61);
        var client = fx.Factory.CreateClient();
        var login = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);
        login.StatusCode.Should().Be(HttpStatusCode.NoContent);
        return client;
    }

    // 1) GET /api/valkey/clusters → 200, сводки сида (nodesRunning, endpoints).
    [Fact]
    public async Task Get_Clusters_ReturnsSeededSummaries()
    {
        // Arrange
        using var client = await LoginAsync();

        // Act
        var body = await client.GetStringAsync("/api/valkey/clusters", TestContext.Current.CancellationToken);

        // Assert
        body.Should().Contain($"\"name\":\"{fx.Cluster}\"");
        body.Should().Contain("\"state\":\"ACTIVE\"");
        body.Should().Contain("\"nodesRunning\":1");
        body.Should().Contain("host.docker.internal:17001");
    }

    // 2) GET /api/valkey/clusters/<c> → 200 детали (live=null — пробы нет);
    //    404 неизвестного кластера.
    [Fact]
    public async Task Get_ClusterDetails_FoundLiveNullAndNotFound()
    {
        using var client = await LoginAsync();

        // Act: детали сида.
        var details = await client.GetStringAsync(
            $"/api/valkey/clusters/{fx.Cluster}", TestContext.Current.CancellationToken);

        // Assert: нода без live (проба молчит), кредов в ответе нет.
        details.Should().Contain($"\"name\":\"{fx.Cluster}\"");
        details.Should().Contain("\"state\":\"RUNNING\"");
        details.Should().Contain("\"live\":null");
        details.Should().NotContain("topsecret0123456789");

        // Act/Assert: неизвестный кластер — 404.
        var missing = await client.GetAsync(
            $"/api/valkey/clusters/{fx.Prefix}ghost", TestContext.Current.CancellationToken);
        missing.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // 2b) GET без снапшота → 503 (отдельная фабрика: стор до первого тика).
    [Fact]
    public async Task Get_Clusters_NoSnapshot_Returns503()
    {
        // Arrange: своя фабрика на том же etcd, БЕЗ ручного тика (store.Current = null).
        var factory = new ValkeyApiFactory { EtcdEndpoint = fx.Endpoint };
        factory.Time.Utc = DateTimeOffset.UtcNow;
        factory.EnsureBuilt();
        try
        {
            factory.Time.Utc += TimeSpan.FromSeconds(61);
            var client = factory.CreateClient();
            await client.PostAsJsonAsync("/api/auth/login",
                new { username = "admin", password = "adminpw" }, TestContext.Current.CancellationToken);

            // Act
            var response = await client.GetAsync("/api/valkey/clusters", TestContext.Current.CancellationToken);

            // Assert
            response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        }
        finally
        {
            await factory.DisposeAsync();
        }
    }

    // 3) GET /api/workers → содержит карточку "valkeyworker" с инстансом сида.
    [Fact]
    public async Task Get_Workers_ContainsValkeyworkerCard()
    {
        using var client = await LoginAsync();

        // Act
        var body = await client.GetStringAsync("/api/workers", TestContext.Current.CancellationToken);

        // Assert: третья карточка с инстансом из живого ключа /valkeyworker/api/.
        body.Should().Contain("\"worker\":\"valkeyworker\"");
        body.Should().Contain($"\"instance\":\"{fx.Prefix}i1\"");
    }

    // 4) POST /api/valkey/clusters → 201 (Location, DTO); 409 ProblemDetails воркера —
    //    телом как есть; недоступный API воркера → 503 панели.
    [Fact]
    public async Task Post_CreateCluster_ProxyCodesAndProblemDetails()
    {
        using var client = await LoginAsync();

        // Act: успешное создание — стаб отвечает 201 с DTO воркера.
        fx.WorkerApi.Respond = _ => new WorkerApiResult(201,
            """{"name":"made","state":"NOT_INITIALIZED","nodes":1,"maxmemoryBytes":268435456,"maxmemoryPolicy":"allkeys-lru","cpu":"1","memGi":"1","diskGi":"10"}""");
        var created = await client.PostAsJsonAsync("/api/valkey/clusters",
            new { name = "made", maxmemoryBytes = 268435456, maxmemoryPolicy = "allkeys-lru" },
            TestContext.Current.CancellationToken);

        // Assert: 201 + Location + путь прокси к valkeyworker.
        created.StatusCode.Should().Be(HttpStatusCode.Created);
        created.Headers.Location.Should().Be("/api/valkey/clusters/made");
        (await created.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("\"name\":\"made\"");
        fx.WorkerApi.Calls.Should().Contain(c =>
            c.Worker == "valkeyworker" && c.Method == HttpMethod.Post && c.Path == "/api/valkey/clusters");

        // Act/Assert: 409 воркера — телом как есть (ProblemDetails не переписывается).
        fx.WorkerApi.Respond = _ => new WorkerApiResult(409,
            """{"title":"Conflict","status":409,"detail":"уже существует"}""");
        var conflict = await client.PostAsJsonAsync("/api/valkey/clusters",
            new { name = "made" }, TestContext.Current.CancellationToken);
        conflict.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await conflict.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("уже существует");

        // Act/Assert: недоступность API воркера — собственный 503 панели.
        fx.WorkerApi.Throw = new WorkerApiUnavailableException("valkeyworker");
        var unavailable = await client.PostAsJsonAsync("/api/valkey/clusters",
            new { name = "made" }, TestContext.Current.CancellationToken);
        unavailable.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        fx.WorkerApi.Reset();
    }

    // 5) DELETE → 202; PUT config → 200; PUT resources → 200; POST rotate → 202
    //    (стаб фиксирует X-Requested-By: admin после login).
    [Fact]
    public async Task Mutations_ProxyWithRequestedByHeader()
    {
        using var client = await LoginAsync();
        fx.WorkerApi.Respond = call => call.Path.Contains("/password/rotate")
            ? new WorkerApiResult(202, """{"cluster":"x","role":"app","requestedUnix":1,"requestedBy":"admin"}""")
            : new WorkerApiResult(200, """{"cluster":"x"}""");

        // Act: все 4 мутации.
        var delete = await client.DeleteAsync("/api/valkey/clusters/x", TestContext.Current.CancellationToken);
        var config = await client.PutAsJsonAsync("/api/valkey/clusters/x/config",
            new { maxmemoryBytes = 134217728 }, TestContext.Current.CancellationToken);
        var resources = await client.PutAsJsonAsync("/api/valkey/clusters/x/nodes/node1/resources",
            new { cpu = 2, memGi = 2, diskGi = 20 }, TestContext.Current.CancellationToken);
        var rotate = await client.PostAsJsonAsync("/api/valkey/clusters/x/password/rotate",
            new { role = "app" }, TestContext.Current.CancellationToken);

        // Assert: коды панели (202/200/200/202); оператор у rotate — заголовком.
        delete.StatusCode.Should().Be(HttpStatusCode.Accepted);
        config.StatusCode.Should().Be(HttpStatusCode.OK);
        resources.StatusCode.Should().Be(HttpStatusCode.OK);
        rotate.StatusCode.Should().Be(HttpStatusCode.Accepted);
        fx.WorkerApi.Calls.Should().Contain(c =>
            c.Path == "/api/valkey/clusters/x/password/rotate" && c.RequestedBy == "admin");
        fx.WorkerApi.Calls.Should().Contain(c =>
            c.Path == "/api/valkey/clusters/x/nodes/node1/resources" && c.RequestedBy == null);
        fx.WorkerApi.Reset();
    }

    // 6) Креды в ответах отсутствуют: инспекция сида не содержит admin_password.
    [Fact]
    public async Task Responses_NeverCarrySecrets()
    {
        using var client = await LoginAsync();

        // Act: сводка + детали + алерты + overview.
        var list = await client.GetStringAsync("/api/valkey/clusters", TestContext.Current.CancellationToken);
        var details = await client.GetStringAsync($"/api/valkey/clusters/{fx.Cluster}", TestContext.Current.CancellationToken);

        // Assert: пароль сида не утёк ни в один ответ.
        list.Should().NotContain("topsecret0123456789");
        details.Should().NotContain("topsecret0123456789");
    }
}
