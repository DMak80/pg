using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Core.S3;
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

// Сквозной путь s3-домена (arch/03 §12.1, t34): панель (WAF на PanelHostBuilder) →
// реальный etcd-контейнер (сид guid-ключей; refresher тикает вручную — hosted
// сняты) → стаб IWorkerApiGateway (коды/тела/ProblemDetails API воркера 1:1).
public sealed class S3ApiFactory : WebApplicationFactory<Program>
{
    public string EtcdEndpoint { get; set; } = "";

    public FixedTimeProvider Time { get; } = new();

    public TestWorkerApi WorkerApi { get; } = new();

    // false — оставить реальный WorkerApiGateway (кейс «нет живых ключей»):
    // стаб подменяется по умолчанию (коды/тела/журнал вызовов).
    public bool UseStubGateway { get; init; } = true;

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
            if (UseStubGateway)
                services.Replace(new ServiceDescriptor(typeof(IWorkerApiGateway), WorkerApi));
        });
    }
}

// Фикстура сценария: свой etcd-контейнер (динамический порт) + сид s3-ключей
// + фабрика (EnsureBuilt ПОСЛЕ etcd) + ручной тик refresher'а. Teardown при любом
// исходе: prefix-delete владельческих ключей + ассерт чистоты + контейнер.
public sealed class S3ApiFixture : IAsyncLifetime
{
    private readonly EtcdContainerFixture _etcd = new();

    public S3ApiFactory Factory { get; } = new();

    // Стаб API воркера фабрики (кейсы настраивают ответы/читают журнал вызовов).
    public TestWorkerApi WorkerApi => Factory.WorkerApi;

    public string Endpoint => _etcd.Endpoint;

    public string Prefix { get; } = $"t34a{Guid.NewGuid():N}";

    public string Cluster => $"{Prefix}demo";

    public bool SeedWorkerApi { get; init; } = true;

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _etcd.InitializeAsync();
        Factory.EtcdEndpoint = Endpoint;
        Factory.Time.Utc = DateTimeOffset.UtcNow;
        Factory.EnsureBuilt();

        // Сид: Active-кластер (config/нода/placement/resources/links/зонды/
        // denylist/endpoints) + живой ключ API воркера (если не выключен кейсом).
        await EtcdSeed.PutAsync(Endpoint, $"/s3/clusters/{Cluster}/config",
            """{"platform":"owns3","nodes":1,"secret_profile":"backups-main","created_unix":1760156800}""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/s3/clusters/{Cluster}/endpoints", "h1:18001,h2:18002", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/s3/clusters/{Cluster}/links", """[{"a":"east1","b":"east2"}]""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/s3/clusters/{Cluster}/denylist", """["west1"]""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/s3/clusters/{Cluster}/connectivity",
            """{"state":"OK","checked_unix":1760156900,"links":{"east1":{"east2":"ok"}}}""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/s3/clusters/{Cluster}/sync",
            """{"state":"SYNCED","checked_unix":1760156900,"pairs":[{"from":"east1","to":"east2","missing":0,"extra":0}]}""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/s3/clusters/{Cluster}/nodes/east1/placement", """{"docker_host":"h1"}""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/s3/clusters/{Cluster}/nodes/east1/resources", """{"cpu":"2","mem":"4Gi","disk":"200Gi"}""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/s3/clusters/{Cluster}/nodes/east1/state", "RUNNING", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/s3/clusters/{Cluster}/nodes/east1/placed_on", "h1", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/s3/clusters/{Cluster}/nodes/east1/usage",
            """{"used_bytes":107374182400,"total_bytes":214748364800,"updated_unix":1760156860}""", ct);
        if (SeedWorkerApi)
            await EtcdSeed.PutAsync(Endpoint, $"/s3worker/api/{Prefix}i1",
                $$"""{"url":"https://s3worker:8080","instance":"{{Prefix}}i1","since_unix":1760150001}""", ct);

        await RefreshSnapshotAsync();
    }

    // Тик refresher'а вручную (hosted сняты) — полный путь панель→etcd.
    public async Task RefreshSnapshotAsync()
    {
        var refresher = Factory.Services.GetRequiredService<S3SnapshotRefresher>();
        (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
    }

    public async ValueTask DisposeAsync()
    {
        await Factory.DisposeAsync();

        // Чистка владельческих ключей + ассерт чистоты до удаления контейнера.
        var gateway = EtcdTestHarness.NewGateway();
        await gateway.DeleteAsync(Endpoint, $"/s3/clusters/{Prefix}", prefix: true, CancellationToken.None);
        await gateway.DeleteAsync(Endpoint, $"/s3worker/api/{Prefix}", prefix: true, CancellationToken.None);
        await gateway.DeleteAsync(Endpoint, $"/s3worker/rotations/{Prefix}", prefix: true, CancellationToken.None);
        var leftovers = await gateway.RangeAsync(Endpoint, $"/s3/clusters/{Prefix}", CancellationToken.None);
        if (leftovers.IsSuccess && leftovers.Value.Count > 0)
            throw new InvalidOperationException("teardown cleanliness assert: cluster guid keys remain");

        await _etcd.DisposeAsync();
    }
}

[CollectionDefinition("s3-api")]
public sealed class S3ApiCollection : ICollectionFixture<S3ApiFixture>;

[Collection("s3-api")]
public class S3ApiTests(S3ApiFixture fx)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

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

    // Arrange: живой ключ /s3worker/api/ + стаб воркера 201. Act: POST /api/s3/clusters.
    // Assert: 201; стаб получил POST /api/s3/clusters с телом как есть (json-сравнение,
    // вкл. platform:"owns3").
    [Fact]
    public async Task Create_ProxiesToWorker_201()
    {
        using var client = await LoginAsync();
        fx.WorkerApi.Respond = _ => new WorkerApiResult(201,
            """{"name":"made","state":"NOT_INITIALIZED","nodes":1,"secretProfile":"backups-main","instances":["node1"]}""");
        var body = new
        {
            name = "made",
            secretProfile = "backups-main",
            instances = new[]
            {
                new { name = "node1", placement = new { dockerHost = "h1" } },
            },
        };

        // Act
        var created = await client.PostAsJsonAsync("/api/s3/clusters", body, TestContext.Current.CancellationToken);

        // Assert: 201 + Location; тело ушло воркеру как есть (platform подставлен).
        created.StatusCode.Should().Be(HttpStatusCode.Created);
        created.Headers.Location.Should().Be("/api/s3/clusters/made");
        var call = fx.WorkerApi.Calls.Should().ContainSingle().Which;
        call.Worker.Should().Be("s3worker");
        call.Method.Should().Be(HttpMethod.Post);
        call.Path.Should().Be("/api/s3/clusters");
        var sent = JsonSerializer.Deserialize<JsonElement>(
            JsonSerializer.Serialize(call.Body, Json), Json);
        sent.GetProperty("platform").GetString().Should().Be("owns3");
        sent.GetProperty("name").GetString().Should().Be("made");
        sent.GetProperty("instances").GetArrayLength().Should().Be(1);
        fx.WorkerApi.Reset();
    }

    // Arrange: стаб отвечает 404 ProblemDetails / 400 / 409 / 422 (подCase-блоки
    // внутри одного теста, каждый — AAA-комментарием). Act: DELETE-узел /
    // POST-кластер / POST-ротация. Assert: код и тело 1:1
    // (ProblemDetails application/problem+json как есть).
    [Fact]
    public async Task WorkerErrors_MappedOneToOne_404_400_409_422()
    {
        using var client = await LoginAsync();

        // Arrange: стаб 404 (ProblemDetails воркера).
        fx.WorkerApi.Respond = _ => new WorkerApiResult(404,
            """{"title":"Not found","status":404,"detail":"no such cluster"}""");

        // Act: DELETE ноды.
        var missing = await client.DeleteAsync(
            $"/api/s3/clusters/{fx.Cluster}/nodes/ghost", TestContext.Current.CancellationToken);

        // Assert: 404 и тело как есть.
        missing.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await missing.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("no such cluster");

        // Arrange: стаб 400 (валидация воркера).
        fx.WorkerApi.Respond = _ => new WorkerApiResult(400,
            """{"title":"Validation failed","status":400,"detail":"cpu out of range"}""");

        // Act: POST кластера с битым телом.
        var bad = await client.PostAsJsonAsync("/api/s3/clusters",
            new { name = "X", secretProfile = "p" }, TestContext.Current.CancellationToken);

        // Assert: 400 и тело как есть.
        bad.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await bad.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("cpu out of range");

        // Arrange: стаб 409 (имя занято).
        fx.WorkerApi.Respond = _ => new WorkerApiResult(409,
            """{"title":"Conflict","status":409,"detail":"cluster already exists"}""");

        // Act: POST кластера с занятым именем.
        var conflict = await client.PostAsJsonAsync("/api/s3/clusters",
            new { name = "made", secretProfile = "p" }, TestContext.Current.CancellationToken);

        // Assert: 409 как есть.
        conflict.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await conflict.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("cluster already exists");

        // Arrange: стаб 422 (профиль секретов недоступен).
        fx.WorkerApi.Respond = _ => new WorkerApiResult(422,
            """{"title":"Secret profile unavailable","status":422,"detail":"no profile","profiles":["backups-main"]}""");

        // Act: POST кластера с недоступным профилем.
        var profile = await client.PostAsJsonAsync("/api/s3/clusters",
            new { name = "made", secretProfile = "no-such" }, TestContext.Current.CancellationToken);

        // Assert: 422 с перечнем профилей как есть.
        profile.StatusCode.Should().Be(HttpStatusCode.UnprocessableEntity);
        (await profile.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("backups-main");
        fx.WorkerApi.Reset();
    }

    // Arrange: сид БЕЗ ключа /s3worker/api/. Act: любая мутация.
    // Assert: 503 "API воркера недоступен" (панельный ProblemDetails).
    [Fact]
    public async Task NoLiveWorkerKeys_Mutations_503()
    {
        // Arrange: своя фабрика на общем etcd, сид без api-ключа (отдельный префикс).
        var prefix = $"t34b{Guid.NewGuid():N}";
        var gateway = EtcdTestHarness.NewGateway();
        var ct = TestContext.Current.CancellationToken;
        try
        {
            await EtcdSeed.PutAsync(fx.Endpoint, $"/s3/clusters/{prefix}lonely/config",
                """{"platform":"owns3","nodes":0,"secret_profile":"backups-main"}""", ct);
            var factory = new S3ApiFactory { EtcdEndpoint = fx.Endpoint, UseStubGateway = false };
            factory.Time.Utc = DateTimeOffset.UtcNow;
            factory.EnsureBuilt();
            try
            {
                factory.Time.Utc += TimeSpan.FromSeconds(61);
                var client = factory.CreateClient();
                await client.PostAsJsonAsync("/api/auth/login",
                    new { username = "admin", password = "adminpw" }, ct);

                // Act: мутация при пустом WorkerEndpoints снапшота.
                var response = await client.DeleteAsync(
                    $"/api/s3/clusters/{prefix}lonely", ct);

                // Assert: панельный 503 «API воркера недоступен».
                response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
                (await response.Content.ReadAsStringAsync(ct))
                    .Should().Contain("Worker API unavailable");
            }
            finally
            {
                await factory.DisposeAsync();
            }
        }
        finally
        {
            await gateway.DeleteAsync(fx.Endpoint, $"/s3/clusters/{prefix}", prefix: true, CancellationToken.None);
        }
    }

    // Arrange: сид + refresher-тик. Act: GET /api/s3/clusters и /{c}.
    // Assert: сводка/детали со всеми секциями (connectivity/sync/denylist/denied).
    [Fact]
    public async Task Inspection_ReadsS3Snapshot()
    {
        using var client = await LoginAsync();

        // Act: сводный список.
        var list = await client.GetStringAsync("/api/s3/clusters", TestContext.Current.CancellationToken);

        // Assert: сводка сида.
        list.Should().Contain($"\"name\":\"{fx.Cluster}\"");
        list.Should().Contain("\"state\":\"ACTIVE\"");
        list.Should().Contain("\"nodesRunning\":1");
        list.Should().Contain("\"connectivityState\":\"OK\"");
        list.Should().Contain("\"syncState\":\"SYNCED\"");
        list.Should().Contain("\"rotationPending\":false");

        // Act: детали.
        var details = await client.GetStringAsync(
            $"/api/s3/clusters/{fx.Cluster}", TestContext.Current.CancellationToken);

        // Assert: все секции; PKI/секретов в ответе нет.
        details.Should().Contain("\"secretProfile\":\"backups-main\"");
        details.Should().Contain("\"denied\":false");  // east1 не в denylist
        details.Should().Contain("\"state\":\"SYNCED\"");
        details.Should().Contain("\"denylist\":[\"west1\"]");
        details.Should().NotContain("pem");

        // Act/Assert: неизвестный кластер — 404.
        var missing = await client.GetAsync(
            $"/api/s3/clusters/{fx.Prefix}ghost", TestContext.Current.CancellationToken);
        missing.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // Arrange: стаб 202/204 (delete-кластер: первый вызов 202, повтор 204) и
    // 201/200 (ротация: первый 201, повтор 200). Act: две мутации × 2.
    // Assert: панель зеркалит код оба раза (202→202, 204→204; 201→201, 200→200).
    [Fact]
    public async Task IdempotentRepeats_MirrorWorkerCodes_202_204_201_200()
    {
        using var client = await LoginAsync();

        // Arrange: delete-кластер — 202 на первый вызов, 204 на повтор (already).
        var deletes = 0;
        fx.WorkerApi.Respond = call => call.Method == HttpMethod.Delete
            ? new WorkerApiResult(++deletes == 1 ? 202 : 204, null)
            : new WorkerApiResult(201, """{"cluster":"x"}""");

        // Act: DELETE кластера дважды.
        var first = await client.DeleteAsync($"/api/s3/clusters/{fx.Cluster}", TestContext.Current.CancellationToken);
        var second = await client.DeleteAsync($"/api/s3/clusters/{fx.Cluster}", TestContext.Current.CancellationToken);

        // Assert: 202→202, 204→204.
        first.StatusCode.Should().Be(HttpStatusCode.Accepted);
        second.StatusCode.Should().Be(HttpStatusCode.NoContent);

        // Arrange: ротация — 201 на первый вызов, 200 на повтор (already).
        var rotations = 0;
        fx.WorkerApi.Respond = call => new WorkerApiResult(++rotations == 1 ? 201 : 200,
            """{"cluster":"x"}""");

        // Act: POST ротации дважды.
        var created = await client.PostAsync(
            $"/api/s3/clusters/{fx.Cluster}/rotations", null, TestContext.Current.CancellationToken);
        var repeated = await client.PostAsync(
            $"/api/s3/clusters/{fx.Cluster}/rotations", null, TestContext.Current.CancellationToken);

        // Assert: 201→201, 200→200.
        created.StatusCode.Should().Be(HttpStatusCode.Created);
        repeated.StatusCode.Should().Be(HttpStatusCode.OK);
        fx.WorkerApi.Reset();
    }

    // Arrange: стаб 202. Act: PUT links / sync-probe / deny / allow / resources.
    // Assert: 202 у асинхронных; resources — 200 (контракт §12.1); X-Requested-By
    // передан стабу у ротации/зонда, отсутствует у прочих.
    [Fact]
    public async Task AsyncMutations_202_RequestedByAuditing()
    {
        using var client = await LoginAsync();
        fx.WorkerApi.Respond = call => call.Path.EndsWith("/resources", StringComparison.Ordinal)
            ? new WorkerApiResult(200, """{"cpu":"2","mem":"4Gi","disk":"200Gi"}""")
            : new WorkerApiResult(202, null);

        // Act: пять мутаций.
        var links = await client.PutAsJsonAsync($"/api/s3/clusters/{fx.Cluster}/links",
            new { links = new[] { new { a = "east1", b = "east2" } } },
            TestContext.Current.CancellationToken);
        var probe = await client.PostAsync(
            $"/api/s3/clusters/{fx.Cluster}/sync-probe", null, TestContext.Current.CancellationToken);
        var deny = await client.PostAsync(
            $"/api/s3/clusters/{fx.Cluster}/nodes/east2/deny", null, TestContext.Current.CancellationToken);
        var allow = await client.PostAsync(
            $"/api/s3/clusters/{fx.Cluster}/nodes/east2/allow", null, TestContext.Current.CancellationToken);
        var resources = await client.PutAsJsonAsync(
            $"/api/s3/clusters/{fx.Cluster}/nodes/east1/resources",
            new { cpu = (decimal?)2m, memGi = 4, diskGi = 200 },
            TestContext.Current.CancellationToken);

        // Assert: коды зеркала; аудит — только у зонда (мутации 9–10) и НЕ у прочих.
        links.StatusCode.Should().Be(HttpStatusCode.Accepted);
        probe.StatusCode.Should().Be(HttpStatusCode.Accepted);
        deny.StatusCode.Should().Be(HttpStatusCode.Accepted);
        allow.StatusCode.Should().Be(HttpStatusCode.Accepted);
        resources.StatusCode.Should().Be(HttpStatusCode.OK);
        fx.WorkerApi.Calls.Should().Contain(c =>
            c.Path.EndsWith("/sync-probe", StringComparison.Ordinal) && c.RequestedBy == "admin");
        fx.WorkerApi.Calls.Should().Contain(c =>
            c.Path.EndsWith("/links", StringComparison.Ordinal) && c.RequestedBy == null);
        fx.WorkerApi.Calls.Should().Contain(c =>
            c.Path.EndsWith("/deny", StringComparison.Ordinal) && c.RequestedBy == null);
        fx.WorkerApi.Reset();
    }
}
