using System.Net;
using System.Net.Http.Json;
using System.Text;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using PgWorker.App;
using PgWorker.Backups.Supervisor;
using PgWorker.Core.Templates;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// POST/DELETE /api/backups/orphans/{c}/{x}/hold, POST .../delete (t04,
// arch/19 §4): hold/unhold/заявка явного удаления сирот. Гварды 400
// (confirm-мисматч/нет тела), 404 (не сирота/мусорные имена), 409 (DELETING),
// 503 (etcd-сбой/подсистема выключена); повторные hold/заявки идемпотентны.
// Целевая архитектура E2E (docs/e2e-isolation.md §1): единица изоляции —
// класс; окружение — СВОЙ etcd в СВОЙ docker-сети (OwnedEtcdFixture) —
// никто вне класса не может писать в его etcd.
[Collection(NonE2eCollection.Name)]
public class OrphansApiTests(OwnedEtcdFixture etcdFixture)
    : IClassFixture<OwnedEtcdFixture>, IAsyncDisposable
{
    // Фабрика с ВКЛЮЧЕННОЙ подсистемой бэкапов (основные кейсы); выключенное
    // состояние проверяет _disabledFactory (default false, тот же etcd).
    private readonly OrphansApiFactory _factory = new(etcdFixture);

    // Фабрика с выключенной подсистемой (кейс 503) — на ТОМ ЖЕ собственном etcd.
    private readonly DisabledBackupsApiFactory _disabledFactory = new(etcdFixture);

    private HttpClient Client => _factory.CreateClient();

    private OwnedEtcdFixture Etcd => etcdFixture;

    // Конфигурация WAF-хоста PgWorker: копия легационной PgWorkerApiFactory
    // (та привязана к типу коллекционной EtcdFixture) + собственные секреты
    // Д7 через DI вместо process-env — класс не зависит от process-wide
    // переменных и не делит окружение ни с кем. Loops не стартуют (API-мутации).
    private static void ConfigurePgWorkerHost(IWebHostBuilder builder, string etcdEndpoint)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                ["PgWorker:Etcd:Endpoints:0"] = etcdEndpoint,
                ["PgWorker:Docker:Hosts:0:Name"] = "local",
                ["PgWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
                // WAF-хост без сертов: mTLS выключен (прод-канон — false, arch/14 §1.1).
                ["PgWorker:Api:Tls:AllowInsecureHttp"] = "true",
                ["PgWorker:Api:AdvertiseUrl"] = "https://localhost:9999",
                ["PgWorker:Api:EnableSeedEndpoint"] = "true",
            }));
        builder.ConfigureServices(services =>
        {
            services.RemoveAll<IHostedService>(); // Reconcile/Keepalive/Snapshot не стартуют
            // Секреты Д7 — фиксированные значения вместо env (SecretsFromEnv
            // читает process-env при старте хоста; здесь заменяем регистрацию).
            services.RemoveAll<InstallSecrets>();
            services.AddSingleton(new InstallSecrets("x", "x", "x", "x"));
        });
    }

    // WAF-оверрайд: подсистема бэкапов включена (гвард 503 снят для основных
    // кейсов; выключенное состояние проверяет DisabledBackupsApiFactory).
    // S3-комплект/Job:Image — dummy: fail-fast валидация Enabled=true требует
    // непустой комплект (API-грани сирот S3 не касается — это только валидатор).
    private sealed class OrphansApiFactory(OwnedEtcdFixture etcd) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            ConfigurePgWorkerHost(builder, etcd.Endpoint);
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["PgWorker:Backups:Enabled"] = "true",
                    ["PgWorker:Backups:S3:Endpoint"] = "http://minio-test",
                    ["PgWorker:Backups:S3:Bucket"] = "bkt",
                    ["PgWorker:Backups:S3:AccessKey"] = "test",
                    ["PgWorker:Backups:S3:SecretKey"] = "test",
                    ["PgWorker:Backups:Job:Image"] = "pgworker-backup:test",
                }));
        }
    }

    // Выключенная подсистема (default false): кейс 503 «подсистема выключена».
    private sealed class DisabledBackupsApiFactory(OwnedEtcdFixture etcd)
        : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder) =>
            ConfigurePgWorkerHost(builder, etcd.Endpoint);
    }

    // Чистка контура сирот: реестр + hold/заявки предыдущих кейсов (изоляция —
    // порядок кейсов в коллекции не гарантирован).
    private async Task CleanupAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await Etcd.Gateway.DeleteAsync(Etcd.Endpoint, OrphanRegistry.Key, prefix: false, ct);
        await Etcd.Gateway.DeleteAsync(Etcd.Endpoint, OrphanRegistry.HoldsPrefix, prefix: true, ct);
        await Etcd.Gateway.DeleteAsync(Etcd.Endpoint, OrphanRegistry.DeletesPrefix, prefix: true, ct);
    }

    // Сид реестра сирот: одна запись ghost/s1 в заданном state (AAA-Arrange).
    private async Task SeedRegistryAsync(string prefix, string state = "OBSERVED",
        bool hasValidFull = false)
    {
        var registry = new OrphanRegistry.Registry(
            [new OrphanEntry(prefix, "cluster", 100, 1757760000,
                state == "DELETING" ? OrphanState.Deleting : OrphanState.Observed,
                hasValidFull)],
            1757764800);
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, OrphanRegistry.Key,
            OrphanRegistry.ToJson(registry), null, TestContext.Current.CancellationToken);
    }

    private async Task<string?> GetValueAsync(string key)
        => (await Etcd.Gateway.GetAsync(
            Etcd.Endpoint, key, TestContext.Current.CancellationToken)).Value?.Value;

    // AAA (AC6): hold → 204; в etcd ключ канона с set_by из заголовка
    [Fact]
    public async Task Hold_204_ключ_в_etcd()
    {
        // Arrange — реестр с сиротой ghost/s1
        await CleanupAsync();
        await SeedRegistryAsync("ghost/s1");
        var ct = TestContext.Current.CancellationToken;
        var req = new HttpRequestMessage(HttpMethod.Post, "/api/backups/orphans/ghost/s1/hold");
        req.Headers.Add("X-Requested-By", "panel");

        // Act
        var resp = await Client.SendAsync(req, ct);

        // Assert — 204 и ключ канона со значениями заголовка
        resp.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var raw = await GetValueAsync(OrphanRegistry.HoldKey("ghost/s1"));
        raw.Should().NotBeNull();
        raw.Should().Contain("\"set_by\":\"panel\"").And.Contain("\"set_unix\":");
    }

    // AAA (AC6): повторный hold идемпотентен — 204, put поверх (значение заменено)
    [Fact]
    public async Task Hold_повторный_204_перезаписывает()
    {
        // Arrange — реестр + hold чужого автора (operator)
        await CleanupAsync();
        await SeedRegistryAsync("ghost/s1");
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, OrphanRegistry.HoldKey("ghost/s1"),
            OrphanRegistry.HoldToJson(1757760000, "operator"), null, TestContext.Current.CancellationToken);
        var ct = TestContext.Current.CancellationToken;

        // Act — два hold подряд (второй поверх первого)
        var first = await SendHoldAsync(ct);
        var second = await SendHoldAsync(ct);

        // Assert — оба 204; ключ перезаписан автором panel
        first.StatusCode.Should().Be(HttpStatusCode.NoContent);
        second.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var raw = await GetValueAsync(OrphanRegistry.HoldKey("ghost/s1"));
        raw.Should().Contain("\"set_by\":\"panel\"", "put поверх — не 409");
    }

    // AAA (AC6): префикса нет в реестре (реестр пуст) → 404
    [Fact]
    public async Task Hold_404_не_сирота()
    {
        // Arrange — контур чист (реестра нет)
        await CleanupAsync();
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await SendHoldAsync(ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // AAA (AC6): имена не-regex → 404 БЕЗ похода в etcd (реестр нетронут)
    [Fact]
    public async Task Hold_404_мусорные_имена()
    {
        // Arrange — реестр с сиротой; ожидаемое значение для сверки нетронутости
        await CleanupAsync();
        await SeedRegistryAsync("ghost/s1");
        var seeded = await GetValueAsync(OrphanRegistry.Key);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsync("/api/backups/orphans/BAD!/x/hold", null, ct);

        // Assert — 404; реестр не изменился
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await GetValueAsync(OrphanRegistry.Key)).Should().Be(seeded);
    }

    // AAA (AC6): запись в DELETING → 409; hold-ключа нет (поздно защищать)
    [Fact]
    public async Task Hold_409_DELETING()
    {
        // Arrange — запись в доводке
        await CleanupAsync();
        await SeedRegistryAsync("ghost/s1", "DELETING");
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await SendHoldAsync(ct);

        // Assert — 409; ключа hold нет
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await GetValueAsync(OrphanRegistry.HoldKey("ghost/s1"))).Should().BeNull();
    }

    // AAA (AC6): unhold 204 и ключ удалён; повторный DELETE (ключа нет) — тоже 204
    [Fact]
    public async Task Unhold_204_идемпотентен()
    {
        // Arrange — реестр + живой hold-ключ
        await CleanupAsync();
        await SeedRegistryAsync("ghost/s1");
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, OrphanRegistry.HoldKey("ghost/s1"),
            OrphanRegistry.HoldToJson(1757760000, "operator"), null, TestContext.Current.CancellationToken);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var first = await Client.DeleteAsync("/api/backups/orphans/ghost/s1/hold", ct);
        var gone = await GetValueAsync(OrphanRegistry.HoldKey("ghost/s1"));
        var second = await Client.DeleteAsync("/api/backups/orphans/ghost/s1/hold", ct);

        // Assert — оба 204; ключ удалён после первого
        first.StatusCode.Should().Be(HttpStatusCode.NoContent);
        gone.Should().BeNull("unhold снимает hold-ключ");
        second.StatusCode.Should().Be(HttpStatusCode.NoContent, "unhold идемпотентен");
    }

    // AAA (AC6): заявка delete с confirm → 202 + DTO; ключ заявки канона в etcd
    [Fact]
    public async Task Delete_202_ключ_заявки()
    {
        // Arrange — реестр с сиротой ghost/s1
        await CleanupAsync();
        await SeedRegistryAsync("ghost/s1");
        var ct = TestContext.Current.CancellationToken;
        var req = new HttpRequestMessage(HttpMethod.Post, "/api/backups/orphans/ghost/s1/delete")
        {
            Content = new StringContent("""{"confirm":"ghost/s1"}""", Encoding.UTF8, "application/json"),
        };
        req.Headers.Add("X-Requested-By", "panel");

        // Act
        var resp = await Client.SendAsync(req, ct);

        // Assert — 202 с DTO воркера (camelCase); ключ канона в etcd
        resp.StatusCode.Should().Be(HttpStatusCode.Accepted);
        var body = await resp.Content.ReadAsStringAsync(ct);
        body.Should().Contain("\"prefix\":\"ghost/s1\"");
        var raw = await GetValueAsync(OrphanRegistry.DeleteKey("ghost/s1"));
        raw.Should().NotBeNull();
        raw.Should().Contain("\"requested_by\":\"panel\"").And.Contain("\"requested_unix\":");
    }

    // AAA (AC6): повторная заявка — put поверх (202, не 409: заявка — состояние)
    [Fact]
    public async Task Delete_повторный_202_put_поверх()
    {
        // Arrange — реестр + заявка другого автора (operator)
        await CleanupAsync();
        await SeedRegistryAsync("ghost/s1");
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, OrphanRegistry.DeleteKey("ghost/s1"),
            OrphanRegistry.DeleteToJson(1757760000, "operator"), null, TestContext.Current.CancellationToken);
        var ct = TestContext.Current.CancellationToken;

        // Act — две заявки подряд
        var first = await SendDeleteAsync(ct);
        var second = await SendDeleteAsync(ct);

        // Assert — обе 202; ключ перезаписан автором panel
        first.StatusCode.Should().Be(HttpStatusCode.Accepted);
        second.StatusCode.Should().Be(HttpStatusCode.Accepted);
        var raw = await GetValueAsync(OrphanRegistry.DeleteKey("ghost/s1"));
        raw.Should().Contain("\"requested_by\":\"panel\"", "put поверх — не 409");
    }

    // AAA (AC6): confirm != "<C>/<X>" → 400; ключа заявки нет
    [Fact]
    public async Task Delete_400_confirm_мисматч()
    {
        // Arrange — реестр с сиротой
        await CleanupAsync();
        await SeedRegistryAsync("ghost/s1");
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsync("/api/backups/orphans/ghost/s1/delete",
            new StringContent("""{"confirm":"other/s1"}""", Encoding.UTF8, "application/json"), ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await GetValueAsync(OrphanRegistry.DeleteKey("ghost/s1"))).Should().BeNull();
    }

    // AAA (AC6): POST без тела → 400
    [Fact]
    public async Task Delete_400_нет_тела()
    {
        // Arrange — реестр с сиротой
        await CleanupAsync();
        await SeedRegistryAsync("ghost/s1");
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsync("/api/backups/orphans/ghost/s1/delete",
            content: null, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await GetValueAsync(OrphanRegistry.DeleteKey("ghost/s1"))).Should().BeNull();
    }

    // AAA (AC6): delete — префикса нет в реестре → 404
    [Fact]
    public async Task Delete_404_не_сирота()
    {
        // Arrange — контур чист
        await CleanupAsync();
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await SendDeleteAsync(ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // AAA (AC6): delete — запись в DELETING → 409 (доводку не остановить)
    [Fact]
    public async Task Delete_409_DELETING()
    {
        // Arrange — запись в доводке
        await CleanupAsync();
        await SeedRegistryAsync("ghost/s1", "DELETING");
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await SendDeleteAsync(ct);

        // Assert — 409; ключа заявки нет
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await GetValueAsync(OrphanRegistry.DeleteKey("ghost/s1"))).Should().BeNull();
    }

    // AAA (AC6): подсистема бэкапов выключена (_disabledFactory, default false) → 503
    [Fact]
    public async Task Hold_503_подсистема_выключена()
    {
        // Arrange — клиент выключенной фабрики (Enabled=false по умолчанию)
        await CleanupAsync();
        await SeedRegistryAsync("ghost/s1");
        using var client = _disabledFactory.CreateClient();
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await client.PostAsync("/api/backups/orphans/ghost/s1/hold", null, ct);

        // Assert — 503; ключа hold нет
        resp.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        (await GetValueAsync(OrphanRegistry.HoldKey("ghost/s1"))).Should().BeNull();
    }

    // Хелпер Act: POST hold с X-Requested-By: panel (без тела).
    private async Task<HttpResponseMessage> SendHoldAsync(CancellationToken ct)
    {
        var req = new HttpRequestMessage(HttpMethod.Post, "/api/backups/orphans/ghost/s1/hold");
        req.Headers.Add("X-Requested-By", "panel");
        return await Client.SendAsync(req, ct);
    }

    // Хелпер Act: POST delete c корректным confirm и X-Requested-By: panel.
    private async Task<HttpResponseMessage> SendDeleteAsync(CancellationToken ct)
    {
        var req = new HttpRequestMessage(HttpMethod.Post, "/api/backups/orphans/ghost/s1/delete")
        {
            Content = new StringContent("""{"confirm":"ghost/s1"}""", Encoding.UTF8, "application/json"),
        };
        req.Headers.Add("X-Requested-By", "panel");
        return await Client.SendAsync(req, ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _factory.DisposeAsync();
        await _disabledFactory.DisposeAsync();
    }
}
