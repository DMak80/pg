using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PgWorker.App;
using PgWorker.App.Api;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// mTLS HTTP API PgWorker (spec §8.2, t03): клиент без серта — отказ TLS-хендшейка,
// с сертом API-CA — 200; /healthz за тем же TLS. Реальный Kestrel-сокет (WAF-
// транспорт in-memory TLS не исполняет) — порт динамический (зонд FreePort).
// https-валидация advertise: http-URL при выключенном AllowInsecureHttp — fail-fast
// старта хоста (ValidateOnStart, arch/14 §1.1).
[Collection(PgApiCollection.Name)]
public class MtlsApiTests(PgApiFixture fx)
{
    // Локальный PKI-хелпер: CertificateRequest + RSA-2048 (паттерн TestPki из
    // AdminPanel.UnitTests/Workers/WorkerTlsHandlerTests; тесты не тянут
    // зависимость от воркерских ClusterPki).
    private static class TestPki
    {
        public static (string CaPem, string CaKeyPem) GenerateCa()
        {
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(
                "CN=test-pg-api-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            using var ca = request.CreateSelfSigned(
                DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
            return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
        }

        public static (string CertPem, string KeyPem) Issue(string caPem, string caKeyPem, string commonName)
        {
            using var caCert = X509Certificate2.CreateFromPem(caPem);
            using var caKey = RSA.Create();
            caKey.ImportFromPem(caKeyPem);
            using var caWithKey = caCert.CopyWithPrivateKey(caKey);
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(
                $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            var san = new SubjectAlternativeNameBuilder();
            san.AddDnsName(commonName);
            request.CertificateExtensions.Add(san.Build());
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
                [new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2")], critical: false));
            using var cert = request.Create(
                caWithKey, DateTimeOffset.UtcNow.AddDays(-1), caCert.NotAfter.AddMinutes(-1),
                RandomNumberGenerator.GetBytes(16));
            return (cert.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
        }
    }

    // Свободный порт на рантайме (паттерн E2eFixture.FreePort): TcpListener :0.
    private static int FreePort()
    {
        using var listener = TcpListener.Create(0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed record TlsHost(WebApplication App, HttpClient Client, X509Certificate2 ClientCert);

    private static TlsHost StartTlsHost(int port)
    {
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (serverCertPem, serverKeyPem) = TestPki.Issue(caPem, caKeyPem, "pgworker");
        var (clientCertPem, clientKeyPem) = TestPki.Issue(caPem, caKeyPem, "panel");

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddHealthChecks(); // маршрут /healthz в тесте
        builder.Configuration["PgWorker:Api:Tls:ServerCertPem"] = serverCertPem;
        builder.Configuration["PgWorker:Api:Tls:ServerKeyPem"] = serverKeyPem;
        builder.Configuration["PgWorker:Api:Tls:ClientCaPem"] = caPem;
        // Изоляция от чужих env (WAF-фабрики ставят AllowInsecureHttp=true переменной
        // процесса): явный in-memory оверрайд сильнее env-провайдера.
        builder.Configuration["PgWorker:Api:Tls:AllowInsecureHttp"] = "false";
        builder.Configuration["urls"] = $"https://localhost:{port}"; // проверка ResolvePort на реальном хосте
        ApiTlsEndpoints.ConfigureMtls(builder); // ДО Build — ConfigureKestrel этап хоста
        var app = builder.Build();
        app.MapGet("/api/ping", () => Results.Ok("pong"));
        app.MapHealthChecks("/healthz");
        app.Start();

        // PFX round-trip: эфемерный ключ CreateFromPem не годится для SslStream
        // на macOS (ре-импорт делает ключ экспортируемым).
        var pemCert = X509Certificate2.CreateFromPem(clientCertPem, clientKeyPem);
        var clientCert = X509CertificateLoader.LoadPkcs12(pemCert.Export(X509ContentType.Pkcs12), null);
        // TLS 1.2: macOS SslStream не отправляет клиентские серты в TLS 1.3
        // (dotnet/runtime#37961); прод-контур — Linux-контейнеры, ограничение
        // касается только host-прогона теста.
        var handler = new SocketsHttpHandler
        {
            SslOptions = new()
            {
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                ClientCertificates = [clientCert],
                RemoteCertificateValidationCallback = (_, _, _, _) => true, // тест доверяет фикстурной CA
            },
        };
        return new TlsHost(app, new HttpClient(handler) { BaseAddress = new Uri($"https://localhost:{port}") }, clientCert);
    }

    [Fact]
    public async Task Mtls_NoClientCert_Refused_WithCert_Ok()
    {
        // Arrange: TLS-хост на свободном порту (зонд FreePort — динамический).
        var port = FreePort();
        var host = StartTlsHost(port);
        using var _ = host.ClientCert; // серт нужен хендшейкам до конца теста
        using var app = host.App;
        // Готовность листенера: Start() вернулся, но под параллельным
        // docker-нагрузом прогона первый хендшейк изредка ловил «refused» —
        // ждём фактического приёма TCP, чтобы не маскировать это под TLS-отказ.
        var readyDeadline = DateTimeOffset.UtcNow.AddSeconds(15);
        while (true)
        {
            try
            {
                using var probe = new TcpClient();
                await probe.ConnectAsync(IPAddress.Loopback, port, TestContext.Current.CancellationToken);
                break; // порт принимает — Kestrel слушает
            }
            catch (SocketException) when (DateTimeOffset.UtcNow < readyDeadline)
            {
                await Task.Delay(200, TestContext.Current.CancellationToken);
            }
        }
        using var badClient = new HttpClient(new SocketsHttpHandler
        {
            SslOptions = new()
            {
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                RemoteCertificateValidationCallback = (_, _, _, _) => true,
            },
        }) { BaseAddress = new Uri($"https://localhost:{port}") };

        // Act 1: запрос без клиентского серта.
        var ct = TestContext.Current.CancellationToken;
        var refused = await Assert.ThrowsAnyAsync<HttpRequestException>(
            () => badClient.GetAsync("/api/ping", ct));

        // Assert 1: TLS-отказ (хендшейк не прошёл — ClientCertificateMode.Required).
        refused.Should().NotBeNull();

        // Act 2 / Assert 2: с сертом API-CA — 200; /healthz — тоже за TLS.
        (await host.Client.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.OK);
        (await host.Client.GetAsync("/healthz", ct)).StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // Оверрайд базовой фабрики: http-advertise при выключенном AllowInsecureHttp.
    private sealed class HttpAdvertiseFactory(EtcdFixture etcd) : PgWorkerApiFactory(etcd)
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["PgWorker:Api:AdvertiseUrl"] = "http://localhost:9999",
                ["PgWorker:Api:Tls:AllowInsecureHttp"] = "false",
            }));
        }
    }

    [Fact]
    public void AdvertiseUrl_HttpWithoutInsecureFlag_HostStartFails()
    {
        // Arrange: валидный http-advertise + mTLS-канон (AllowInsecureHttp=false).
        using var factory = new HttpAdvertiseFactory(fx.Etcd);

        // Act / Assert: ValidateOnStart — старт хоста падает fail-fast
        // (AdvertiseUrl обязан быть https://, arch/14 §1.1).
        Assert.ThrowsAny<Exception>(() => factory.CreateClient());
    }
}
