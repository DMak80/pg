using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ValkeyWorker.App.Api;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Api;

// mTLS HTTP API (arch/21 §1.1; порт MtlsApiTests kfw): клиент без серта —
// отказ TLS-хендшейка, с сертом API-CA — 200; /healthz за тем же TLS.
// Реальный Kestrel-сокет (WAF-транспорт in-memory TLS не исполняет) — порт
// динамический (зонд). Клиентский серт живёт в SslOptions хендшейков.
public class MtlsApiTests
{
    private static readonly (string CaPem, string CaKeyPem) ApiCa = TestPki.GenerateCa("api-test");

    private sealed record TlsHost(WebApplication App, HttpClient Client, X509Certificate2 ClientCert);

    private static TlsHost StartTlsHost(int port)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddHealthChecks();
        var (serverCertPem, serverKeyPem) = TestPki.IssueCertificate(
            ApiCa.CaPem, ApiCa.CaKeyPem, "valkeyworker");
        builder.Configuration["ValkeyWorker:Api:Tls:ServerCertPem"] = serverCertPem;
        builder.Configuration["ValkeyWorker:Api:Tls:ServerKeyPem"] = serverKeyPem;
        builder.Configuration["ValkeyWorker:Api:Tls:ClientCaPem"] = ApiCa.CaPem;
        // Изоляция от чужих env (фабрики ставят AllowInsecureHttp=true
        // переменной процесса): явный in-memory оверрайд сильнее env.
        builder.Configuration["ValkeyWorker:Api:Tls:AllowInsecureHttp"] = "false";
        TlsEndpoints.ConfigureMtls(builder, port); // ДО Build — ConfigureKestrel этап хоста
        var app = builder.Build();
        app.MapGet("/api/ping", () => Results.Ok("pong"));
        app.MapHealthChecks("/healthz");
        app.Start();

        var (clientCertPem, clientKeyPem) = TestPki.IssueCertificate(
            ApiCa.CaPem, ApiCa.CaKeyPem, "panel");
        // PFX round-trip: эфемерный ключ CreateFromPem не годится для SslStream
        // на macOS (ре-импорт делает ключ экспортируемым).
        var pemCert = X509Certificate2.CreateFromPem(clientCertPem, clientKeyPem);
        var clientCert = X509CertificateLoader.LoadPkcs12(pemCert.Export(X509ContentType.Pkcs12), null);
        // TLS 1.2: macOS SslStream не отправляет клиентские серты в TLS 1.3.
        var handler = new SocketsHttpHandler
        {
            SslOptions = new()
            {
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                ClientCertificates = [clientCert],
                RemoteCertificateValidationCallback = (_, _, _, _) => true,
            },
        };
        return new TlsHost(app, new HttpClient(handler) { BaseAddress = new Uri($"https://localhost:{port}") }, clientCert);
    }

    private static int FreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    [Fact]
    public async Task Mtls_БезКлиентСертаОтказ_ССертомОк()
    {
        // Arrange: TLS-хост на свободном порту (динамический).
        var port = FreePort();
        var host = StartTlsHost(port);
        using var _ = host.ClientCert;
        using var app = host.App;
        // Готовность листенера: ждём фактического приёма TCP (параллельный
        // docker-нагрузк прогона может дать «refused» на первом хендшейке).
        var readyDeadline = DateTimeOffset.UtcNow.AddSeconds(15);
        while (true)
        {
            try
            {
                using var probe = new TcpClient();
                await probe.ConnectAsync(IPAddress.Loopback, port, TestContext.Current.CancellationToken);
                break;
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

        // Assert 1: TLS-отказ (ClientCertificateMode.Required).
        refused.Should().NotBeNull();

        // Act 2 / Assert 2: с сертом API-CA — 200; /healthz — тоже за TLS.
        (await host.Client.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.OK);
        (await host.Client.GetAsync("/healthz", ct)).StatusCode.Should().Be(HttpStatusCode.OK);
    }
}
