using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using KafkaWorker.App.Api;
using KafkaWorker.Core.Templates;
using KafkaWorker.IntegrationTests.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace KafkaWorker.IntegrationTests.Api;

// mTLS HTTP API (spec §8.2): клиент без серта — отказ TLS-хендшейка, с сертом
// API-CA — 200; /healthz за тем же TLS. Реальный Kestrel-сокет (WAF-транспорт
// in-memory TLS не исполняет) — порт динамический (зонд FreePortWindow).
// Клиентский серт возвращается НАРУЖУ (без using в хелпере — серт живёт в
// SslOptions.ClientCertificates хендшейков после возврата; диспоз — тест).
public class MtlsApiTests
{
    private static readonly (string CaPem, string CaKeyPem) ApiCa = ClusterPki.GenerateCa("api-test");

    private sealed record TlsHost(WebApplication App, HttpClient Client, X509Certificate2 ClientCert);

    private static TlsHost StartTlsHost(int port)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddHealthChecks(); // маршрут /healthz в тесте
        var (serverCertPem, serverKeyPem) = ClusterPki.IssueBrokerCertificate(
            ApiCa.CaPem, ApiCa.CaKeyPem, "kafkaworker", ["localhost"], ip: null);
        builder.Configuration["KafkaWorker:Api:Tls:ServerCertPem"] = serverCertPem;
        builder.Configuration["KafkaWorker:Api:Tls:ServerKeyPem"] = serverKeyPem;
        builder.Configuration["KafkaWorker:Api:Tls:ClientCaPem"] = ApiCa.CaPem;
        // Изоляция от чужих env (KafkaApiFactory ставит AllowInsecureHttp=true
        // переменной процесса): явный in-memory оверрайд сильнее env-провайдера.
        builder.Configuration["KafkaWorker:Api:Tls:AllowInsecureHttp"] = "false";
        TlsEndpoints.ConfigureMtls(builder, port); // ДО Build — ConfigureKestrel этап хоста
        var app = builder.Build();
        app.MapGet("/api/ping", () => Results.Ok("pong"));
        app.MapHealthChecks("/healthz");
        app.Start();

        var (clientCertPem, clientKeyPem) = ClusterPki.IssueBrokerCertificate(
            ApiCa.CaPem, ApiCa.CaKeyPem, "panel", ["panel"], ip: null);
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
                RemoteCertificateValidationCallback = (_, _, _, _) => true, // тест доверяет всё
            },
        };
        return new TlsHost(app, new HttpClient(handler) { BaseAddress = new Uri($"https://localhost:{port}") }, clientCert);
    }

    [Fact]
    public async Task Mtls_NoClientCert_Refused_WithCert_Ok()
    {
        // Arrange: TLS-хост на свободном порту (зонд FreePortWindow — динамический).
        var port = FreePortWindow.Find().From;
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
}
