using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using KafkaWorker.App.Api;
using KafkaWorker.Core.Templates;
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
        TlsEndpoints.ConfigureMtls(builder, port); // ДО Build — ConfigureKestrel этап хоста
        var app = builder.Build();
        app.MapGet("/api/ping", () => Results.Ok("pong"));
        app.MapHealthChecks("/healthz");
        app.Start();

        var (clientCertPem, clientKeyPem) = ClusterPki.IssueBrokerCertificate(
            ApiCa.CaPem, ApiCa.CaKeyPem, "panel", ["panel"], ip: null);
        var clientCert = X509Certificate2.CreateFromPem(clientCertPem, clientKeyPem);
        var handler = new SocketsHttpHandler
        {
            SslOptions = new()
            {
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
        using var badClient = new HttpClient(new SocketsHttpHandler
        {
            SslOptions = new() { RemoteCertificateValidationCallback = (_, _, _, _) => true },
        }) { BaseAddress = new Uri($"https://localhost:{port}") };

        // Act 1: запрос без клиентского серта.
        var refused = await Assert.ThrowsAnyAsync<HttpRequestException>(
            () => badClient.GetAsync("/api/ping"));

        // Assert 1: TLS-отказ (хендшейк не прошёл — ClientCertificateMode.Required).
        refused.Should().NotBeNull();

        // Act 2 / Assert 2: с сертом API-CA — 200; /healthz — тоже за TLS.
        (await host.Client.GetAsync("/api/ping")).StatusCode.Should().Be(HttpStatusCode.OK);
        (await host.Client.GetAsync("/healthz")).StatusCode.Should().Be(HttpStatusCode.OK);
    }
}
