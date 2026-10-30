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

// HTTPS HTTP API (arch/16 §1.1): клиентский серт опционален на хендшейке;
// /api без серта и с невалидным сертом — 401 (HTTP-ответ, не TLS-отказ);
// /healthz и /metrics — открытая грань (200 без серта, невалидный серт
// игнорируется). Реальный Kestrel-сокет (WAF-транспорт in-memory TLS не
// исполняет) — порт динамический (зонд FreePortWindow). Клиентский серт
// возвращается НАРУЖУ (диспоз — тест).
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
        // 401-политика /api в pipeline тест-хоста (в Program.cs воркера
        // подключается так же — здесь хост собирает тест).
        app.UseMiddleware<Shared.Tls.ClientCertAuthMiddleware>();
        app.MapGet("/api/ping", () => Results.Ok("pong"));
        app.MapHealthChecks("/healthz");
        app.MapGet("/metrics", () => Results.Ok("")); // заглушка: транспортная семантика, не формат выгрузки
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
    public async Task Api_WithoutCert_401_HealthzMetrics_WithoutCert_200_InvalidCert_401_WithCert_200()
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
        using var noCertClient = new HttpClient(new SocketsHttpHandler
        {
            SslOptions = new()
            {
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                RemoteCertificateValidationCallback = (_, _, _, _) => true,
            },
        }) { BaseAddress = new Uri($"https://localhost:{port}") };
        // Невалидный клиентский серт: подписан ДРУГОЙ тестовой CA (не CA сервера).
        var badCa = ClusterPki.GenerateCa("api-test-invalid");
        var (badCertPem, badKeyPem) = ClusterPki.IssueBrokerCertificate(
            badCa.CaPem, badCa.CaKeyPem, "panel", ["panel"], ip: null);
        // PFX round-trip: эфемерный ключ CreateFromPem не годится для SslStream
        // на macOS (ре-импорт делает ключ экспортируемым).
        var badPemCert = X509Certificate2.CreateFromPem(badCertPem, badKeyPem);
        using var invalidCert = X509CertificateLoader.LoadPkcs12(badPemCert.Export(X509ContentType.Pkcs12), null);
        using var invalidClient = new HttpClient(new SocketsHttpHandler
        {
            SslOptions = new()
            {
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                ClientCertificates = [invalidCert],
                RemoteCertificateValidationCallback = (_, _, _, _) => true,
            },
        }) { BaseAddress = new Uri($"https://localhost:{port}") };

        var ct = TestContext.Current.CancellationToken;

        // Act 1 / Assert 1: /api без серта — 401 (HTTP-ответ, не TLS-отказ).
        (await noCertClient.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.Unauthorized);

        // Act 2 / Assert 2: /healthz без серта — 200 (открытая грань).
        (await noCertClient.GetAsync("/healthz", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

        // Act 3 / Assert 3: /metrics без серта — 200 (открытая грань, заглушка).
        (await noCertClient.GetAsync("/metrics", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

        // Act 4 / Assert 4: /api с сертом API-CA — 200.
        (await host.Client.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

        // Act 5 / Assert 5: /api с НЕВАЛИДНЫМ сертом — 401, хендшейк НЕ рвётся
        // (сервер принял соединение, middleware отверг путь).
        (await invalidClient.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.Unauthorized);

        // Act 6 / Assert 6: /healthz с невалидным сертом — 200 (серт игнорируется
        // открытой гранью).
        (await invalidClient.GetAsync("/healthz", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

        // Act 7 / Assert 7: /apifoo без серта — 404 (не /api: сегмент-префикс
        // точный, прочие пути — открытая грань).
        (await noCertClient.GetAsync("/apifoo", ct)).StatusCode.Should().Be(HttpStatusCode.NotFound);
    }
}
