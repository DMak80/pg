using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using KafkaWorker.App.Api;
using KafkaWorker.Core.Templates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace KafkaWorker.IntegrationTests.Api;

// Чтение ключа /workers/api_tls/kafkaworker при старте (spec §3.2 п.1,
// симметрично PgWorker): Found/Missing/Unreachable/Broken + применение в
// ConfigureMtls (серт грани из ключа; fail-fast на битый; env-фоллбек).
[Collection(KafkaApiCollection.Name)]
public class WorkerApiCertStartupTests(KafkaApiFixture fx)
{
    private string Endpoints => fx.Etcd.Endpoint;

    private async Task PutKeyAsync(string json)
        => await fx.Etcd.Gateway.PutAsync(
            Endpoints, "/workers/api_tls/kafkaworker", json, null, TestContext.Current.CancellationToken);

    // Предочистка/посточистка ключа серта: тесты класса живут в общем etcd
    // фикстуры — чистим свой ключ при любом исходе, чтобы тесты не зависели
    // от порядка прогона и не оставляли мусора соседям.
    private async Task DeleteKeyAsync()
        => await fx.Etcd.Gateway.DeleteAsync(
            Endpoints, "/workers/api_tls/kafkaworker", prefix: false, TestContext.Current.CancellationToken);

    [Fact]
    public async Task Read_KeyMissing_Missing()
    {
        // Arrange: ключа нет (чистим свой ключ фикстуры)
        await DeleteKeyAsync();
        // Act
        var read = await WorkerApiCertReader.ReadAsync([Endpoints], "kafkaworker", TestContext.Current.CancellationToken);
        // Assert: env-фоллбек по правилам §3.2 п.1
        read.Status.Should().Be(ManagedCertStatus.Missing);
    }

    [Fact]
    public async Task Read_ValidPair_Found()
    {
        // Arrange
        await DeleteKeyAsync();
        var (caPem, caKeyPem) = ClusterPki.GenerateCa("cert-startup");
        var (certPem, keyPem) = ClusterPki.IssueBrokerCertificate(caPem, caKeyPem, "kafkaworker-api", ["localhost"], null);
        await PutKeyAsync(JsonSerializer.Serialize(new { cert_pem = certPem, key_pem = keyPem }));
        try
        {
            // Act
            var read = await WorkerApiCertReader.ReadAsync([Endpoints], "kafkaworker", TestContext.Current.CancellationToken);
            // Assert
            read.Status.Should().Be(ManagedCertStatus.Found);
            read.CertPem.Should().Be(certPem);
        }
        finally
        {
            await DeleteKeyAsync();
        }
    }

    [Fact]
    public async Task Read_BrokenJson_Broken()
    {
        // Arrange: JSON-мусор — fail-fast-ветка старта
        await DeleteKeyAsync();
        await PutKeyAsync("{not-json");
        try
        {
            // Act
            var read = await WorkerApiCertReader.ReadAsync([Endpoints], "kafkaworker", TestContext.Current.CancellationToken);
            // Assert
            read.Status.Should().Be(ManagedCertStatus.Broken);
            read.Error.Should().NotBeNullOrEmpty();
        }
        finally
        {
            await DeleteKeyAsync();
        }
    }

    [Fact]
    public async Task Read_DeadEndpoint_Unreachable()
    {
        // Arrange: ключ чист; мёртвый порт (зонд свободного → никто не слушает)
        await DeleteKeyAsync();
        var port = Etcd.EtcdFixture.ReserveHostPort();
        // Act
        var read = await WorkerApiCertReader.ReadAsync([$"http://localhost:{port}"], "kafkaworker", TestContext.Current.CancellationToken);
        // Assert: env-фоллбек с warning при живом env-серте
        read.Status.Should().Be(ManagedCertStatus.Unreachable);
    }

    [Fact]
    public async Task ConfigureMtls_ManagedFound_ServerCertFromEtcd()
    {
        // Arrange: ключ с валидной парой в etcd; env-серт ДРУГОЙ (тот же Issue);
        // клиентский серт той же CA (грань — mTLS, клиент обязан предъявить серт)
        await DeleteKeyAsync();
        var (caPem, caKeyPem) = ClusterPki.GenerateCa("cert-startup-apply");
        var (etdCert, etdKey) = ClusterPki.IssueBrokerCertificate(caPem, caKeyPem, "kafkaworker-etcd", ["localhost"], null);
        var (clientCertPem, clientKeyPem) = ClusterPki.IssueBrokerCertificate(caPem, caKeyPem, "panel", ["panel"], null);
        await PutKeyAsync(JsonSerializer.Serialize(new { cert_pem = etdCert, key_pem = etdKey }));
        var read = await WorkerApiCertReader.ReadAsync([Endpoints], "kafkaworker", TestContext.Current.CancellationToken);
        var port = FreePort();

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddHealthChecks();
        builder.Configuration["KafkaWorker:Api:Tls:ServerCertPem"] = etdCert.Replace("X", "Z"); // env-мусор — источник etcd обязан победить
        builder.Configuration["KafkaWorker:Api:Tls:ServerKeyPem"] = etdKey;
        builder.Configuration["KafkaWorker:Api:Tls:ClientCaPem"] = caPem;
        builder.Configuration["KafkaWorker:Api:Tls:AllowInsecureHttp"] = "false";
        var setup = TlsEndpoints.ConfigureMtls(builder, port, read);
        var app = builder.Build();
        app.MapGet("/api/ping", () => Results.Ok("pong"));
        app.Start();

        try
        {
            // Act: клиент доверяет серверу ТОЛЬКО по thumbprint серта из ключа
            using var client = TlsClientTrustThumbprint(etdCert, port, clientCertPem, clientKeyPem);
            using var response = await client.GetAsync("/api/ping", TestContext.Current.CancellationToken);

            // Assert: грань поднята на etcd-серте, источник — etcd
            response.StatusCode.Should().Be(HttpStatusCode.OK);
            setup.Source.Should().Be("etcd:/workers/api_tls/kafkaworker");
            setup.ServerCert.Should().NotBeNull();
        }
        finally
        {
            await app.StopAsync(TestContext.Current.CancellationToken);
            await DeleteKeyAsync();
        }
    }

    [Fact]
    public void ConfigureMtls_ManagedBroken_FailFast()
    {
        // Arrange: ключ бит — явное намерение оператора, тихий fallback запрещён
        var read = new ManagedCertRead(ManagedCertStatus.Broken, null, null, "битый JSON");
        var builder = WebApplication.CreateBuilder();
        builder.Configuration["KafkaWorker:Api:Tls:AllowInsecureHttp"] = "false";

        // Act / Assert
        Assert.ThrowsAny<Exception>(() => TlsEndpoints.ConfigureMtls(builder, 8080, read));
    }

    [Fact]
    public void ConfigureMtls_UnreachableWithEnv_EnvSourceAndWarning()
    {
        // Arrange: etcd недоступен + валидный env-серт → старт на env + warning
        var (caPem, caKeyPem) = ClusterPki.GenerateCa("cert-startup-env");
        var (certPem, keyPem) = ClusterPki.IssueBrokerCertificate(caPem, caKeyPem, "kafkaworker-env", ["localhost"], null);
        var builder = WebApplication.CreateBuilder();
        builder.Configuration["KafkaWorker:Api:Tls:ServerCertPem"] = certPem;
        builder.Configuration["KafkaWorker:Api:Tls:ServerKeyPem"] = keyPem;
        builder.Configuration["KafkaWorker:Api:Tls:ClientCaPem"] = caPem;
        builder.Configuration["KafkaWorker:Api:Tls:AllowInsecureHttp"] = "false";
        var read = new ManagedCertRead(ManagedCertStatus.Unreachable, null, null, "etcd недоступен");

        // Act
        var setup = TlsEndpoints.ConfigureMtls(builder, 8080, read);

        // Assert
        setup.Source.Should().Be("env");
        setup.Warning.Should().NotBeNullOrEmpty();
        setup.ServerCert.Should().NotBeNull();
    }

    // Свободный порт на рантайме (паттерн MtlsApiTests.FreePort): TcpListener :0.
    private static int FreePort()
    {
        using var listener = TcpListener.Create(0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    // TLS-клиент: сервер доверен ТОЛЬКО по sha256-thumbprint эталонного серта
    // (проверка «факт на грани == ожидаемый»), клиентский серт — та же CA
    // (грань mTLS). TLS 1.2 (macOS SslStream не шлёт клиентские серты в 1.3
    // — dotnet/runtime#37961).
    private static HttpClient TlsClientTrustThumbprint(string certPem, int port, string clientCertPem, string clientKeyPem)
    {
        using var expected = X509Certificate2.CreateFromPem(certPem);
        var expectedHash = Convert.ToHexString(SHA256.HashData(expected.GetRawCertData()));
        // PFX round-trip: эфемерный ключ CreateFromPem не годится для SslStream на macOS.
        var pemCert = X509Certificate2.CreateFromPem(clientCertPem, clientKeyPem);
        var clientCert = X509CertificateLoader.LoadPkcs12(pemCert.Export(X509ContentType.Pkcs12), null);
        var handler = new SocketsHttpHandler
        {
            SslOptions = new()
            {
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                ClientCertificates = [clientCert],
                RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                    certificate is not null
                    && Convert.ToHexString(SHA256.HashData(certificate.GetRawCertData())) == expectedHash,
            },
        };
        return new HttpClient(handler) { BaseAddress = new Uri($"https://localhost:{port}") };
    }
}
