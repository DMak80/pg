using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
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

// Чтение ключа /workers/api_tls/pgworker при старте (spec §3.2 п.1):
// Found/Missing/Unreachable/Broken + применение в ConfigureMtls (серт грани
// из ключа; fail-fast на битый; env-фоллбек при недоступном etcd).
[Collection(PgApiCollection.Name)]
public class WorkerApiCertStartupTests(PgApiFixture fx)
{
    private string Endpoints => fx.Etcd.Endpoint;

    private async Task PutKeyAsync(string json)
        => await fx.Etcd.Gateway.PutAsync(
            Endpoints, "/workers/api_tls/pgworker", json, null, TestContext.Current.CancellationToken);

    // Предочистка/посточистка ключа серта: тесты класса живут в общем etcd
    // фикстуры — чистим свой ключ при любом исходе, чтобы тесты не зависели
    // от порядка прогона и не оставляли мусора соседям.
    private async Task DeleteKeyAsync()
        => await fx.Etcd.Gateway.DeleteAsync(
            Endpoints, "/workers/api_tls/pgworker", prefix: false, TestContext.Current.CancellationToken);

    [Fact]
    public async Task Read_KeyMissing_Missing()
    {
        // Arrange: ключа нет (чистим свой ключ фикстуры)
        await DeleteKeyAsync();
        // Act
        var read = await WorkerApiCertReader.ReadAsync([Endpoints], "pgworker", TestContext.Current.CancellationToken);
        // Assert: env-фоллбек по правилам §3.2 п.1
        read.Status.Should().Be(ManagedCertStatus.Missing);
    }

    [Fact]
    public async Task Read_ValidPair_Found()
    {
        // Arrange
        await DeleteKeyAsync();
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (certPem, keyPem) = TestPki.Issue(caPem, caKeyPem, "pgworker-api");
        await PutKeyAsync(JsonSerializer.Serialize(new { cert_pem = certPem, key_pem = keyPem }));
        try
        {
            // Act
            var read = await WorkerApiCertReader.ReadAsync([Endpoints], "pgworker", TestContext.Current.CancellationToken);
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
            var read = await WorkerApiCertReader.ReadAsync([Endpoints], "pgworker", TestContext.Current.CancellationToken);
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
        // Arrange: ключ чист (этот тест бьёт в мёртвый endpoint, префикс не читается)
        await DeleteKeyAsync();
        var port = EtcdFixture.ReserveHostPort();
        // Act
        var read = await WorkerApiCertReader.ReadAsync([$"http://localhost:{port}"], "pgworker", TestContext.Current.CancellationToken);
        // Assert: env-фоллбек с warning при живом env-серте
        read.Status.Should().Be(ManagedCertStatus.Unreachable);
    }

    [Fact]
    public async Task ConfigureMtls_ManagedFound_ServerCertFromEtcd()
    {
        // Arrange: ключ с валидной парой в etcd; env-серт ДРУГОЙ (тот же метод Issue);
        // клиентский серт той же CA (грань — mTLS, клиент обязан предъявить серт)
        await DeleteKeyAsync();
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (etdCert, etdKey) = TestPki.Issue(caPem, caKeyPem, "pgworker-etcd");
        var (clientCertPem, clientKeyPem) = TestPki.Issue(caPem, caKeyPem, "panel");
        await PutKeyAsync(JsonSerializer.Serialize(new { cert_pem = etdCert, key_pem = etdKey }));
        var read = await WorkerApiCertReader.ReadAsync([Endpoints], "pgworker", TestContext.Current.CancellationToken);
        var port = FreePort();

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddHealthChecks();
        builder.Configuration["PgWorker:Api:Tls:ServerCertPem"] = etdCert.Replace("X", "Z"); // env-мусор — источник etcd обязан победить
        builder.Configuration["PgWorker:Api:Tls:ServerKeyPem"] = etdKey;
        builder.Configuration["PgWorker:Api:Tls:ClientCaPem"] = caPem;
        builder.Configuration["PgWorker:Api:Tls:AllowInsecureHttp"] = "false";
        builder.Configuration["urls"] = $"https://localhost:{port}";
        var setup = ApiTlsEndpoints.ConfigureMtls(builder, read);
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
            setup.Source.Should().Be("etcd:/workers/api_tls/pgworker");
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
        builder.Configuration["PgWorker:Api:Tls:AllowInsecureHttp"] = "false";

        // Act / Assert
        Assert.ThrowsAny<Exception>(() => ApiTlsEndpoints.ConfigureMtls(builder, read));
    }

    [Fact]
    public void ConfigureMtls_UnreachableWithEnv_EnvSourceAndWarning()
    {
        // Arrange: etcd недоступен + валидный env-серт → старт на env + warning
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (certPem, keyPem) = TestPki.Issue(caPem, caKeyPem, "pgworker-env");
        var builder = WebApplication.CreateBuilder();
        builder.Configuration["PgWorker:Api:Tls:ServerCertPem"] = certPem;
        builder.Configuration["PgWorker:Api:Tls:ServerKeyPem"] = keyPem;
        builder.Configuration["PgWorker:Api:Tls:ClientCaPem"] = caPem;
        builder.Configuration["PgWorker:Api:Tls:AllowInsecureHttp"] = "false";
        var read = new ManagedCertRead(ManagedCertStatus.Unreachable, null, null, "etcd недоступен");

        // Act
        var setup = ApiTlsEndpoints.ConfigureMtls(builder, read);

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

    // Локальный PKI-хелпер: CertificateRequest + RSA-2048 (копия вложенного
    // TestPki из MtlsApiTests того же каталога).
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
}
