using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using AdminPanel.Etcd.Workers;
using Xunit;
using FluentAssertions;
using Microsoft.Extensions.Configuration;

namespace AdminPanel.UnitTests.Workers;

// WorkerTlsHandler (t03, arch/02 §2.3.2): клиентский серт панели — ЕДИНЫЙ на оба
// воркера (pgworker и kafkaworker — одна per-install API-CA, t03-pg); доверие
// ServerCA в SocketsHttpHandler; env-маппинг WORKERS_PANEL_TLS_*; пустые опции —
// plain handler (dev/локальные http://-вызовы).
public class WorkerTlsHandlerTests
{
    [Fact]
    public void Build_ClientCertAndServerCa_SocketsHandlerTlsOptions()
    {
        // Arrange: тестовые PEM (CertificateRequest прямо в тесте — панельные
        // тесты не тянут зависимость от KafkaWorker.Core) + опции.
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (certPem, keyPem) = TestPki.Issue(caPem, caKeyPem, "panel");
        var tls = new WorkerTlsOptions { ClientCertPem = certPem, ClientKeyPem = keyPem, ServerCaPem = caPem };

        // Act: сборка handler'а.
        var handler = WorkerTlsHandler.Build(tls) as SocketsHttpHandler;

        // Assert: клиентский серт подан, колбэк доверия установлен.
        handler.Should().NotBeNull();
        handler!.SslOptions.ClientCertificates.Should().NotBeNull();
        handler.SslOptions.RemoteCertificateValidationCallback.Should().NotBeNull();
    }

    [Fact]
    public void Build_NoTls_PlainSocketsHandler()
    {
        // Arrange: пустые опции (dev/локальные вызовы без сертов).
        // Act / Assert: handler без TLS-настроек (http://-вызовы работают).
        (WorkerTlsHandler.Build(new WorkerTlsOptions()) as SocketsHttpHandler)!
            .SslOptions.ClientCertificates.Should().BeNull();
    }

    [Fact]
    public void ApplyEnvOverrides_PanelTlsKeysMapped()
    {
        // Arrange: env-словарь (inject, без окружения).
        var env = new Dictionary<string, string>
        {
            ["WORKERS_PANEL_TLS_CERT_PATH"] = "/tls-workers/panel.crt",
            ["WORKERS_PANEL_TLS_SERVER_CA_PATH"] = "/tls-workers/ca.pem",
        };
        var config = new ConfigurationManager();

        // Act.
        WorkerTlsHandler.ApplyEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert: ключи легли в AdminPanel:Workers:WorkerTls:*; таблица — 6 записей.
        config["AdminPanel:Workers:WorkerTls:ClientCertPath"].Should().Be("/tls-workers/panel.crt");
        config["AdminPanel:Workers:WorkerTls:ServerCaPath"].Should().Be("/tls-workers/ca.pem");
        WorkerTlsHandler.EnvBindings.Should().HaveCount(6);
    }

    // Локальный PKI-хелпер: GenerateCa + Issue по образцу ClusterPki воркера
    // (internal: переиспользуется тестами thumbprint-доверия ниже).
    internal static class TestPki
    {
        public static (string CaPem, string CaKeyPem) GenerateCa()
        {
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(
                "CN=test-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            using var ca = request.CreateSelfSigned(
                DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
            return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
        }

        public static (string CertPem, string KeyPem) Issue(
            string caPem, string caKeyPem, string commonName)
        {
            using var ca = X509CertificateLoader.LoadCertificate(PemBody(caPem));
            using var caKey = RSA.Create();
            caKey.ImportFromPem(caKeyPem);
            using var leafKey = RSA.Create(2048);
            var request = new CertificateRequest(
                $"CN={commonName}", leafKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            using var caWithKey = ca.CopyWithPrivateKey(caKey);
            var notAfter = DateTimeOffset.UtcNow.AddDays(30);
            if (notAfter > caWithKey.NotAfter)
                notAfter = caWithKey.NotAfter; // кламп к CA: UtcNow+30d мог уйти за срок CA
            using var cert = request.Create(
                caWithKey, DateTimeOffset.UtcNow.AddDays(-1), notAfter,
                RandomNumberGenerator.GetBytes(8));
            return (cert.ExportCertificatePem(), leafKey.ExportPkcs8PrivateKeyPem());
        }

        // Self-signed лист БЕЗ CA (проверка thumbprint-доверия, spec §3.3 п.3).
        public static (string CertPem, string KeyPem) IssueSelfSigned()
        {
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(
                "CN=self-signed-worker", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            var san = new SubjectAlternativeNameBuilder();
            san.AddDnsName("localhost");
            san.AddIpAddress(System.Net.IPAddress.Loopback);
            request.CertificateExtensions.Add(san.Build());
            using var cert = request.CreateSelfSigned(
                DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
            return (cert.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
        }

        private static byte[] PemBody(string pem)
        {
            var begin = pem.IndexOf("-----\n", StringComparison.Ordinal) + 6;
            var end = pem.LastIndexOf("-----END", StringComparison.Ordinal);
            var base64 = string.Concat(pem[begin..end].Where(c => !char.IsWhiteSpace(c)));
            return Convert.FromBase64String(base64);
        }
    }
}

// ===== Thumbprint-доверие: реальный TLS-хендшейк против локального SslStream-сервера =====

public class WorkerTlsHandlerThumbprintTests
{
    // Локальный TLS-сервер: один хендшейк с сертом serverPem (self-signed), ответ 200.
    private static async Task<int> ServeOnceAsync(string certPem, string keyPem, int port)
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, port);
        listener.Start();
        var pem = System.Security.Cryptography.X509Certificates.X509Certificate2.CreateFromPem(certPem, keyPem);
        var serverCert = System.Security.Cryptography.X509Certificates.X509CertificateLoader.LoadPkcs12(
            pem.Export(System.Security.Cryptography.X509Certificates.X509ContentType.Pkcs12), null);
        using var client = await listener.AcceptTcpClientAsync(TestContext.Current.CancellationToken);
        using var ssl = new SslStream(client.GetStream(), leaveInnerStreamOpen: false);
        await ssl.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
        {
            ServerCertificate = serverCert,
            ClientCertificateRequired = false,
            EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
        });
        // Прочитать заголовки запроса, ответить 200 и закрыть.
        var buffer = new byte[4096];
        _ = await ssl.ReadAsync(buffer);
        var response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"u8.ToArray();
        await ssl.WriteAsync(response);
        listener.Stop();
        return port;
    }

    private static int FreePort()
    {
        using var probe = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        probe.Start();
        var port = ((System.Net.IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    [Fact]
    public async Task Build_SelfSignedWithTrustedThumbprint_HandshakeOk()
    {
        // Arrange: серверный self-signed лист ВНЕ ServerCa (своя RSA-пара);
        // доверие — thumbprint этого листа (spec §3.3 п.3: панель доверяет
        // сертам, которые сама записала)
        var (certPem, keyPem) = WorkerTlsHandlerTests.TestPki.IssueSelfSigned();
        using var serverCert = System.Security.Cryptography.X509Certificates.X509Certificate2.CreateFromPem(certPem);
        var thumb = Convert.ToHexString(SHA256.HashData(serverCert.RawData)).ToLowerInvariant();
        var port = FreePort();
        var serverTask = ServeOnceAsync(certPem, keyPem, port);

        // Act: handler с доверенным thumbprint → GET за TLS
        using var handler = WorkerTlsHandler.Build(new WorkerTlsOptions(), () => [thumb]);
        using var client = new HttpClient(handler) { BaseAddress = new Uri($"https://127.0.0.1:{port}") };
        var response = await client.GetAsync("/", TestContext.Current.CancellationToken);

        // Assert: хендшейк успешен, запрос прошёл
        response.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);
        await serverTask;
    }

    [Fact]
    public async Task Build_UnknownSelfSigned_HandshakeRefused()
    {
        // Arrange: тот же серверный серт, но доверие-колбэк отдаёт ДРУГОЙ thumbprint
        var (certPem, keyPem) = WorkerTlsHandlerTests.TestPki.IssueSelfSigned();
        var port = FreePort();
        var serverTask = ServeOnceAsync(certPem, keyPem, port);
        var otherThumb = new string('f', 64);

        // Act: колбэк доверяет только «другой» отпечаток
        using var handler = WorkerTlsHandler.Build(new WorkerTlsOptions(), () => [otherThumb]);
        using var client = new HttpClient(handler) { BaseAddress = new Uri($"https://127.0.0.1:{port}") };

        // Assert: TLS-хендшейк отказ (сертификат отвергнут колбэком)
        var act = async () => await client.GetAsync("/", TestContext.Current.CancellationToken);
        (await Assert.ThrowsAnyAsync<System.Net.Http.HttpRequestException>(act)).Should().NotBeNull();
        await serverTask;
    }
}
