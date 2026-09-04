using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using AdminPanel.Etcd.Workers;
using Xunit;
using FluentAssertions;
using Microsoft.Extensions.Configuration;

namespace AdminPanel.UnitTests.Workers;

// WorkerTlsHandler (t03, arch/02 §2.3.2): клиентский серт + доверие ServerCA в
// SocketsHttpHandler; env-маппинг KFW_PANEL_TLS_*; пустые опции — plain handler
// (http:// к PgWorker не затронут).
public class WorkerTlsHandlerTests
{
    [Fact]
    public void Build_ClientCertAndServerCa_SocketsHandlerTlsOptions()
    {
        // Arrange: тестовые PEM (CertificateRequest прямо в тесте — панельные
        // тесты не тянут зависимость от KafkaWorker.Core) + опции.
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (certPem, keyPem) = TestPki.Issue(caPem, caKeyPem, "panel");
        var tls = new KafkaTlsOptions { ClientCertPem = certPem, ClientKeyPem = keyPem, ServerCaPem = caPem };

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
        // Arrange: пустые опции (pg-only конфигурация).
        // Act / Assert: handler без TLS-настроек (http к pgworker работает).
        (WorkerTlsHandler.Build(new KafkaTlsOptions()) as SocketsHttpHandler)!
            .SslOptions.ClientCertificates.Should().BeNull();
    }

    [Fact]
    public void ApplyEnvOverrides_PanelTlsKeysMapped()
    {
        // Arrange: env-словарь (inject, без окружения).
        var env = new Dictionary<string, string>
        {
            ["KFW_PANEL_TLS_CERT_PATH"] = "/tls/panel.crt",
            ["KFW_PANEL_TLS_SERVER_CA_PATH"] = "/tls/ca.pem",
        };
        var config = new ConfigurationManager();

        // Act.
        WorkerTlsHandler.ApplyEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert: ключи легли в AdminPanel:Workers:KafkaTls:*; таблица — 6 записей.
        config["AdminPanel:Workers:KafkaTls:ClientCertPath"].Should().Be("/tls/panel.crt");
        config["AdminPanel:Workers:KafkaTls:ServerCaPath"].Should().Be("/tls/ca.pem");
        WorkerTlsHandler.EnvBindings.Should().HaveCount(6);
    }

    // Локальный PKI-хелпер: GenerateCa + Issue по образцу ClusterPki воркера.
    private static class TestPki
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
            using var cert = request.Create(
                ca.CopyWithPrivateKey(caKey), DateTimeOffset.UtcNow.AddDays(-1),
                DateTimeOffset.UtcNow.AddDays(30), RandomNumberGenerator.GetBytes(8));
            return (cert.ExportCertificatePem(), leafKey.ExportPkcs8PrivateKeyPem());
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
