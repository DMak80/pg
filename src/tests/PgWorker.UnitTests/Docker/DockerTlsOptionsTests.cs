using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using PgWorker.Docker.Engine;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// TLS к Engine API (arch/14 §2.2.1, t03): env-биндинги, сборка handler'а с
// клиентским сертом и доверием docker-CA, fail-fast частичной конфигурации,
// unix:// игнорирует TLS, plaintext tcp:// без TLS остаётся рабочим (R15).
public class DockerTlsOptionsTests
{
    [Fact]
    public void ApplyEnvOverrides_DockerTlsKeysMapped()
    {
        // Arrange: env-словарь (inject, без окружения).
        var env = new Dictionary<string, string>
        {
            ["PGW_DOCKER_TLS_CA"] = "ca-pem",
            ["PGW_DOCKER_TLS_CERT_PATH"] = "/tls/pgworker-docker.crt",
        };
        var config = new ConfigurationManager();

        // Act
        DockerTlsOptions.ApplyEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert: ключи легли в PgWorker:Docker:Tls:*; таблица — 6 записей.
        config["PgWorker:Docker:Tls:CaPem"].Should().Be("ca-pem");
        config["PgWorker:Docker:Tls:ClientCertPath"].Should().Be("/tls/pgworker-docker.crt");
        DockerTlsOptions.EnvBindings.Should().HaveCount(6);
    }

    [Fact]
    public void Factory_TcpWithTls_ClientCertAndChainCallbackSet()
    {
        // Arrange: фикстурная docker-CA + клиентская пара (локальный TestPki).
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (certPem, keyPem) = TestPki.Issue(caPem, caKeyPem, "pgworker");
        var factory = new DockerEngineFactory(new DockerTlsOptions
        {
            CaPem = caPem, ClientCertPem = certPem, ClientKeyPem = keyPem,
        });

        // Act: транспортный handler tcp-эндпоинта.
        var handler = factory.CreateHandler("tcp://host1:2376") as SocketsHttpHandler;

        // Assert: клиентский серт подан, колбэк доверия цепочки установлен
        // (паттерн WorkerTlsHandlerTests).
        handler.Should().NotBeNull();
        handler!.SslOptions.ClientCertificates.Should().NotBeNull();
        handler.SslOptions.ClientCertificates!.Count.Should().BePositive();
        handler.SslOptions.RemoteCertificateValidationCallback.Should().NotBeNull();
    }

    [Fact]
    public void Factory_PartialTlsConfig_FailFast()
    {
        // Arrange: CA задан, клиентская пара — нет (частичная конфигурация).
        var (caPem, _) = TestPki.GenerateCa();

        // Act / Assert: ошибка старта фабрики (spec §5.1), а не молчаливый plaintext.
        var ex = Assert.Throws<ApplicationException>(() =>
            new DockerEngineFactory(new DockerTlsOptions { CaPem = caPem }));
        ex.Message.Should().Contain("PgWorker:Docker:Tls");
    }

    [Fact]
    public void Factory_NoTls_PlainTcpHandlerWithoutSslOptions()
    {
        // Arrange / Act: фабрика без TLS-конфигурации (dev/тесты, R15).
        var handler = new DockerEngineFactory().CreateHandler("tcp://host1:2375") as SocketsHttpHandler;

        // Assert: plaintext-путь не сломан.
        handler!.SslOptions.ClientCertificates.Should().BeNull();
    }

    [Fact]
    public void Factory_UnixEndpoint_TlsIgnored()
    {
        // Arrange: TLS задан, но endpoint — unix-сокет.
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (certPem, keyPem) = TestPki.Issue(caPem, caKeyPem, "pgworker");
        var factory = new DockerEngineFactory(new DockerTlsOptions
        {
            CaPem = caPem, ClientCertPem = certPem, ClientKeyPem = keyPem,
        });

        // Act
        var handler = factory.CreateHandler("unix:///var/run/docker.sock") as SocketsHttpHandler;

        // Assert: unix-транспорт — без TLS (сокет локальный, arch/14 §2.2).
        handler!.SslOptions.ClientCertificates.Should().BeNull();
    }

    // Локальный PKI-хелпер: CertificateRequest + RSA-2048 (паттерн TestPki из
    // AdminPanel.UnitTests/Workers/WorkerTlsHandlerTests.cs:64-100).
    private static class TestPki
    {
        public static (string CaPem, string CaKeyPem) GenerateCa()
        {
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(
                "CN=test-docker-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
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
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
                [new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2")], critical: false));
            using var cert = request.Create(
                caWithKey, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1),
                RandomNumberGenerator.GetBytes(16));
            return (cert.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
        }
    }
}
