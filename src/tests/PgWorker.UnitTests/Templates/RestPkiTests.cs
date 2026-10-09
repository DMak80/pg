using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using PgWorker.Core.Templates;

namespace PgWorker.UnitTests.Templates;

// PKI REST-эндпоинтов нод (arch/14 §2.1/§2.4): серты нод из per-install CA,
// SAN = DNS <n> + DNS pgw-<C>-<X>-<n> + IP 127.0.0.1, EKU ServerAuth.
public class RestPkiTests
{
    [Fact]
    public void IssueNodeCertificate_SanCoversShortAndFullNodeNamesAndLoopback()
    {
        // Arrange: per-install CA (RSA-2048 self-signed, тестовый образец
        // E2eTestPki.GenerateCa-механики).
        var (caPem, caKeyPem) = TestRestCa.Generate();

        // Act: выпуск серта REST-эндпоинта ноды shard1a шарда shard1 кластера c1.
        var (certPem, keyPem) = RestPki.IssueNodeCertificate(
            caPem, caKeyPem, nodeName: "shard1a", nodeFullName: "pgw-c1-shard1-shard1a");

        // Assert: SAN несёт короткий alias, полное docker-имя и loopback IP;
        // CN = полное имя ноды; ключ — PKCS#8 PEM.
        using var cert = X509Certificate2.CreateFromPem(certPem);
        cert.Subject.Should().Be("CN=pgw-c1-shard1-shard1a");
        var san = cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().Single();
        san.EnumerateDnsNames().Should().Contain(["shard1a", "pgw-c1-shard1-shard1a"]);
        san.EnumerateIPAddresses().Should().Contain(IPAddress.Loopback);
        keyPem.Should().StartWith("-----BEGIN PRIVATE KEY-----");
    }

    [Fact]
    public void IssueNodeCertificate_EkuServerAuthOnly()
    {
        // Arrange: тестовый CA.
        var (caPem, caKeyPem) = TestRestCa.Generate();

        // Act: выпуск серта.
        var (certPem, _) = RestPki.IssueNodeCertificate(caPem, caKeyPem, "n1", "pgw-c1-s1-n1");

        // Assert: EKU — ровно ServerAuth (клиентские серты REST-грани не
        // выпускаются — mTLS не используется, arch/02 §2 п.8); ClientAuth
        // отсутствует. DER-кодировка EKU-набора детерминирована — сравнение
        // с эталонным расширением из одного OID.
        using var cert = X509Certificate2.CreateFromPem(certPem);
        var eku = cert.Extensions.OfType<X509EnhancedKeyUsageExtension>().Single();
        var serverAuthOnly = new X509EnhancedKeyUsageExtension(
            [new Oid("1.3.6.1.5.5.7.3.1")], critical: false);
        eku.RawData.SequenceEqual(serverAuthOnly.RawData).Should().BeTrue();
    }

    [Fact]
    public void IssueNodeCertificate_NotAfterClampedToCa()
    {
        // Arrange: CA с КОРОТКИМ сроком (30 дней) — срок серта обязан
        // зажиматься в NotAfter CA (валидация issuer).
        var (caPem, caKeyPem) = TestRestCa.Generate(validity: TimeSpan.FromDays(30));

        // Act: выпуск серта (штатные 10 лет зажимаются).
        var (certPem, _) = RestPki.IssueNodeCertificate(caPem, caKeyPem, "n1", "pgw-c1-s1-n1");

        // Assert: NotAfter серта не выходит за NotAfter CA.
        using var ca = X509Certificate2.CreateFromPem(caPem);
        using var cert = X509Certificate2.CreateFromPem(certPem);
        cert.NotAfter.Should().BeOnOrBefore(ca.NotAfter);
    }

    [Fact]
    public void IssueNodeCertificate_CertificateValidatesAgainstCaChain()
    {
        // Arrange: тестовый CA.
        var (caPem, caKeyPem) = TestRestCa.Generate();

        // Act: выпуск серта.
        var (certPem, _) = RestPki.IssueNodeCertificate(caPem, caKeyPem, "n1", "pgw-c1-s1-n1");

        // Assert: цепочка валидна против CA (CustomRootTrust-паттерн
        // Shared.Tls/TlsChain — тот же способ, которым клиенты REST
        // верифицируют серты нод).
        using var ca = X509Certificate2.CreateFromPem(caPem);
        using var cert = X509Certificate2.CreateFromPem(certPem);
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(ca);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.Build(cert).Should().BeTrue();
    }

    [Fact]
    public void TryParse_MalformedPemIsFalse()
    {
        // Arrange: мусор вместо PEM.
        // Act / Assert: мягкий разбор — битый PEM не бросает исключение.
        RestPki.TryParseCertificate("not a pem", out var cert).Should().BeFalse();
        cert.Should().BeNull();
        RestPki.TryParseRsaKey("not a pem", out var key).Should().BeFalse();
        key.Should().BeNull();
    }
}

// Тестовый CA REST-TLS: RSA-2048 self-signed .NET (механика ClusterPki.GenerateCa
// KafkaWorker / E2eTestPki.GenerateCa); срок параметризуется кейсом зажима.
public static class TestRestCa
{
    public static (string CaPem, string CaKeyPem) Generate(TimeSpan? validity = null)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN=pgw-test-install-ca-{Guid.NewGuid():N}", rsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.Add(validity ?? TimeSpan.FromDays(3650)));
        return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
    }
}
