using System.Net;
using System.Security.Cryptography.X509Certificates;
using FluentAssertions;
using KafkaWorker.Core.Templates;

namespace KafkaWorker.UnitTests.Templates;

// PKI кластера (arch/16 §2.3): self-signed CA + серверные серты нод, PEM one-line.
public class ClusterPkiTests
{
    [Fact]
    public void GenerateCa_SelfSignedCaWithCanonicalCnAndLongValidity()
    {
        // Arrange: кластер "events".
        // Act: генерация CA.
        var (caPem, caKeyPem) = ClusterPki.GenerateCa("events");

        // Assert: PEM-маркеры, канонический CN, срок ~10 лет, публичный серт без ключа.
        caPem.Should().StartWith("-----BEGIN CERTIFICATE-----").And.Contain("\n-----END CERTIFICATE-----");
        caKeyPem.Should().StartWith("-----BEGIN PRIVATE KEY-----"); // PKCS#8 (15 §2.1)
        using var cert = X509Certificate2.CreateFromPem(caPem);
        cert.Subject.Should().Be("CN=kfw-events-ca");
        cert.HasPrivateKey.Should().BeFalse("публичный серт не несёт ключа");
        (cert.NotAfter - cert.NotBefore).TotalDays.Should().BeInRange(3600, 3700);
    }

    [Fact]
    public void IssueBrokerCertificate_DnsSanCoverNodeAndAdvertisedHost()
    {
        // Arrange: CA + нода broker2, advertised host.docker.internal.
        var (caPem, caKeyPem) = ClusterPki.GenerateCa("events");

        // Act: выпуск серта ноды (SAN: docker-DNS + advertised host).
        var (certPem, keyPem) = ClusterPki.IssueBrokerCertificate(
            caPem, caKeyPem, "broker2", ["broker2", "host.docker.internal"], ip: null);

        // Assert: подписан CA, CN/SAN/EKU, PEM round-trip с приватным ключом.
        using var ca = X509Certificate2.CreateFromPem(caPem);
        using var cert = X509Certificate2.CreateFromPem(certPem);
        cert.Subject.Should().Be("CN=broker2");
        var san = cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().Single();
        san.EnumerateDnsNames().Should().Contain(["broker2", "host.docker.internal"]);
        cert.Issuer.Should().Be(ca.Subject, "серт подписан ключом CA");
        keyPem.Should().StartWith("-----BEGIN PRIVATE KEY-----");
    }

    [Fact]
    public void IssueBrokerCertificate_IpAdvertisedHostBecomesIpSan()
    {
        // Arrange: advertised-хост — IP-литерал (мульти-хост plain).
        var (caPem, caKeyPem) = ClusterPki.GenerateCa("ev");

        // Act: серт с IP-SAN.
        var (certPem, _) = ClusterPki.IssueBrokerCertificate(
            caPem, caKeyPem, "broker1", ["broker1"], IPAddress.Parse("10.0.0.5"));

        // Assert: IP в SAN.
        using var cert = X509Certificate2.CreateFromPem(certPem);
        cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().Single()
            .EnumerateIPAddresses().Should().Contain(IPAddress.Parse("10.0.0.5"));
    }

    [Fact]
    public void TryParseCertificate_MalformedPemIsFalse()
    {
        // Arrange: мусор вместо PEM.
        // Act / Assert: мягкий разбор — битый PEM не бросает исключение.
        ClusterPki.TryParseCertificate("not a pem", out var cert).Should().BeFalse();
        cert.Should().BeNull();
    }
}

// Кеш сертов нод: один серт на (кластер, нода, CA) в рамках процесса — R3.
public class BrokerCertificateCacheTests
{
    [Fact]
    public void GetOrCreate_SameInputsReturnSameCertificate()
    {
        // Arrange: кеш и per-cluster CA.
        var cache = new BrokerCertificateCache();
        var (caPem, caKeyPem) = ClusterPki.GenerateCa("events");

        // Act: два вызова для broker1.
        var first = cache.GetOrCreate("events", "broker1", caPem, caKeyPem, "host.docker.internal");
        var second = cache.GetOrCreate("events", "broker1", caPem, caKeyPem, "host.docker.internal");

        // Assert: идентичный PEM (серт не перегенерируется).
        first.CertPem.Should().Be(second.CertPem);
        first.KeyPem.Should().Be(second.KeyPem);
        // SAN покрывает docker-DNS и advertised: host без порта → DNS.
        using var cert = X509Certificate2.CreateFromPem(first.CertPem);
        cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().Single()
            .EnumerateDnsNames().Should().Contain(["broker1", "host.docker.internal"]);
    }

    [Fact]
    public void GetOrCreate_NewCaInvalidatesCache()
    {
        // Arrange: два разных CA (перегенерация).
        var cache = new BrokerCertificateCache();
        var (ca1, caKey1) = ClusterPki.GenerateCa("events");
        var (ca2, caKey2) = ClusterPki.GenerateCa("events");

        // Act: серт под вторым CA.
        var cert2 = cache.GetOrCreate("events", "broker1", ca2, caKey2, "host.docker.internal");

        // Assert: серт подписан вторым CA (кеш не вернул протухший).
        using var ca2Cert = X509Certificate2.CreateFromPem(ca2);
        X509Certificate2.CreateFromPem(cert2.CertPem).Issuer.Should().Be(ca2Cert.Subject);
    }
}
