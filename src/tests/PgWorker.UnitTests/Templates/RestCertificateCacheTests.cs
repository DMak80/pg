using System.Security.Cryptography.X509Certificates;
using PgWorker.Core.Templates;

namespace PgWorker.UnitTests.Templates;

// Кеш сертов REST-эндпоинтов (arch/14 §2.1): один серт на
// (кластер, шард, нода, CA) в рамках жизни процесса — повторные сборки env
// (надзор/ensure/rebuild) дают тот же PEM; серт — не etcd-состояние.
public class RestCertificateCacheTests
{
    [Fact]
    public void GetOrCreate_SameNodeReturnsSamePem()
    {
        // Arrange: кеш на тестовом CA.
        var (caPem, caKeyPem) = TestRestCa.Generate();
        var cache = new RestCertificateCache(caPem, caKeyPem);

        // Act: два вызова для одной ноды.
        var first = cache.GetOrCreate("c1", "shard1", "shard1a");
        var second = cache.GetOrCreate("c1", "shard1", "shard1a");

        // Assert: идентичный PEM (серт не перегенерируется).
        first.CertPem.Should().Be(second.CertPem);
        first.KeyPem.Should().Be(second.KeyPem);
    }

    [Fact]
    public void GetOrCreate_DifferentNodesGetDifferentCertificates()
    {
        // Arrange: кеш.
        var (caPem, caKeyPem) = TestRestCa.Generate();
        var cache = new RestCertificateCache(caPem, caKeyPem);

        // Act: серты двух нод.
        var nodeA = cache.GetOrCreate("c1", "shard1", "shard1a");
        var nodeB = cache.GetOrCreate("c1", "shard1", "shard1b");

        // Assert: разные серты (CN/SAN различаются по ноде).
        nodeA.CertPem.Should().NotBe(nodeB.CertPem);
    }

    [Fact]
    public void GetOrCreate_NewCaKeyIssuesNewCertificate()
    {
        // Arrange: два CA (перегенерация per-install пакета).
        var (ca1, caKey1) = TestRestCa.Generate();
        var (ca2, caKey2) = TestRestCa.Generate();
        var cache1 = new RestCertificateCache(ca1, caKey1);
        var cache2 = new RestCertificateCache(ca2, caKey2);

        // Act: серты одной ноды из двух кешей.
        var cert1 = cache1.GetOrCreate("c1", "shard1", "shard1a");
        var cert2 = cache2.GetOrCreate("c1", "shard1", "shard1a");

        // Assert: серт второго кеша подписан вторым CA (смена CA — новый серт).
        using var ca2Cert = X509Certificate2.CreateFromPem(ca2);
        X509Certificate2.CreateFromPem(cert2.CertPem).Issuer.Should().Be(ca2Cert.Subject);
        cert1.CertPem.Should().NotBe(cert2.CertPem);
    }

    [Fact]
    public void GetOrCreate_CertificateValidatesAgainstCa()
    {
        // Arrange: кеш.
        var (caPem, caKeyPem) = TestRestCa.Generate();
        var cache = new RestCertificateCache(caPem, caKeyPem);

        // Act: серт из кеша.
        var (certPem, _) = cache.GetOrCreate("c1", "shard1", "shard1a");

        // Assert: цепочка валидна против CA; SAN несёт полное имя ноды
        // pgw-<C>-<X>-<n> (строится внутри кеша).
        using var ca = X509Certificate2.CreateFromPem(caPem);
        using var cert = X509Certificate2.CreateFromPem(certPem);
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(ca);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.Build(cert).Should().BeTrue();
        cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().Single()
            .EnumerateDnsNames().Should().Contain("pgw-c1-shard1-shard1a");
    }
}
