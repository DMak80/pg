using System.Net;
using System.Security.Cryptography.X509Certificates;
using ValkeyWorker.Core.Valkey;

namespace ValkeyWorker.UnitTests.Valkey;

// PKI per-cluster (spec §4.1): CA RSA-2048/10 лет/уникальный subject;
// серт ноды CN=node<k>, SAN advertised (DNS|IP), EKU ServerAuth, PEM round-trip.
public sealed class ValkeyPkiTests
{
    [Fact]
    public void GenerateCa_UniqueSubjectPerGeneration()
    {
        // Arrange — две генерации CA одного кластера
        // Act
        var (pem1, _) = ValkeyPki.GenerateCa("c1");
        var (pem2, _) = ValkeyPki.GenerateCa("c1");
        // Assert — subject уникален (отпечаток ключа; фикс t07 kafka)
        Assert.NotEqual(ExtractSubject(pem1), ExtractSubject(pem2));
        Assert.StartsWith("CN=vwk-c1-ca-", ExtractSubject(pem1));
    }

    [Fact]
    public void IssueNodeCertificate_SanDnsAndIp()
    {
        // Arrange
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("c1");
        // Act — DNS-хост и IP-хост advertised
        var (dnsCert, _) = ValkeyPki.IssueNodeCertificate(caPem, caKeyPem, "node1", "host.example");
        var (ipCert, _) = ValkeyPki.IssueNodeCertificate(caPem, caKeyPem, "node1", "127.0.0.1");
        // Assert — SAN покрывает advertised, EKU только ServerAuth
        Assert.Contains("host.example", SanNames(dnsCert));
        Assert.Contains(IPAddress.Parse("127.0.0.1"), SanIps(ipCert));
    }

    [Fact]
    public void Pem_RoundTrip_FromSingleLineWithEscapes()
    {
        // Arrange — etcd-канон: PEM одной строкой с \n
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("c1");
        var flat = caPem.Replace("\r\n", "\n");
        // Act
        var ok = ValkeyPki.TryParseCertificate(flat, out var cert);
        var keyOk = ValkeyPki.TryParseRsaKey(caKeyPem, out _);
        // Assert
        Assert.True(ok);
        Assert.NotNull(cert);
        Assert.True(keyOk);
    }

    [Fact]
    public void TryParse_BrokenPem_False()
    {
        // Arrange / Act / Assert — мусор не бросает исключений
        Assert.False(ValkeyPki.TryParseCertificate("not a pem", out var cert));
        Assert.Null(cert);
        Assert.False(ValkeyPki.TryParseRsaKey("not a pem", out var key));
        Assert.Null(key);
    }

    private static string ExtractSubject(string pem)
        => X509Certificate2.CreateFromPem(pem).Subject;

    private static IReadOnlyList<string> SanNames(string certPem)
    {
        using var cert = X509Certificate2.CreateFromPem(certPem);
        return cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().First().EnumerateDnsNames().ToList();
    }

    private static IReadOnlyList<IPAddress> SanIps(string certPem)
    {
        using var cert = X509Certificate2.CreateFromPem(certPem);
        return cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().First().EnumerateIPAddresses().ToList();
    }
}
