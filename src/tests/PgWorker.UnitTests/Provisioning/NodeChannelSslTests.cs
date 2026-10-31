using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using PgWorker.Core.Templates;
using PgWorker.Provisioning.Probes;
using PgWorker.UnitTests.Templates;
using Xunit;

// AAA: SanContainsDnsName на РЕАЛЬНОМ серте (RestPki.IssueNodeCertificate —
// SAN из SubjectAlternativeNameBuilder): мини-парсер DER SAN обязан находить
// полное имя ноды и отличать чужое (регрессия: сдвиг на байт в копии
// DerLengthSize не ловится рукотворными массивами — код прецедентов
// копируется дословно и покрывается тестом на реальном серте).
public class NodeChannelSslTests
{
    private static (string CaPem, string CaKeyPem) GenerateCa()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN=pgw-test-install-ca-{Guid.NewGuid():N}", rsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(3650));
        return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
    }

    [Fact]
    public void San_ContainsFullNodeName_RealCertificate()
    {
        // Arrange: per-install CA + серт ноды (реальный X509, SAN builder).
        var (caPem, caKeyPem) = GenerateCa();
        var (certPem, _) = RestPki.IssueNodeCertificate(
            caPem, caKeyPem, nodeName: "shard1a", nodeFullName: "pgw-c1-shard1-shard1a");
        using var cert = X509Certificate2.CreateFromPem(certPem);

        // Act/Assert: полное имя найдено; чужое — нет.
        NodeChannelSsl.SanContainsDnsName(cert, "pgw-c1-shard1-shard1a").Should().BeTrue();
        NodeChannelSsl.SanContainsDnsName(cert, "pgw-c1-shard1-shard1b").Should().BeFalse();
        NodeChannelSsl.SanContainsDnsName(cert, "").Should().BeFalse();
    }
}
