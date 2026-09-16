using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace ValkeyWorker.IntegrationTests.Api;

// Тестовый генератор TLS-материала (аналог ClusterPki kfw, минимум для
// mTLS/серт-стартап тестов): self-signed CA per-инсталл + лист клиентских/
// серверных сертов этой CA. PEM одной строкой; PFX round-trip — в тестах.
public static class TestPki
{
    public static (string CaPem, string CaKeyPem) GenerateCa(string commonName)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName} CA", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(
            new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(
            new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign, true));
        using var ca = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(5));
        return (PemCert(ca), PemKey(rsa));
    }

    public static (string CertPem, string KeyPem) IssueCertificate(
        string caPem, string caKeyPem, string commonName)
    {
        using var ca = X509Certificate2.CreateFromPem(caPem, caKeyPem);
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(
            new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(
            new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(commonName);
        san.AddDnsName("localhost");
        request.CertificateExtensions.Add(san.Build());
        using var cert = request.Create(ca, DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(3), Guid.NewGuid().ToByteArray());
        var withKey = cert.CopyWithPrivateKey(rsa);
        return (PemCert(withKey), PemKey(rsa));
    }

    private static string PemCert(X509Certificate2 cert)
        => cert.ExportCertificatePem().ReplaceLineEndings("\n").TrimEnd() + "\n";

    private static string PemKey(RSA rsa)
        => rsa.ExportPkcs8PrivateKeyPem().ReplaceLineEndings("\n").TrimEnd() + "\n";
}
