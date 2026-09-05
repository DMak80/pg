using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace PgWorker.IntegrationTests.Docker;

// PKI-хелпер транспортных интеграционных тестов (t03): фикстурная CA + выпуски
// сертов с SAN — копия механики ClusterPki воркера (KafkaWorker.Core), локально
// в тестовой сборке (тесты PgWorker не тянут зависимость от KafkaWorker.Core).
public static class EngineProxyTestPki
{
    private static readonly Oid ServerAuthOid = new("1.3.6.1.5.5.7.3.1");
    private static readonly Oid ClientAuthOid = new("1.3.6.1.5.5.7.3.2");

    public static (string CaPem, string CaKeyPem) GenerateCa()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=pgw-test-docker-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
        return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
    }

    public static (string CertPem, string KeyPem) Issue(
        string caCertPem, string caKeyPem, string commonName,
        IReadOnlyList<string> dnsNames, IPAddress? ip)
    {
        using var caCertificate = X509Certificate2.CreateFromPem(caCertPem);
        using var caKey = RSA.Create();
        caKey.ImportFromPem(caKeyPem);
        using var caWithKey = caCertificate.CopyWithPrivateKey(caKey);
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        foreach (var dns in dnsNames)
            san.AddDnsName(dns);
        if (ip is not null)
            san.AddIpAddress(ip);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension([ServerAuthOid, ClientAuthOid], critical: false));
        // notAfter — не позже CA (минус минута): последовательные выпуски за год
        // иначе дают notAfter серта на миллисекунды позже issuer (ArgumentException).
        using var certificate = request.Create(
            caWithKey, DateTimeOffset.UtcNow.AddDays(-1), caCertificate.NotAfter.AddMinutes(-1),
            RandomNumberGenerator.GetBytes(16));
        return (certificate.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
    }
}
