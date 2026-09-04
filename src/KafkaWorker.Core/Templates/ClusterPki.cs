using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace KafkaWorker.Core.Templates;

/// <summary>
/// Per-cluster PKI (arch/16 §2.3): self-signed CA (RSA-2048, CN=kfw-&lt;C&gt;-ca,
/// 10 лет) и серверные серты нод (CN=broker&lt;k&gt;, SAN docker-DNS + advertised,
/// EKU ServerAuth, 10 лет) — CertificateRequest .NET, без внешних инструментов.
/// PEM — одной строкой с \n (канон значений etcd, arch/15 §2.1).
/// </summary>
public static class ClusterPki
{
    private static readonly Oid ServerAuthOid = new("1.3.6.1.5.5.7.3.1");
    private static readonly Oid ClientAuthOid = new("1.3.6.1.5.5.7.3.2");

    public static (string CaPem, string CaKeyPem) GenerateCa(string cluster)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN=kfw-{cluster}-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(10));
        return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
    }

    public static (string CertPem, string KeyPem) IssueBrokerCertificate(
        string caCertPem, string caKeyPem, string commonName,
        IReadOnlyList<string> dnsNames, IPAddress? ip)
    {
        using var caCertificate = ParseCertificate(caCertPem);
        using var caKey = ParseRsaKey(caKeyPem);
        // Create требует приватный ключ У issuer-серта: прикрепляем ключ парсера.
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
        // EKU: ServerAuth (серти нод Kafka) + ClientAuth (клиентские серты
        // панели/healthcheck из той же per-cluster CA — один выпускающий helper).
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension([ServerAuthOid, ClientAuthOid], critical: false));
        // Окно серта ноды не может выходить за границы CA (валидация issuer):
        // NotAfter зажимаем в NotAfter CA (CA создан моментом ранее).
        var notAfter = DateTimeOffset.UtcNow.AddYears(10);
        if (notAfter > caWithKey.NotAfter)
            notAfter = caWithKey.NotAfter;
        using var certificate = request.Create(
            caWithKey, DateTimeOffset.UtcNow.AddDays(-1), notAfter,
            RandomNumberGenerator.GetBytes(16));
        return (certificate.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
    }

    public static bool TryParseCertificate(string pem, out X509Certificate2? certificate)
    {
        try
        {
            certificate = ParseCertificate(pem);
            return true;
        }
        catch (Exception e) when (
            e is ArgumentException or CryptographicException or FormatException)
        {
            certificate = null;
            return false;
        }
    }

    public static bool TryParseRsaKey(string pem, out RSA? key)
    {
        try
        {
            key = ParseRsaKey(pem);
            return true;
        }
        catch (Exception e) when (
            e is ArgumentException or CryptographicException or FormatException)
        {
            key = null;
            return false;
        }
    }

    // Разбор PEM без внешних инструментов: первый блок CERTIFICATE → DER →
    // X509CertificateLoader (цепочка сертов — берётся первый, конечный).
    private static X509Certificate2 ParseCertificate(string pem)
        => X509CertificateLoader.LoadCertificate(DecodePemBlock(pem, "CERTIFICATE"));

    private static RSA ParseRsaKey(string pem)
    {
        var rsa = RSA.Create();
        try
        {
            rsa.ImportFromPem(pem);
            return rsa;
        }
        catch
        {
            rsa.Dispose();
            throw;
        }
    }

    // Декодирование первого PEM-блока метки: base64-тело (пробелы/переносы
    // выбрасываются) → байты; отсутствующий/битый блок — ArgumentException/
    // FormatException (ловятся TryParse-обёртками).
    private static byte[] DecodePemBlock(string pem, string label)
    {
        var beginMarker = $"-----BEGIN {label}-----";
        var endMarker = $"-----END {label}-----";
        var start = pem.IndexOf(beginMarker, StringComparison.Ordinal);
        if (start < 0)
            throw new ArgumentException($"PEM не содержит блока {label}");
        var bodyStart = start + beginMarker.Length;
        var end = pem.IndexOf(endMarker, bodyStart, StringComparison.Ordinal);
        if (end < 0)
            throw new ArgumentException($"PEM не содержит конца блока {label}");
        var base64 = new string(pem[bodyStart..end].Where(c => !char.IsWhiteSpace(c)).ToArray());
        return Convert.FromBase64String(base64);
    }
}
