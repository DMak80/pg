using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Linq;

namespace PgWorker.Core.Templates;

/// <summary>
/// PKI REST-эндпоинтов нод (arch/14 §2.1/§2.4): выпуск серверных сертов
/// REST `:8008` из per-install API-CA воркером. SAN = DNS &lt;n&gt; (короткий
/// alias в pgw-net-&lt;C&gt;) + DNS pgw-&lt;C&gt;-&lt;X&gt;-&lt;n&gt; (полное docker-имя) +
/// IP 127.0.0.1 (loopback lease-скрипта P11); advertised-хост в SAN не
/// входит. Порт механики ClusterPki KafkaWorker (CertificateRequest .NET);
/// EKU — только ServerAuth (mTLS REST-грани нет).
/// </summary>
public static class RestPki
{
    private static readonly Oid ServerAuthOid = new("1.3.6.1.5.5.7.3.1");

    /// <summary>
    /// Выпуск серта REST-эндпоинта ноды: CN = полное имя ноды, RSA-2048,
    /// SHA256, срок 10 лет с зажимом NotAfter в NotAfter CA. Ключ — PKCS#8 PEM.
    /// </summary>
    public static (string CertPem, string KeyPem) IssueNodeCertificate(
        string caCertPem, string caKeyPem, string nodeName, string nodeFullName)
    {
        using var caCertificate = ParseCertificate(caCertPem);
        using var caKey = ParseRsaKey(caKeyPem);
        // Create требует приватный ключ у issuer-серта: прикрепляем ключ парсера.
        using var caWithKey = caCertificate.CopyWithPrivateKey(caKey);
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={nodeFullName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(nodeName);
        san.AddDnsName(nodeFullName);
        san.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension([ServerAuthOid], critical: false));
        // Окно серта ноды не может выходить за границы CA (валидация issuer):
        // NotAfter зажимаем в NotAfter CA.
        var notAfter = DateTimeOffset.UtcNow.AddYears(10);
        if (notAfter > caWithKey.NotAfter)
            notAfter = caWithKey.NotAfter;
        using var certificate = request.Create(
            caWithKey, DateTimeOffset.UtcNow.AddDays(-1), notAfter,
            RandomNumberGenerator.GetBytes(16));
        return (certificate.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
    }

    /// <summary>Мягкий разбор PEM-сертификата (валидация старта воркера).</summary>
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

    /// <summary>Мягкий разбор PEM RSA-ключа (валидация старта воркера).</summary>
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
    // X509CertificateLoader (порт ClusterPki.DecodePemBlock).
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
