using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace ValkeyWorker.Core.Valkey;

/// <summary>
/// Per-cluster PKI valkey (arch/20 §2, arch/21 §2; t06): self-signed CA
/// (RSA-2048, CN=vwk-&lt;C&gt;-ca-&lt;отпечаток&gt; — subject уникален на генерацию,
/// 10 лет) и серверные серты нод (CN=node&lt;k&gt;, SAN ТОЛЬКО advertised-хост
/// DNS|IP, EKU ServerAuth, 10 лет зажаты в CA) — CertificateRequest .NET,
/// без внешних инструментов. PEM — одной строкой с \n (канон значений etcd).
/// Порт kafka ClusterPki (t03/t07); клиентские серты не выпускает —
/// принципалы из ACL (--tls-auth-clients no).
/// </summary>
public static class ValkeyPki
{
    private static readonly Oid ServerAuthOid = new("1.3.6.1.5.5.7.3.1");

    public static (string CaPem, string CaKeyPem) GenerateCa(string cluster)
    {
        using var rsa = RSA.Create(2048);
        // Отпечаток ключа в CN: одинаковые subject поколений ломали
        // верификацию в бандлах двойного доверия (t07 kafka) — фикс переносится.
        var fingerprint = Convert.ToHexString(
            SHA256.HashData(rsa.ExportSubjectPublicKeyInfo()), 0, 4).ToLowerInvariant();
        var request = new CertificateRequest(
            $"CN=vwk-{cluster}-ca-{fingerprint}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(10));
        return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
    }

    public static (string CertPem, string KeyPem) IssueNodeCertificate(
        string caCertPem, string caKeyPem, string commonName, string advertisedHost)
    {
        using var caCertificate = ParseCertificate(caCertPem);
        using var caKey = ParseRsaKey(caKeyPem);
        // Create требует приватный ключ У issuer-серта: прикрепляем ключ парсера.
        using var caWithKey = caCertificate.CopyWithPrivateKey(caKey);
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        // SAN — ТОЛЬКО advertised-хост кластера (arch/21 §2): standalone,
        // inter-node-алиаса нет; IP-хост — IP-запись SAN.
        if (IPAddress.TryParse(advertisedHost, out var ip))
            san.AddIpAddress(ip);
        else
            san.AddDnsName(advertisedHost);
        request.CertificateExtensions.Add(san.Build());
        // EKU только ServerAuth: клиентские серты не выпускаются (--tls-auth-clients no).
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension([ServerAuthOid], critical: false));
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
        try { certificate = ParseCertificate(pem); return true; }
        catch (Exception e) when (e is ArgumentException or CryptographicException or FormatException)
        { certificate = null; return false; }
    }

    public static bool TryParseRsaKey(string pem, out RSA? key)
    {
        try { key = ParseRsaKey(pem); return true; }
        catch (Exception e) when (e is ArgumentException or CryptographicException or FormatException)
        { key = null; return false; }
    }

    // Разбор PEM без внешних инструментов: первый блок CERTIFICATE → DER →
    // X509CertificateLoader (образец kafka ClusterPki).
    private static X509Certificate2 ParseCertificate(string pem)
        => X509CertificateLoader.LoadCertificate(DecodePemBlock(pem, "CERTIFICATE"));

    private static RSA ParseRsaKey(string pem)
    {
        var rsa = RSA.Create();
        try { rsa.ImportFromPem(pem); return rsa; }
        catch { rsa.Dispose(); throw; }
    }

    // Декодирование первого PEM-блока метки: base64-тело (пробелы/переносы
    // выбрасываются) → байты; отсутствующий/битый блок — ArgumentException/
    // FormatException (ловятся TryParse-обёртками).
    private static byte[] DecodePemBlock(string pem, string label)
    {
        var beginMarker = $"-----BEGIN {label}-----";
        var endMarker = $"-----END {label}-----";
        var start = pem.IndexOf(beginMarker, StringComparison.Ordinal);
        if (start < 0) throw new ArgumentException($"PEM не содержит блока {label}");
        var bodyStart = start + beginMarker.Length;
        var end = pem.IndexOf(endMarker, bodyStart, StringComparison.Ordinal);
        if (end < 0) throw new ArgumentException($"PEM не содержит конца блока {label}");
        var base64 = new string(pem[bodyStart..end].Where(c => !char.IsWhiteSpace(c)).ToArray());
        return Convert.FromBase64String(base64);
    }
}
