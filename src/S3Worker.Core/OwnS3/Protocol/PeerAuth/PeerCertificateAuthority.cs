using System.Security.Cryptography;
using System.Text;

namespace OwnS3.Protocol.PeerAuth;

// Издатель мини-PKI ownS3 (глава 07 §2): генерация CA, выпуск и верификация
// сертификатов серверов. Используется CLI OwnS3.Pki и тестами; сервер ownS3
// ключи не генерирует. Подписи — ECDSA P-256 over SHA-256, P1363 fixed-length.
public static class PeerCertificateAuthority
{
    public const int DefaultExpiresDays = 365;

    // Новая пара CA: trust-anchor JSON + приватный ключ PEM (PKCS#8).
    public static (string CaJson, string CaPrivatePem) CreateCa(string name)
    {
        using var ca = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var trust = new PeerTrustAnchor(name, ca.ExportSubjectPublicKeyInfoPem());
        return (trust.ToJson(), ca.ExportPkcs8PrivateKeyPem());
    }

    // Выпуск сертификата сервера: подпись CA — P1363 base64 по канонической
    // строке сертификата; expiresDays ≤ 0 — бессрочный (expiresAtUnixMs = 0).
    public static (PeerCertificate Cert, string PrivatePem) IssueServer(string name,
        PeerTrustAnchor ca, string caPrivatePem, DateTimeOffset at, int expiresDays = DefaultExpiresDays)
    {
        using var caKey = ECDsa.Create();
        caKey.ImportFromPem(caPrivatePem);
        using var server = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var publicKeyPem = server.ExportSubjectPublicKeyInfoPem();
        var issued = at.ToUnixTimeMilliseconds();
        var expires = expiresDays <= 0 ? 0 : at.AddDays(expiresDays).ToUnixTimeMilliseconds();
        var canonical = $"{name}\n{publicKeyPem}\n{issued}\n{expires}\n{ca.Name}";
        var signature = Convert.ToBase64String(caKey.SignData(Encoding.UTF8.GetBytes(canonical),
            HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation));
        return (new PeerCertificate(name, publicKeyPem, issued, expires, ca.Name, signature),
            server.ExportPkcs8PrivateKeyPem());
    }

    // Сертификат валиден ⟺ caName совпадает ∧ подпись CA верна ∧ не истёк (глава 07 §2).
    // Тотальна на атакуемом входе: битый PEM trust-anchor / не-base64 подпись — не
    // исключение сквозь Result-границу (join-грань отдала бы 500), а «невалиден»
    // (403/502 по месту вызова, spec §4.4 п.3/п.4).
    public static bool Verify(PeerCertificate cert, PeerTrustAnchor trust, DateTimeOffset now)
    {
        if (!string.Equals(cert.CaName, trust.Name, StringComparison.Ordinal)) return false;
        if (cert.IsExpired(now)) return false;
        try
        {
            using var caKey = ECDsa.Create();
            caKey.ImportFromPem(trust.PublicKeyPem);
            return caKey.VerifyData(Encoding.UTF8.GetBytes(cert.CanonicalForm()),
                Convert.FromBase64String(cert.Signature), HashAlgorithmName.SHA256,
                DSASignatureFormat.IeeeP1363FixedFieldConcatenation);
        }
        catch (Exception)
        {
            return false; // FormatException/ArgumentException — сертификат просто невалиден
        }
    }
}
