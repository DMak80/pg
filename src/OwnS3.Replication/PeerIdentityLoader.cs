using System.Security.Cryptography;
using OwnS3.Protocol.PeerAuth;

namespace OwnS3.Replication;

// Identity сервера (глава 07 §2): пара ключей + мини-сертификат + исходный JSON
// сертификата (для заголовка X-OwnS3-Peer-Certificate). Материал env каждого
// старта — на томе не персистится; живёт в DI весь процесс (Key не диспозится).
public sealed record PeerIdentity(string Name, PeerCertificate Certificate, ECDsa Key,
    string CertificateJson);

// Загрузка identity/trust-anchor из env-конфига (глава 07 §2/§11): значение или
// путь — прецедент TLS-материала (глава 05 §4, OwnS3Tls.Resolve); любое нарушение
// материала — исключение → отказ старта Program (fail-fast, без «тихой» деградации).
// Сервер ключи НЕ генерирует — выпуск CLI OwnS3.Pki у владельца инсталляции.
public static class PeerIdentityLoader
{
    public static PeerIdentity Load(PeerIdentityOptions o)
    {
        var certGiven = !string.IsNullOrWhiteSpace(o.Certificate)
                        || !string.IsNullOrWhiteSpace(o.CertificatePath);
        var keyGiven = !string.IsNullOrWhiteSpace(o.PrivateKeyPem)
                       || !string.IsNullOrWhiteSpace(o.PrivateKeyPath);
        if (!certGiven && !keyGiven)
            throw new InvalidOperationException(
                "OwnS3:Replication:Identity: секция задана, материала нет — укажите оба " +
                "(env OWNS3_REPLICATION_CERT[_PATH] + OWNS3_REPLICATION_PRIVATE_KEY[_PATH])");
        if (!certGiven || !keyGiven)
            throw new InvalidOperationException(
                "OwnS3:Replication:Identity: задан только один из пары сертификат/ключ — укажите оба " +
                "(env OWNS3_REPLICATION_CERT[_PATH] + OWNS3_REPLICATION_PRIVATE_KEY[_PATH])");

        // Значение приоритетнее пути; задано одно из пары — берётся заданное.
        var certJson = ReadMaterial(o.Certificate, o.CertificatePath,
            "OWNS3_REPLICATION_CERT_PATH");
        var keyPem = ReadMaterial(o.PrivateKeyPem, o.PrivateKeyPath,
            "OWNS3_REPLICATION_PRIVATE_KEY_PATH");

        PeerCertificate cert;
        try
        {
            cert = PeerCertificate.FromJson(certJson);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"OwnS3:Replication:Identity: сертификат пира не парсится — {ex.Message}", ex);
        }
        // Fail-fast срока: истёкший сертификат — отказ старта (глава 07 §2).
        if (cert.IsExpired(TimeProvider.System.GetUtcNow()))
            throw new InvalidOperationException(
                $"OwnS3:Replication:Identity: сертификат пира {cert.Name} истёк " +
                $"(expiresAtUnixMs={cert.ExpiresAtUnixMs}) — выпустите новый (CLI OwnS3.Pki)");

        var created = ECDsa.Create();
        try
        {
            created.ImportFromPem(keyPem);
        }
        catch (Exception ex)
        {
            created.Dispose();
            throw new InvalidOperationException(
                $"OwnS3:Replication:Identity: приватный ключ не импортируется — {ex.Message}", ex);
        }
        // Сверка ключа с сертификатом (fail-fast, глава 07 §2); нормализация концов строк.
        var exported = created.ExportSubjectPublicKeyInfoPem();
        if (!string.Equals(exported.Replace("\r\n", "\n"),
                cert.PublicKeyPem.Replace("\r\n", "\n"), StringComparison.Ordinal))
        {
            created.Dispose();
            throw new InvalidOperationException(
                $"OwnS3:Replication:Identity: приватный ключ не соответствует публичному ключу сертификата {cert.Name}");
        }
        return new PeerIdentity(cert.Name, cert, created, certJson);
    }

    // Trust-anchor инсталляции; null — не задан (join-ручной режим состава).
    // PEM публичного ключа валидируется импортом: битый якорь — отказ старта
    // (fail-fast), а не «тихий» mesh, отказывающий каждому join.
    public static PeerTrustAnchor? LoadTrustAnchor(PeerTrustCaOptions? o)
    {
        if (o is null || (string.IsNullOrWhiteSpace(o.Certificate)
                          && string.IsNullOrWhiteSpace(o.CertificatePath)))
            return null;
        var json = ReadMaterial(o.Certificate, o.CertificatePath, "OWNS3_REPLICATION_TRUST_CA_PATH");
        PeerTrustAnchor trust;
        try
        {
            trust = PeerTrustAnchor.FromJson(json);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"OwnS3:Replication:TrustCa: trust-anchor не парсится — {ex.Message}", ex);
        }
        try
        {
            using var key = ECDsa.Create();
            key.ImportFromPem(trust.PublicKeyPem);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"OwnS3:Replication:TrustCa: PEM публичного ключа CA не импортируется — {ex.Message}", ex);
        }
        return trust;
    }

    // Значение приоритетнее пути (прецедент OwnS3Tls.Resolve); ошибка чтения файла — fail-fast.
    private static string ReadMaterial(string? value, string? path, string pathEnvName)
    {
        if (!string.IsNullOrWhiteSpace(value))
            return value;
        try
        {
            return File.ReadAllText(path!);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"файл материала identity не читается ({pathEnvName}={path}) — {ex.Message}", ex);
        }
    }
}
