using System.Security.Cryptography.X509Certificates;
using ValkeyWorker.Core.Valkey;

namespace ValkeyWorker.Provisioning.Processes;

/// <summary>
/// TLS-материал ноды (env-TLS, arch/21 §2/V3): PEM серта/ключа/CA доставляется
/// env контейнера VALKEY_TLS_{CERT,KEY,CA} (значения — многострочный PEM);
/// cmd-обёртка NodeArgsBuilder.BuildCmd раскатывает их в /tls при старте.
/// Валидность факта = CA == ожидаемому (Trim-сравнение), цепочка валидна,
/// key↔cert, NotAfter жив, SAN покрывает advertised-хост DNS|IP → пропуск;
/// иначе — пересоздание со свежим env (свежий серт случаен — сверка по
/// валидности IsValidNodeEnv, не побайтовая). Нормализация \n → переносы —
/// единый хелпер NormalizePem на границах сборки И сверки (etcd-канон
/// «одной строкой с \n» — arch/20 §2.1 — не меняется).
/// </summary>
public static class NodeTlsProvisioner
{
    /// <summary>Env ноды: свежий серт (CN=node&lt;k&gt;, SAN advertised, подпись ca_key);
    /// значения — нормализованный многострочный PEM (граница сборки, §4.1).</summary>
    public static IReadOnlyDictionary<string, string> BuildNodeTlsEnv(
        string caPem, string caKeyPem, string node, string advertisedHost)
    {
        // Нормализация на границе сборки: вход из etcd-канона «одной строкой
        // с \n» → многострочный PEM (IssueNodeCertificate требует настоящих
        // переносов); свежевыпущенные PEM — уже многострочные (идемпотентно).
        caPem = NormalizePem(caPem);
        caKeyPem = NormalizePem(caKeyPem);
        var (certPem, keyPem) = ValkeyPki.IssueNodeCertificate(caPem, caKeyPem, node, advertisedHost);
        return new Dictionary<string, string>
        {
            ["VALKEY_TLS_CERT"] = NormalizePem(certPem),
            ["VALKEY_TLS_KEY"] = NormalizePem(keyPem),
            ["VALKEY_TLS_CA"] = caPem,
        };
    }

    // Валидность факта по env (идемпотентность по факту, arch/21 §2): CA ==
    // ожидаемому, серт подписан им, key↔cert, SAN покрывает advertised,
    // срок жив. env==null/нет ключей — false (объекта/материала нет).
    internal static bool IsValidNodeEnv(
        IReadOnlyDictionary<string, string>? env, string advertisedHost, string caPem, TimeProvider clock)
    {
        try
        {
            if (env is null
                || !env.TryGetValue("VALKEY_TLS_CERT", out var certPem)
                || !env.TryGetValue("VALKEY_TLS_KEY", out var keyPem)
                || !env.TryGetValue("VALKEY_TLS_CA", out var envCa))
                return false;
            certPem = NormalizePem(certPem);
            keyPem = NormalizePem(keyPem);
            envCa = NormalizePem(envCa);
            if (envCa.Trim() != caPem.Trim())
                return false; // чужой/старый CA — перевыпуск
            if (!ValkeyPki.TryParseCertificate(certPem, out var cert) || cert is null)
                return false;
            using (cert)
            {
                if (!Shared.Tls.TlsChain.ValidateChain(cert, ParseCa(caPem)))
                    return false;
                // Ключ обязан соответствовать серту: неатомарное обновление могло
                // оставить несовпадающую пару — она проходит проверку наличия,
                // но валит boot ноды вечно.
                if (!KeyMatchesCertificate(keyPem, cert))
                    return false;
                if (cert.NotAfter < clock.GetUtcNow())
                    return false;
                var san = cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().FirstOrDefault();
                if (san is null)
                    return false;
                if (System.Net.IPAddress.TryParse(advertisedHost, out var ip))
                    return san.EnumerateIPAddresses().Contains(ip);
                return san.EnumerateDnsNames()
                    .Contains(advertisedHost, StringComparer.OrdinalIgnoreCase);
            }
        }
        catch (Exception e) when (e is ArgumentException or FormatException
            or ApplicationException or System.Security.Cryptography.CryptographicException)
        {
            return false; // битый PEM/ключ — перевыпуск
        }
    }

    // etcd-канон «одной строкой с \n» → многострочный PEM; границы сборки и
    // сверки — один хелпер (идемпотентен на уже-многострочном PEM: в base64-
    // теле и заголовках PEM нет последовательности «\n»).
    private static string NormalizePem(string value)
        => value.Replace("\\n", "\n", StringComparison.Ordinal);

    // Открытый ключ серта == публичная часть VALKEY_TLS_KEY (PKCS#8 RSA).
    private static bool KeyMatchesCertificate(string keyPem, X509Certificate2 cert)
    {
        using var key = System.Security.Cryptography.RSA.Create();
        key.ImportFromPem(keyPem);
        using var certKey = cert.GetRSAPublicKey();
        return certKey is not null
            && key.ExportSubjectPublicKeyInfo().AsSpan()
                .SequenceEqual(certKey.ExportSubjectPublicKeyInfo());
    }

    private static X509Certificate2 ParseCa(string caPem)
        => ValkeyPki.TryParseCertificate(caPem, out var ca) && ca is not null
            ? ca
            : throw new ArgumentException("ca_pem: невалидный PEM");
}
