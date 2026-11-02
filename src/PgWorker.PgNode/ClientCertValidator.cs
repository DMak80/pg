using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace PgWorker.PgNode;

/// <summary>Клиентский валидатор mTLS-канала доставки (spec §8-26, MR-#5):
/// ОТДЕЛЬНЫЙ от общего TlsChain.ValidateChain (прецедент t04 — общая цепочка
/// не знает о роли-семантике канала). Серт клиента валиден ⇔ цепочка к
/// per-install CA (env PGW_AGENT_CLIENT_CA) + EKU ClientAuth (1.3.6.1.5.5.7.3.2)
/// + subject CN == pgworker. Серверный серт ноды (EKU ServerAuth, CN — имя
/// ноды) отвергается.</summary>
public static class ClientCertValidator
{
    private const string ExpectedCn = "pgworker";
    private static readonly Oid ClientAuthOid = new("1.3.6.1.5.5.7.3.2");

    public static bool Validate(X509Certificate? certificate, X509Certificate2 ca)
    {
        if (certificate is not X509Certificate2 client)
        {
            if (certificate is null)
                return false;
            try { client = new X509Certificate2(certificate); }
            catch { return false; }
        }

        // Цепочка к per-install CA издателя.
        using var chain = new X509Chain
        {
            ChainPolicy =
            {
                RevocationMode = X509RevocationMode.NoCheck,
                TrustMode = X509ChainTrustMode.CustomRootTrust,
                CustomTrustStore = { ca },
            },
        };
        if (!chain.Build(client))
            return false;

        // EKU: обязателен ClientAuth (серверный серт ноды его не несёт).
        var hasClientAuth = false;
        foreach (var ext in client.Extensions.OfType<X509EnhancedKeyUsageExtension>())
            if (ext.EnhancedKeyUsages.OfType<Oid>().Any(o => o.Value == ClientAuthOid.Value))
                hasClientAuth = true;
        if (!hasClientAuth)
            return false;

        // CN == pgworker (серверный серт ноды несёт имя ноды).
        var cn = client.GetNameInfo(X509NameType.SimpleName, false);
        return string.Equals(cn, ExpectedCn, StringComparison.Ordinal);
    }
}
