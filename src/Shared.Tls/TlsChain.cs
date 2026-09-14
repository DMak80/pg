using System.Security.Cryptography.X509Certificates;

namespace Shared.Tls;

// Валидация цепочки серта против приватной per-install CA (t08: 4 копии слиты;
// сигнатура — как DockerTlsMaterial.ValidateChain: принимает X509Certificate?).
public static class TlsChain
{
    public static bool ValidateChain(X509Certificate? certificate, X509Certificate2 ca)
    {
        var cert2 = certificate as X509Certificate2
            ?? (certificate is null ? null : new X509Certificate2(certificate));
        if (cert2 is null)
            return false;
        using var chain = new X509Chain();
        // Per-install приватная CA не публикует CRL/OCSP — онлайн-проверка отзыва
        // всегда падала бы и отвергала валидные клиентские серты.
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(ca);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(cert2);
    }
}
