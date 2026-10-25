using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Amazon.Runtime;

namespace PgWorker.Backups;

/// <summary>Фабрика HttpClient AWS SDK с доверенным per-install CA
/// (t42, spec §5 fallback): AWSSDK.Core не читает env AWS_CA_BUNDLE
/// (доказано E2E t42 — сборка строку не содержит, соединение даёт PartialChain) —
/// доверие CA https S3-endpoint программное: RemoteCertificateValidationCallback
/// сверяет корень цепочки с CA из S3CaBundlePath (механизм тот же CA
/// kfw-install-ca, контракт arch/19 §7 не меняется). null-чек клиента,
/// лежащего на http-endpoint, не нужен: фабрика ставится ТОЛЬКО при заданном
/// S3CaBundlePath — поведение без пути прежнее (BackupS3).</summary>
public sealed class S3CaHttpClientFactory : HttpClientFactory
{
    private readonly X509Certificate2 _ca;
    private readonly string _caThumbprint;

    public S3CaHttpClientFactory(X509Certificate2 ca)
    {
        _ca = ca;
        _caThumbprint = ca.Thumbprint;
    }

    public override HttpClient CreateHttpClient(IClientConfig clientConfig)
    {
        // SocketsHttpHandler: HttpClientHandler.SslOptions в net10.0 отсутствует
        // (проверено компиляцией) — транспорт с настройкой TLS прямо здесь.
        var handler = new System.Net.Http.SocketsHttpHandler
        {
            SslOptions = new SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = (_, c, chain, _) =>
                    c is not null && chain is not null && ValidateCertificate(c, chain),
            },
        };
        return new HttpClient(handler);
    }

    /// <summary>Валидация цепочки сервера против per-install CA: корень обязан
    /// нести отпечаток CA. CA — в ExtraStore: без доверенного стора (macOS)
    /// цепочка строится одним элементом (leaf) и сверка корня не срабатывает
    /// (эмпирический факт E2E t42) — с ExtraStore цепочка [leaf, ca].
    /// Hostname/SAN цели НЕ пиннится (осознанно, п.10 ревью t25): ownS3-leaf
    /// несёт SAN канона owns3/03 §6 (DNS owns3/owns3-ec/localhost), а ноды
    /// адресуют ownS3 по алиасам контура, в SAN не входящим — hostname-чек
    /// сломал бы штатный https-контур; граница доверия — per-install CA
    /// (класс цепочки [leaf, install-CA], чужой CA отвергается тестом).</summary>
    public bool ValidateCertificate(X509Certificate certificate, X509Chain chain)
    {
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
        chain.ChainPolicy.ExtraStore.Add(_ca);
        if (!chain.Build(new X509Certificate2(certificate)))
            return false;
        var root = chain.ChainElements[^1].Certificate;
        return string.Equals(root.Thumbprint, _caThumbprint, StringComparison.OrdinalIgnoreCase);
    }
}
