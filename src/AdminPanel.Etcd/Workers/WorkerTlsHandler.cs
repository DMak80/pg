using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Configuration;

namespace AdminPanel.Etcd.Workers;

// HTTP-handler обращений в API ОБОИХ воркеров (arch/02 §2.3.2, t03): клиентский
// серт панели — ЕДИНЫЙ на pgworker и kafkaworker (одна per-install API-CA) +
// доверие ServerCA (валидация цепочки на каждый хендшейк — серты живут время
// жизни handler'а, БЕЗ using). Для http://-запросов (dev/локальные) TLS-опции
// не применяются.
public static class WorkerTlsHandler
{
    // env-секреты → конфиг-дерево (arch/02 §2.3.2): PEM-значения и _PATH-файлы.
    public static readonly (string Env, string Key)[] EnvBindings =
    [
        ("WORKERS_PANEL_TLS_CERT", "AdminPanel:Workers:WorkerTls:ClientCertPem"),
        ("WORKERS_PANEL_TLS_KEY", "AdminPanel:Workers:WorkerTls:ClientKeyPem"),
        ("WORKERS_PANEL_TLS_SERVER_CA", "AdminPanel:Workers:WorkerTls:ServerCaPem"),
        ("WORKERS_PANEL_TLS_CERT_PATH", "AdminPanel:Workers:WorkerTls:ClientCertPath"),
        ("WORKERS_PANEL_TLS_KEY_PATH", "AdminPanel:Workers:WorkerTls:ClientKeyPath"),
        ("WORKERS_PANEL_TLS_SERVER_CA_PATH", "AdminPanel:Workers:WorkerTls:ServerCaPath"),
    ];

    // Перенос env → конфиг; getenv-инъекция — для юнит-теста (без окружения).
    public static void ApplyEnvOverrides(ConfigurationManager configuration, Func<string, string?>? getenv = null)
    {
        getenv ??= Environment.GetEnvironmentVariable;
        foreach (var (env, key) in EnvBindings)
        {
            var value = getenv(env);
            if (!string.IsNullOrWhiteSpace(value))
                configuration[key] = value;
        }
    }

    // trustedThumbprints — живые thumbprint'ы целевых сертов воркеров (из
    // снапшотов): панель доверяет сертам, которые сама записала (spec §3.3 п.3,
    // arch/adminpanel/02 §9.9) — иначе self-signed генерация рвала бы доступ
    // к перезапущенному воркеру.
    public static HttpMessageHandler Build(
        WorkerTlsOptions tls, Func<IReadOnlyCollection<string>>? trustedThumbprints = null)
    {
        var handler = new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(5) };
        var certPem = tls.ClientCertPem ?? ReadFile(tls.ClientCertPath);
        var keyPem = tls.ClientKeyPem ?? ReadFile(tls.ClientKeyPath);
        var serverCaPem = tls.ServerCaPem ?? ReadFile(tls.ServerCaPath);
        X509Certificate2? ca = null;
        if (certPem is not null && keyPem is not null)
        {
            // PFX round-trip: ключ CreateFromPem эфемерный — macOS SslStream
            // требует ре-импорт (прод — Linux, паттерн переносим).
            var pem = X509Certificate2.CreateFromPem(certPem, keyPem);
            var clientCert = X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), null);
            handler.SslOptions.ClientCertificates = new X509CertificateCollection { clientCert };
        }

        if (serverCaPem is not null)
            ca = X509Certificate2.CreateFromPem(serverCaPem);
        if (ca is not null || trustedThumbprints is not null)
            handler.SslOptions.RemoteCertificateValidationCallback =
                (_, certificate, _, _) =>
                {
                    // Колбэк отдаёт X509Certificate — построим X509Certificate2.
                    var cert2 = certificate as X509Certificate2
                        ?? (certificate is null ? null : new X509Certificate2(certificate));
                    if (cert2 is null)
                        return false;
                    // Цепочка к per-install ServerCA ИЛИ доверие по thumbprint.
                    if (ca is not null && ValidateChain(cert2, ca))
                        return true;
                    var thumbprint = Convert.ToHexString(
                        System.Security.Cryptography.SHA256.HashData(cert2.RawData)).ToLowerInvariant();
                    return trustedThumbprints?.Invoke().Contains(thumbprint) == true;
                };

        return handler;
    }

    // Валидация цепочки серверного серта против per-install ServerCA.
    private static bool ValidateChain(X509Certificate2? certificate, X509Certificate2 ca)
    {
        if (certificate is null)
            return false;
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(ca);
        // Приватная CA без CRL/OCSP — онлайн-проверка всегда падала бы.
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(certificate);
    }

    private static string? ReadFile(string? path)
        => path is null || !File.Exists(path) ? null : File.ReadAllText(path).Trim();
}
