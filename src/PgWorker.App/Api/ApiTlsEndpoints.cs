using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.Configuration;
using PgWorker.App;

namespace PgWorker.App.Api;

// mTLS HTTP-грани PgWorker (arch/14 §1.1, t03): вся грань (вкл. /healthz и
// /metrics) — только TLS; клиентские серты — per-install API-CA (единая пакета
// с KafkaWorker, решение О1). Вызывается на WebApplicationBuilder ДО Build().
// Сертификаты живут всё приложение: ClientCertificateValidation — на каждом
// хендшейке (без using).
public static class ApiTlsEndpoints
{
    // env-секреты → конфиг-дерево (arch/14 §4): PEM-значения и _PATH-файлы.
    public static readonly (string Env, string Key)[] EnvBindings =
    [
        ("PGW_API_TLS_CERT", "PgWorker:Api:Tls:ServerCertPem"),
        ("PGW_API_TLS_KEY", "PgWorker:Api:Tls:ServerKeyPem"),
        ("PGW_API_TLS_CLIENT_CA", "PgWorker:Api:Tls:ClientCaPem"),
        ("PGW_API_TLS_CERT_PATH", "PgWorker:Api:Tls:ServerCertPath"),
        ("PGW_API_TLS_KEY_PATH", "PgWorker:Api:Tls:ServerKeyPath"),
        ("PGW_API_TLS_CLIENT_CA_PATH", "PgWorker:Api:Tls:ClientCaPath"),
    ];

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

    // Порт Kestrel: из urls/ASPNETCORE_URLS (E2E поднимает хост-процесс на
    // свободном порту; жёсткий 8080 kafka-прецедента НЕ переиспользуется),
    // иначе дефолт 8080.
    public static int ResolvePort(ConfigurationManager configuration)
    {
        var urls = configuration["urls"] ?? Environment.GetEnvironmentVariable("ASPNETCORE_URLS");
        if (string.IsNullOrWhiteSpace(urls))
            return 8080;
        foreach (var binding in urls.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Reverse())
        {
            if (Uri.TryCreate(binding, UriKind.Absolute, out var uri) && uri.Port > 0)
                return uri.Port;
        }

        return 8080;
    }

    public static void ConfigureMtls(WebApplicationBuilder builder)
    {
        var tls = builder.Configuration.GetSection("PgWorker:Api:Tls").Get<TlsOptions>() ?? new TlsOptions();
        if (tls.AllowInsecureHttp)
            return; // без TLS — только WAF-тесты; warning логирует Program.cs

        // Fail-fast при конфигурации хоста: серт/ключ/ClientCA обязаны быть заданы.
        var serverCert = LoadServerCertificate(tls) ?? throw new ApplicationException(
            "PgWorker:Api:Tls: серверный серт/ключ не заданы (PGW_API_TLS_CERT/KEY или *_PATH; arch/14 §1.1)");
        var clientCa = LoadClientCa(tls) ?? throw new ApplicationException(
            "PgWorker:Api:Tls: ClientCA не задан (PGW_API_TLS_CLIENT_CA[_PATH])");

        var port = ResolvePort(builder.Configuration);
        // Явный Listen подавляет default-URL — только mTLS-грань.
        builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
            new HttpsConnectionAdapterOptions
            {
                ServerCertificate = serverCert,
                ClientCertificateMode = ClientCertificateMode.RequireCertificate,
                ClientCertificateValidation = (certificate, _, _) => ValidateChain(certificate, clientCa),
            })));
    }

    // Валидация цепочки клиентского серта против per-install API-CA (копия
    // KafkaWorker.App/Api/TlsEndpoints.cs:63-75, тексты — PgWorker:Api:Tls).
    private static bool ValidateChain(X509Certificate2? certificate, X509Certificate2 clientCa)
    {
        if (certificate is null)
            return false;
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(clientCa);
        // Per-install приватная CA не публикует CRL/OCSP — онлайн-проверка отзыва
        // всегда падала бы и отвергала валидные клиентские серты.
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(certificate);
    }

    private static X509Certificate2? LoadServerCertificate(TlsOptions tls)
    {
        var certPem = tls.ServerCertPem ?? ReadFile(tls.ServerCertPath);
        var keyPem = tls.ServerKeyPem ?? ReadFile(tls.ServerKeyPath);
        if (certPem is null || keyPem is null)
            return null;

        // PFX round-trip: ключ из CreateFromPem эфемерный (не экспортируемый) —
        // SslStream (macOS) не может его использовать без ре-импорта.
        var pem = X509Certificate2.CreateFromPem(certPem, keyPem);
        return X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), null);
    }

    private static X509Certificate2? LoadClientCa(TlsOptions tls)
    {
        var caPem = tls.ClientCaPem ?? ReadFile(tls.ClientCaPath);
        if (caPem is null)
            return null;
        var ca = X509Certificate2.CreateFromPem(caPem);
        return OperatingSystem.IsMacOS()
            ? X509CertificateLoader.LoadPkcs12(ca.Export(X509ContentType.Pkcs12), null)
            : ca;
    }

    private static string? ReadFile(string? path)
        => path is null || !File.Exists(path) ? null : File.ReadAllText(path).Trim();
}
