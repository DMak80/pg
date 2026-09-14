using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.Configuration;
using PgWorker.App;
using Shared.Tls;

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
        => TlsEnv.ApplyEnvOverrides(EnvBindings, configuration, getenv);

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

    public static ApiTlsSetup ConfigureMtls(WebApplicationBuilder builder, ManagedCertRead? managedCert = null)
    {
        var tls = builder.Configuration.GetSection("PgWorker:Api:Tls").Get<TlsOptions>() ?? new TlsOptions();
        if (tls.AllowInsecureHttp)
            return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs

        // Управляемый серт: etcd-ключ > env (spec §3.2 п.1). Битый ключ —
        // fail-fast: ключ — явное намерение оператора.
        X509Certificate2? serverCert;
        string? source;
        string? warning = null;
        switch (managedCert?.Status)
        {
            case ManagedCertStatus.Found:
                serverCert = TlsMaterial.LoadPemPair(managedCert.CertPem!, managedCert.KeyPem!);
                source = "etcd:/workers/api_tls/pgworker";
                break;
            case ManagedCertStatus.Broken:
                throw new ApplicationException(
                    $"PgWorker:Api:Tls: ключ /workers/api_tls/pgworker бит ({managedCert.Error}) — "
                    + "исправьте из панели (PUT api-cert) или удалите (DELETE api-cert)");
            default:
                serverCert = LoadServerCertificate(tls);
                source = "env";
                if (managedCert?.Status == ManagedCertStatus.Unreachable)
                    warning = $"etcd недоступен ({managedCert.Error}) — стартую на env-серте";
                break;
        }

        if (serverCert is null)
            throw new ApplicationException(
                "PgWorker:Api:Tls: серверный серт/ключ не заданы (PGW_API_TLS_CERT/KEY или *_PATH; etcd-ключ /workers/api_tls/pgworker; arch/14 §1.1)");

        var clientCa = LoadClientCa(tls) ?? throw new ApplicationException(
            "PgWorker:Api:Tls: ClientCA не задан (PGW_API_TLS_CLIENT_CA[_PATH])");

        var port = ResolvePort(builder.Configuration);
        // Явный Listen подавляет default-URL — только mTLS-грань.
        builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
            new HttpsConnectionAdapterOptions
            {
                ServerCertificate = serverCert,
                ClientCertificateMode = ClientCertificateMode.RequireCertificate,
                ClientCertificateValidation = (certificate, _, _) => TlsChain.ValidateChain(certificate, clientCa),
            })));
        return new ApiTlsSetup(serverCert, source, warning);
    }

    // Итог конфигурации серта (spec §3.2 п.1): применённый серт + источник
    // ("etcd:/workers/api_tls/pgworker" | "env") + warning (etcd недоступен,
    // старт на env). ServerCert=null — AllowInsecureHttp (WAF-тесты).
    public sealed record ApiTlsSetup(X509Certificate2? ServerCert, string? Source, string? Warning);

    private static X509Certificate2? LoadServerCertificate(TlsOptions tls)
    {
        var certPem = tls.ServerCertPem ?? TlsMaterial.ReadPemFile(tls.ServerCertPath);
        var keyPem = tls.ServerKeyPem ?? TlsMaterial.ReadPemFile(tls.ServerKeyPath);
        if (certPem is null || keyPem is null)
            return null;
        return TlsMaterial.LoadPemPair(certPem, keyPem);
    }

    private static X509Certificate2? LoadClientCa(TlsOptions tls)
    {
        var caPem = tls.ClientCaPem ?? TlsMaterial.ReadPemFile(tls.ClientCaPath);
        return caPem is null ? null : TlsMaterial.LoadPem(caPem);
    }
}
