using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.Configuration;
using PgWorker.App;
using Shared.Tls;

namespace PgWorker.App.Api;

// HTTPS HTTP-грань PgWorker (arch/14 §1.1): клиентский серт опционален на
// хендшейке, mTLS обязателен только на /api (401 без валидного серта —
// Shared.Tls.ClientCertAuthMiddleware); /healthz и /metrics — открытая грань
// доверенной docker-сети. Вызывается на WebApplicationBuilder ДО Build().
// Сертификаты живут всё приложение (без using).
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
        {
            // WAF-режим (in-memory тесты, без TLS): middleware в pipeline у всех —
            // pass-through (Enabled=false).
            builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: false, ClientCa: null));
            return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs
        }

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
                    $"PgWorker:Api:Tls: key /workers/api_tls/pgworker is broken ({managedCert.Error}) — "
                    + "fix it from the panel (PUT api-cert) or delete it (DELETE api-cert)");
            default:
                serverCert = LoadServerCertificate(tls);
                source = "env";
                if (managedCert?.Status == ManagedCertStatus.Unreachable)
                    warning = $"etcd unreachable ({managedCert.Error}) — starting on env cert";
                break;
        }

        if (serverCert is null)
            throw new ApplicationException(
                "PgWorker:Api:Tls: server cert/key not set (PGW_API_TLS_CERT/KEY or *_PATH; etcd key /workers/api_tls/pgworker; arch/14 §1.1)");

        var clientCa = LoadClientCa(tls) ?? throw new ApplicationException(
            "PgWorker:Api:Tls: ClientCA not set (PGW_API_TLS_CLIENT_CA[_PATH])");

        // 401-политика /api (arch/14 §1.1): опция middleware — DI-синглтон;
        // clientCa живёт всё приложение (уже загружен без using, как серверный).
        builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: true, ClientCa: clientCa));

        var port = ResolvePort(builder.Configuration);
        // Явный Listen подавляет default-URL — только HTTPS-грань. Серт
        // запрашивается, но НЕ требуется (AllowCertificate) и НЕ валидируется
        // на хендшейке: без AllowAnyClientCertificate Kestrel применяет
        // дефолтную системную валидацию цепочки и рвёт хендшейк. Валидация —
        // в ClientCertAuthMiddleware (401 только на /api; /healthz и /metrics
        // — открытая грань доверенной docker-сети).
        // AllowAnyClientCertificate — void (мутирует объект), поэтому до ConfigureKestrel.
        var httpsOptions = new HttpsConnectionAdapterOptions
        {
            ServerCertificate = serverCert,
            ClientCertificateMode = ClientCertificateMode.AllowCertificate,
        };
        httpsOptions.AllowAnyClientCertificate();
        builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(httpsOptions)));
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
