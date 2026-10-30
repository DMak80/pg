using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.Configuration;
using Shared.Tls;

namespace S3Worker.App.Api;

// HTTPS HTTP-грань воркера (arch/24 §1.1): клиентский серт опционален на
// хендшейке, mTLS обязателен только на /api (401 без валидного серта —
// Shared.Tls.ClientCertAuthMiddleware); /healthz и /metrics — открытая грань
// доверенной docker-сети. Вызывается на WebApplicationBuilder ДО Build()
// (ConfigureKestrel — этап хоста) — общий код Program.cs и MtlsApiTests.
// Сертификаты живут всё приложение (без using).
public static class TlsEndpoints
{
    // env-секреты → конфиг-дерево (arch/24 §8): PEM-значения и _PATH-файлы.
    public static readonly (string Env, string Key)[] EnvBindings =
    [
        ("S3W_API_TLS_CERT", "S3Worker:Api:Tls:ServerCertPem"),
        ("S3W_API_TLS_KEY", "S3Worker:Api:Tls:ServerKeyPem"),
        ("S3W_API_TLS_CLIENT_CA", "S3Worker:Api:Tls:ClientCaPem"),
        ("S3W_API_TLS_CERT_PATH", "S3Worker:Api:Tls:ServerCertPath"),
        ("S3W_API_TLS_KEY_PATH", "S3Worker:Api:Tls:ServerKeyPath"),
        ("S3W_API_TLS_CLIENT_CA_PATH", "S3Worker:Api:Tls:ClientCaPath"),
    ];

    // Перенос env → конфиг; getenv-инъекция — для юнит-теста (без окружения).
    public static void ApplyEnvOverrides(ConfigurationManager configuration, Func<string, string?>? getenv = null)
        => TlsEnv.ApplyEnvOverrides(EnvBindings, configuration, getenv);

    public static ApiTlsSetup ConfigureMtls(WebApplicationBuilder builder, int port, ManagedCertRead? managedCert = null)
    {
        var tls = builder.Configuration.GetSection("S3Worker:Api:Tls").Get<TlsOptions>() ?? new TlsOptions();
        if (tls.AllowInsecureHttp)
        {
            // WAF-режим (in-memory тесты, без TLS): middleware в pipeline у всех —
            // pass-through (Enabled=false).
            builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: false, ClientCa: null));
            return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs
        }

        // Управляемый серт: etcd-ключ > env (arch/24 §1.1). Битый ключ —
        // fail-fast: ключ — явное намерение оператора.
        X509Certificate2? serverCert;
        string? source;
        string? warning = null;
        switch (managedCert?.Status)
        {
            case ManagedCertStatus.Found:
                serverCert = TlsMaterial.LoadPemPair(managedCert.CertPem!, managedCert.KeyPem!);
                source = "etcd:/workers/api_tls/s3worker";
                break;
            case ManagedCertStatus.Broken:
                throw new ApplicationException(
                    $"S3Worker:Api:Tls: key /workers/api_tls/s3worker is broken ({managedCert.Error}) — "
                    + "fix it from the panel (PUT api-cert) or delete it (DELETE api-cert)");
            default:
                serverCert = LoadServerCertificate(tls);
                source = "env";
                if (managedCert?.Status == ManagedCertStatus.Unreachable)
                    warning = $"etcd unreachable ({managedCert.Error}) — starting on the env certificate";
                break;
        }

        if (serverCert is null)
            throw new ApplicationException(
                "S3Worker:Api:Tls: server certificate/key not set (S3W_API_TLS_CERT/KEY or *_PATH; etcd key /workers/api_tls/s3worker; arch/24 §1.1)");

        var clientCa = LoadClientCa(tls) ?? throw new ApplicationException(
            "S3Worker:Api:Tls: ClientCA not set (S3W_API_TLS_CLIENT_CA[_PATH])");

        // 401-политика /api (arch/24 §1.1): опция middleware — DI-синглтон;
        // clientCa живёт всё приложение (уже загружен без using, как серверный).
        builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: true, ClientCa: clientCa));

        // Явный Listen подавляет default-URL (ASPNETCORE_HTTP_PORTS) — только
        // HTTPS-грань. Серт запрашивается, но НЕ требуется (AllowCertificate) и
        // НЕ валидируется на хендшейке: валидация — в ClientCertAuthMiddleware
        // (401 только на /api; /healthz и /metrics — открытая грань).
        builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
            new HttpsConnectionAdapterOptions
            {
                ServerCertificate = serverCert,
                ClientCertificateMode = ClientCertificateMode.AllowCertificate,
            })));
        return new ApiTlsSetup(serverCert, source, warning);
    }

    // Итог конфигурации серта (arch/24 §1.1): применённый серт + источник
    // ("etcd:/workers/api_tls/s3worker" | "env") + warning (etcd недоступен,
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
