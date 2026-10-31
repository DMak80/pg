using System.Security.Cryptography.X509Certificates;
using ValkeyWorker.App;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.Configuration;
using Shared.Tls;

namespace ValkeyWorker.App.Api;

// HTTPS HTTP-грань воркера (arch/21 §1.1): клиентский серт опционален на
// хендшейке, mTLS обязателен только на /api (401 без валидного серта —
// Shared.Tls.ClientCertAuthMiddleware); /healthz и /metrics — открытая грань
// доверенной docker-сети. Вызывается на WebApplicationBuilder ДО Build()
// (ConfigureKestrel — этап хоста) — общий код Program.cs и MtlsApiTests.
// Сертификаты живут всё приложение (без using).
public static class TlsEndpoints
{
    // env-секреты → конфиг-дерево (arch/21 §8): PEM-значения и _PATH-файлы.
    public static readonly (string Env, string Key)[] EnvBindings =
    [
        ("VWK_API_TLS_CERT", "ValkeyWorker:Api:Tls:ServerCertPem"),
        ("VWK_API_TLS_KEY", "ValkeyWorker:Api:Tls:ServerKeyPem"),
        ("VWK_API_TLS_CLIENT_CA", "ValkeyWorker:Api:Tls:ClientCaPem"),
        ("VWK_API_TLS_CERT_PATH", "ValkeyWorker:Api:Tls:ServerCertPath"),
        ("VWK_API_TLS_KEY_PATH", "ValkeyWorker:Api:Tls:ServerKeyPath"),
        ("VWK_API_TLS_CLIENT_CA_PATH", "ValkeyWorker:Api:Tls:ClientCaPath"),
    ];

    // Перенос env → конфиг; getenv-инъекция — для юнит-теста (без окружения).
    public static void ApplyEnvOverrides(ConfigurationManager configuration, Func<string, string?>? getenv = null)
        => TlsEnv.ApplyEnvOverrides(EnvBindings, configuration, getenv);

    public static ApiTlsSetup ConfigureMtls(WebApplicationBuilder builder, int port, ManagedCertRead? managedCert = null)
    {
        var tls = builder.Configuration.GetSection("ValkeyWorker:Api:Tls").Get<TlsOptions>() ?? new TlsOptions();
        if (tls.AllowInsecureHttp)
        {
            // WAF-режим (in-memory тесты, без TLS): middleware в pipeline у всех —
            // pass-through (Enabled=false).
            builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: false, ClientCa: null));
            return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs
        }

        // Управляемый серт: etcd-ключ > env (arch/21 §1.1). Битый ключ —
        // fail-fast: ключ — явное намерение оператора.
        X509Certificate2? serverCert;
        string? source;
        string? warning = null;
        switch (managedCert?.Status)
        {
            case ManagedCertStatus.Found:
                serverCert = TlsMaterial.LoadPemPair(managedCert.CertPem!, managedCert.KeyPem!);
                source = "etcd:/workers/api_tls/valkeyworker";
                break;
            case ManagedCertStatus.Broken:
                throw new ApplicationException(
                    $"ValkeyWorker:Api:Tls: key /workers/api_tls/valkeyworker is broken ({managedCert.Error}) — "
                    + "fix it from the panel (PUT api-cert) or delete it (DELETE api-cert)");
            default:
                serverCert = LoadServerCertificate(tls);
                source = "env";
                if (managedCert?.Status == ManagedCertStatus.Unreachable)
                    warning = $"etcd unreachable ({managedCert.Error}) — starting with the env certificate";
                break;
        }

        if (serverCert is null)
            throw new ApplicationException(
                "ValkeyWorker:Api:Tls: server certificate/key is not set (VWK_API_TLS_CERT/KEY or *_PATH; etcd key /workers/api_tls/valkeyworker; arch/21 §1.1)");

        var clientCa = LoadClientCa(tls) ?? throw new ApplicationException(
            "ValkeyWorker:Api:Tls: ClientCA is not set (VWK_API_TLS_CLIENT_CA[_PATH])");

        // 401-политика /api (arch/21 §1.1): опция middleware — DI-синглтон;
        // clientCa живёт всё приложение (уже загружен без using, как серверный).
        builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: true, ClientCa: clientCa));

        // Явный Listen подавляет default-URL (ASPNETCORE_HTTP_PORTS) — только
        // HTTPS-грань. Серт запрашивается, но НЕ требуется (AllowCertificate) и
        // НЕ валидируется на хендшейке: без AllowAnyClientCertificate Kestrel
        // применяет дефолтную системную валидацию и рвёт хендшейк. Валидация —
        // в ClientCertAuthMiddleware (401 только на /api; /healthz и /metrics —
        // открытая грань).
        // AllowAnyClientCertificate() мутирует опцию до захвата её listener'ом; вызываем до ConfigureKestrel для определённости.
        var httpsOptions = new HttpsConnectionAdapterOptions
        {
            ServerCertificate = serverCert,
            ClientCertificateMode = ClientCertificateMode.AllowCertificate,
        };
        httpsOptions.AllowAnyClientCertificate();
        builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(httpsOptions)));
        return new ApiTlsSetup(serverCert, source, warning);
    }

    // Итог конфигурации серта (arch/21 §1.1): применённый серт + источник
    // ("etcd:/workers/api_tls/valkeyworker" | "env") + warning (etcd недоступен,
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
