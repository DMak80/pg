using System.Security.Cryptography.X509Certificates;
using ValkeyWorker.App;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.Configuration;
using Shared.Tls;

namespace ValkeyWorker.App.Api;

// mTLS HTTP-грани воркера (arch/21 §1.1): вся грань (вкл. /healthz) —
// только TLS; клиентские серты — per-install API-CA (ClientCaPem|ClientCaPath).
// Вызывается на WebApplicationBuilder ДО Build() (ConfigureKestrel — этап
// хоста) — общий код Program.cs и MtlsApiTests. Сертификаты живут всё
// приложение: ClientCertificateValidation вызывается на КАЖДОМ хендшейке
// (никаких using — иначе use-after-dispose).
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
            return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs

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
                    $"ValkeyWorker:Api:Tls: ключ /workers/api_tls/valkeyworker бит ({managedCert.Error}) — "
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
                "ValkeyWorker:Api:Tls: серверный серт/ключ не заданы (VWK_API_TLS_CERT/KEY или *_PATH; etcd-ключ /workers/api_tls/valkeyworker; arch/21 §1.1)");

        var clientCa = LoadClientCa(tls) ?? throw new ApplicationException(
            "ValkeyWorker:Api:Tls: ClientCA не задан (VWK_API_TLS_CLIENT_CA[_PATH])");

        // Явный Listen подавляет default-URL (ASPNETCORE_HTTP_PORTS) — только mTLS.
        builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
            new HttpsConnectionAdapterOptions
            {
                ServerCertificate = serverCert,
                ClientCertificateMode = ClientCertificateMode.RequireCertificate,
                ClientCertificateValidation = (certificate, _, _) => TlsChain.ValidateChain(certificate, clientCa),
            })));
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
