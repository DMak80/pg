using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.Configuration;
using Shared.Tls;

namespace S3Worker.App.Api;

// mTLS HTTP-грань воркера (arch/24 §1.1): вся грань (вкл. /healthz) —
// только TLS; клиентские серты — per-install API-CA (ClientCaPem|ClientCaPath).
// Вызывается на WebApplicationBuilder ДО Build() (ConfigureKestrel — этап
// хоста) — общий код Program.cs и MtlsApiTests. Сертификаты живут всё
// приложение: ClientCertificateValidation вызывается на КАЖДОМ хендшейке
// (никаких using — иначе use-after-dispose).
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
            return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs

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
                    $"S3Worker:Api:Tls: ключ /workers/api_tls/s3worker бит ({managedCert.Error}) — "
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
                "S3Worker:Api:Tls: серверный серт/ключ не заданы (S3W_API_TLS_CERT/KEY или *_PATH; etcd-ключ /workers/api_tls/s3worker; arch/24 §1.1)");

        var clientCa = LoadClientCa(tls) ?? throw new ApplicationException(
            "S3Worker:Api:Tls: ClientCA не задан (S3W_API_TLS_CLIENT_CA[_PATH])");

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
