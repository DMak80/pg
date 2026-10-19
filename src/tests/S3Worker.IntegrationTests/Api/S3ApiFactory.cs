using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using S3Worker.IntegrationTests.Etcd;
using Xunit;

namespace S3Worker.IntegrationTests.Api;

// WAF-хост S3Worker с настоящим etcd (fixture) и выключенными фоновыми
// циклами (порт ValkeyApiFactory): loops не нужны для API-мутаций.
// AllowInsecureHttp — env-ом процесса: WAF-конфиг применяется при Build(),
// а mTLS-конфигурация Kestrel исполняется раньше (этап хоста).
// mTLS — MtlsApiTests на реальном сокете.
public class S3ApiFactory(S3EtcdFixture etcd, bool enableSeed = true) : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["S3Worker:Etcd:Endpoints:0"] = etcd.Endpoint,
            ["S3Worker:Bucket"] = "pgworker-backups",
            ["S3Worker:Docker:Hosts:0:Name"] = "local",
            ["S3Worker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
            ["S3Worker:Api:AdvertiseUrl"] = "https://localhost:9998",
            // mTLS — MtlsApiTests на реальном сокете; WAF-фабрика — insecure HTTP.
            ["S3Worker:Api:Tls:AllowInsecureHttp"] = "true",
            ["S3Worker:Api:EnableSeedEndpoint"] = enableSeed ? "true" : "false",
        }));
        builder.ConfigureServices(services =>
            services.RemoveAll<IHostedService>()); // Keepalive/Snapshot/Reconcile не стартуют
    }
}

// Collection-fixture API-тестов: один etcd + одна WAF-фабрика на Api-классы.
public sealed class S3ApiFixture : IAsyncLifetime
{
    public S3EtcdFixture Etcd { get; } = new();

    public S3ApiFactory Factory { get; }

    public S3ApiFixture()
    {
        Factory = new S3ApiFactory(Etcd);
    }

    public async ValueTask InitializeAsync() => await Etcd.InitializeAsync();

    public async ValueTask DisposeAsync()
    {
        await Factory.DisposeAsync();
        await Etcd.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class S3ApiCollection : ICollectionFixture<S3ApiFixture>
{
    public const string Name = "s3-api";
}

// Спец-ключ бэкапа etcd API-контура (значения — стендовые тестовые, не
// прод-секреты): полная env-пара для создания etcd-backup кластеров.
internal static class TestSecrets
{
    public static readonly string[] EnvNames =
    [
        "S3W_ETCD_BACKUP_ACCESSKEY",
        "S3W_ETCD_BACKUP_SECRETKEY",
    ];

    // Гарантированный env WAF-хостов сборки (fail-fast полупар в Program.Main
    // исполняется раньше конфига WAF): пара полная, значения — тестовые.
    [System.Runtime.CompilerServices.ModuleInitializer]
    internal static void Set()
    {
        Environment.SetEnvironmentVariable("S3Worker__Api__Tls__AllowInsecureHttp", "true");
        foreach (var name in EnvNames)
            Environment.SetEnvironmentVariable(name, "it-secret");
    }
}
