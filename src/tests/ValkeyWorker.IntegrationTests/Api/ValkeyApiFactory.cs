using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ValkeyWorker.IntegrationTests.Etcd;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Api;

// WAF-хост ValkeyWorker с настоящим etcd (fixture) и выключенными фоновыми
// циклами (порт KafkaApiFactory): loops не нужны для API-мутаций, а их тики в
// тесте — шум. AllowInsecureHttp — env-ом процесса: WAF-конфиг применяется при
// Build(), а mTLS-конфигурация Kestrel исполняется раньше (этап хоста).
// mTLS — MtlsApiTests на реальном сокете.
// Не sealed: кейсы с оверрайдом конфигурации наследуются.
public class ValkeyApiFactory(ValkeyEtcdFixture etcd) : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ValkeyWorker:Etcd:Endpoints:0"] = etcd.Endpoint,
            ["ValkeyWorker:Docker:Hosts:0:Name"] = "local",
            ["ValkeyWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
            ["ValkeyWorker:Api:AdvertiseUrl"] = "https://localhost:9997",
            // mTLS — MtlsApiTests на реальном сокете; WAF-фабрика — insecure HTTP.
            ["ValkeyWorker:Api:Tls:AllowInsecureHttp"] = "true",
            // Seed-эндпоинт включён для кейсов наливки; выключенный флаг
            // проверяется отдельной фабрикой-оверрайдом.
            ["ValkeyWorker:Api:EnableSeedEndpoint"] = "true",
        }));
        builder.ConfigureServices(services =>
            services.RemoveAll<IHostedService>()); // Keepalive/Snapshot/Reconcile не стартуют
    }
}

// Collection-fixture API-тестов: один etcd + одна WAF-фабрика на Api-классы.
public sealed class ValkeyApiFixture : IAsyncLifetime
{
    public ValkeyEtcdFixture Etcd { get; } = new();

    public ValkeyApiFactory Factory { get; }

    public ValkeyApiFixture()
    {
        Factory = new ValkeyApiFactory(Etcd);
    }

    public async ValueTask InitializeAsync() => await Etcd.InitializeAsync();

    public async ValueTask DisposeAsync()
    {
        await Factory.DisposeAsync();
        await Etcd.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class ValkeyApiCollection : ICollectionFixture<ValkeyApiFixture>
{
    public const string Name = "valkey-api";
}

// Гарантированный env WAF-хостов сборки (arch/21 §1.1): in-memory конфиг из
// ConfigureWebHost применяется при Build(), а fail-fast ConfigureMtls в
// Program.Main исполняется раньше — env процесса должен быть выставлен до
// первого CreateClient. ModuleInitializer исполняется до любого кода сборки.
internal static class TestAssemblyTlsEnv
{
    [System.Runtime.CompilerServices.ModuleInitializer]
    internal static void Set()
        => Environment.SetEnvironmentVariable("ValkeyWorker__Api__Tls__AllowInsecureHttp", "true");
}
