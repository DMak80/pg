using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace KafkaWorker.IntegrationTests.Api;

// WAF-хост KafkaWorker для метрик: копия KafkaApiFactory БЕЗ RemoveAll<IHostedService>
// (OTel-MeterProvider — hosted-сервис; циклы на пустом etcd-фикстуре тикают успешно
// и бесшумно).
// Не sealed: кейсы с оверрайдом конфигурации наследуются (ApiKey в MetricsTests).
public class MetricsApiFactory(Etcd.EtcdFixture etcd) : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["KafkaWorker:Etcd:Endpoints:0"] = etcd.Endpoint,
            ["KafkaWorker:Docker:Hosts:0:Name"] = "local",
            ["KafkaWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
            ["KafkaWorker:Api:AdvertiseUrl"] = "http://localhost:9996",
            ["KafkaWorker:Api:EnableSeedEndpoint"] = "false",
            // WAF-транспорт in-memory — только без TLS (arch/16 §1.1); явный
            // ключ вместо зависимости от env-переменной чужих фабрик.
            ["KafkaWorker:Api:Tls:AllowInsecureHttp"] = "true",
        }));
    }
}

// Collection-fixture метрик KafkaWorker: отдельная от kafka-api (живые циклы).
public sealed class KafkaMetricsFixture : IAsyncLifetime
{
    public Etcd.EtcdFixture Etcd { get; } = new();

    public MetricsApiFactory Factory { get; }

    public KafkaMetricsFixture()
    {
        Factory = new MetricsApiFactory(Etcd);
    }

    public async ValueTask InitializeAsync() => await Etcd.InitializeAsync();

    public async ValueTask DisposeAsync() => await Factory.DisposeAsync();
}

[CollectionDefinition(Name)]
public sealed class KafkaMetricsCollection : ICollectionFixture<KafkaMetricsFixture>
{
    public const string Name = "kafka-metrics";
}

// Гарантированный env WAF-хостов сборки (arch/16 §1.1): in-memory конфиг из
// ConfigureWebHost применяется при Build(), а fail-fast ConfigureMtls в
// Program.Main исполняется раньше — env процесса должен быть выставлен до
// первого CreateClient. ModuleInitializer исполняется до любого кода сборки.
internal static class TestAssemblyTlsEnv
{
    [System.Runtime.CompilerServices.ModuleInitializer]
    internal static void Set()
        => Environment.SetEnvironmentVariable("KafkaWorker__Api__Tls__AllowInsecureHttp", "true");
}
