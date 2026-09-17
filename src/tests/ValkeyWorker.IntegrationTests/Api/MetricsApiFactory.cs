using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using ValkeyWorker.IntegrationTests.Etcd;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Api;

// WAF-хост ValkeyWorker для метрик: копия ValkeyApiFactory БЕЗ
// RemoveAll<IHostedService> (OTel-MeterProvider — hosted-сервис; циклы на
// пустом etcd-фикстуре тикают успешно и бесшумно).
public class MetricsApiFactory(ValkeyEtcdFixture etcd) : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ValkeyWorker:Etcd:Endpoints:0"] = etcd.Endpoint,
            ["ValkeyWorker:Docker:Hosts:0:Name"] = "local",
            ["ValkeyWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
            ["ValkeyWorker:Api:AdvertiseUrl"] = "https://localhost:9996",
            ["ValkeyWorker:Api:EnableSeedEndpoint"] = "false",
            // WAF-транспорт in-memory — только без TLS (arch/21 §1.1).
            ["ValkeyWorker:Api:Tls:AllowInsecureHttp"] = "true",
        }));
    }
}

// Collection-fixture метрик: отдельная от valkey-api (живые циклы).
public sealed class ValkeyMetricsFixture : IAsyncLifetime
{
    public ValkeyEtcdFixture Etcd { get; } = new();

    public MetricsApiFactory Factory { get; }

    public ValkeyMetricsFixture()
    {
        Factory = new MetricsApiFactory(Etcd);
    }

    public async ValueTask InitializeAsync() => await Etcd.InitializeAsync();

    public async ValueTask DisposeAsync() => await Factory.DisposeAsync();
}

[CollectionDefinition(Name)]
public sealed class ValkeyMetricsCollection : ICollectionFixture<ValkeyMetricsFixture>
{
    public const string Name = "valkey-metrics";
}
