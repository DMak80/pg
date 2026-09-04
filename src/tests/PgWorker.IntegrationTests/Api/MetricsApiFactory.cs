using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using PgWorker.App;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// WAF-хост PgWorker для метрик: копия PgWorkerApiFactory БЕЗ RemoveAll<IHostedService>
// (OTel-MeterProvider — hosted-сервис; циклы оставляем живыми — etcd-фикстура
// настоящая, тики по пустому etcd успешны и бесшумны).
// Не sealed: кейсы с оверрайдом конфигурации наследуются (ApiKey в MetricsTests).
public class MetricsApiFactory(Etcd.EtcdFixture etcd) : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["PgWorker:Etcd:Endpoints:0"] = etcd.Endpoint,
            ["PgWorker:Docker:Hosts:0:Name"] = "local",
            ["PgWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
            ["PgWorker:Api:AdvertiseUrl"] = "http://localhost:9997",
            ["PgWorker:Api:EnableSeedEndpoint"] = "false",
        }));
        // hosted-сервисы НЕ выключаем: MeterProvider обязан жить для /metrics,
        // а циклы на пустом etcd-фикстуре тикают успешно и тихо.
    }
}

// Collection-fixture метрик: отдельная от pg-api — циклы живы, префикс 9997 не
// конфликтует с advertise API-серии; env-секреты Д7 до CreateClient (тот же
// паттерн PgApiFixture).
public sealed class PgMetricsFixture : IAsyncLifetime
{
    public Etcd.EtcdFixture Etcd { get; } = new();

    public MetricsApiFactory Factory { get; }

    public PgMetricsFixture()
    {
        Environment.SetEnvironmentVariable("PGW_PG_SUPERUSER_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_PG_STANDBY_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_BUCKET_ADMIN_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_BUCKET_MOVER_PASSWORD", "x");
        Factory = new MetricsApiFactory(Etcd);
    }

    public async ValueTask InitializeAsync() => await Etcd.InitializeAsync();

    public async ValueTask DisposeAsync()
    {
        await Factory.DisposeAsync();
        await Etcd.DisposeAsync();
        Environment.SetEnvironmentVariable("PGW_PG_SUPERUSER_PASSWORD", null);
        Environment.SetEnvironmentVariable("PGW_PG_STANDBY_PASSWORD", null);
        Environment.SetEnvironmentVariable("PGW_BUCKET_ADMIN_PASSWORD", null);
        Environment.SetEnvironmentVariable("PGW_BUCKET_MOVER_PASSWORD", null);
    }
}

[CollectionDefinition(Name)]
public sealed class PgMetricsCollection : ICollectionFixture<PgMetricsFixture>
{
    public const string Name = "pg-metrics";
}
