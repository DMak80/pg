using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using PgWorker.App;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// WAF-хост PgWorker с настоящим etcd (fixture) и выключенными фоновыми циклами:
// loops не нужны для API-мутаций, а их тики в тесте — шум.
// Не sealed: кейсы с оверрайдом конфигурации наследуются (напр., seed-эндпоинт
// с EnableSeedEndpoint=false в SeedApiTests — последний источник конфига выигрывает).
public class PgWorkerApiFactory(Etcd.EtcdFixture etcd) : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["PgWorker:Etcd:Endpoints:0"] = etcd.Endpoint,
            ["PgWorker:Docker:Hosts:0:Name"] = "local",
            ["PgWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
            ["PgWorker:Api:AdvertiseUrl"] = "http://localhost:9999",
            // Seed-эндпоинт включён для кейсов наливки (SeedApiTests); выключенный
            // флаг проверяется отдельной фабрикой-оверрайдом.
            ["PgWorker:Api:EnableSeedEndpoint"] = "true",
        }));
        builder.ConfigureServices(services =>
            services.RemoveAll<IHostedService>()); // Reconcile/Keepalive/Snapshot не стартуют
    }
}

// Collection-fixture API-тестов: один etcd + одна WAF-фабрика на все Api-классы
// (Task 4/5/6). Env-секреты Д7 ставим ДО первого CreateClient (SecretsFromEnv
// читает переменные процесса при старте хоста) и убираем в Dispose.
public sealed class PgApiFixture : IAsyncLifetime
{
    public Etcd.EtcdFixture Etcd { get; } = new();

    public PgWorkerApiFactory Factory { get; }

    public PgApiFixture()
    {
        Environment.SetEnvironmentVariable("PGW_PG_SUPERUSER_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_PG_STANDBY_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_BUCKET_ADMIN_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_BUCKET_MOVER_PASSWORD", "x");
        Factory = new PgWorkerApiFactory(Etcd);
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
public sealed class PgApiCollection : ICollectionFixture<PgApiFixture>
{
    public const string Name = "pg-api";
}
