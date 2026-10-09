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
            // WAF-хост без сертов: mTLS выключен (прод-канон — false, arch/14 §1.1).
            ["PgWorker:Api:Tls:AllowInsecureHttp"] = "true",
            ["PgWorker:Api:AdvertiseUrl"] = "https://localhost:9999",
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

    // Статический тестовый REST-CA (t22, arch/14 §4 гр.3): один RSA-2048 CA
    // на фикстуру — WAF-хосты получают его через те же env PGW_REST_TLS_*,
    // что и прод (двойной семантики нет; fail-fast не роняет серии).
    public static (string CaPem, string CaKeyPem) RestTestCa => RestCa.Value;
    private static readonly Lazy<(string CaPem, string CaKeyPem)> RestCa =
        new(() => E2e.E2eTestPki.GenerateCa("waf-rest"));

    public PgApiFixture()
    {
        Environment.SetEnvironmentVariable("PGW_PG_SUPERUSER_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_PG_STANDBY_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_BUCKET_ADMIN_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_BUCKET_MOVER_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_REST_TLS_CA", RestTestCa.CaPem);
        Environment.SetEnvironmentVariable("PGW_REST_TLS_CA_KEY", RestTestCa.CaKeyPem);
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
        Environment.SetEnvironmentVariable("PGW_REST_TLS_CA", null);
        Environment.SetEnvironmentVariable("PGW_REST_TLS_CA_KEY", null);
    }
}
