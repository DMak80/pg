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
            // WAF-хост без сертов: mTLS выключен (прод-канон — false, arch/pgworker/07-api.md §1).
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

// Класс-фикстура API-тестов (per-class, параллелизм §5): свой etcd + своя
// WAF-фабрика на каждый Api-класс. Env-секреты Д7 — STATIC-ONCE: значения
// идентичны у всех фикстур, параллельные классы читают их при старте WAF-хоста
// (SecretsFromEnv читает переменные процесса при первом CreateClient);
// снятие в Dispose гонялось бы с чужим параллельным CreateClient — env живут
// весь процесс (значения тестовые, утечки за пределы прогона нет).
// Слот Guard'а — на всю жизнь класса (etcd-контур фикстуры), вес — по
// владельцам типа фикстуры (канон §5).
public sealed class PgApiFixture : IAsyncLifetime
{
    public Etcd.EtcdFixture Etcd { get; } = new();

    public PgWorkerApiFactory Factory { get; }

    // Статический тестовый REST-CA (arch/pgworker/04-security.md гр.3): один RSA-2048 CA
    // на фикстуру — WAF-хосты получают его через те же env PGW_REST_TLS_*,
    // что и прод (двойной семантики нет; fail-fast не роняет серии).
    public static (string CaPem, string CaKeyPem) RestTestCa => RestCa.Value;
    private static readonly Lazy<(string CaPem, string CaKeyPem)> RestCa =
        new(() => E2e.E2eTestPki.GenerateCa("waf-rest"));

    // Env-секреты один раз на процесс (идентичны у всех API-фикстур).
    private static readonly Lazy<bool> EnvOnce = new(() =>
    {
        Environment.SetEnvironmentVariable("PGW_PG_SUPERUSER_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_PG_STANDBY_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_BUCKET_ADMIN_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_BUCKET_MOVER_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_REST_TLS_CA", RestTestCa.CaPem);
        Environment.SetEnvironmentVariable("PGW_REST_TLS_CA_KEY", RestTestCa.CaKeyPem);
        return true;
    });

    public PgApiFixture()
    {
        _ = EnvOnce.Value;
        Factory = new PgWorkerApiFactory(Etcd);
    }

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await IntParallelismGuard.AcquireAsync(
            Shared.TestInfra.Parallelism.TestClassWeight.OfFixture(GetType()), "pgapi", ct);
        try
        {
            await Etcd.InitializeAsync();
        }
        catch
        {
            IntParallelismGuard.Release("pgapi");
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Слот — при любом исходе teardown (см. OwnedEtcdFixture.DisposeAsync).
        try
        {
            await Factory.DisposeAsync();
            await Etcd.DisposeAsync();
        }
        finally
        {
            IntParallelismGuard.Release("pgapi");
        }
    }
}
