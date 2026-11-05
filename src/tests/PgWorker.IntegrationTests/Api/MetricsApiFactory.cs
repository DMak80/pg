using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using PgWorker.App;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// WAF-хост PgWorker для метрик: копия PgWorkerApiFactory БЕЗ RemoveAll<IHostedService>
// (OTel-MeterProvider — hosted-сервис; циклы оставляем живыми — etcd-фикстура
// настоящая, тики по пустому etcd успешны и бесшумны).
// Не sealed: кейсы с оверрайдом конфигурации наследуются (MtlsApiTests).
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
            // WAF-хост без сертов: mTLS выключен (прод-канон — false, arch/pgworker/07-api.md §1).
            ["PgWorker:Api:Tls:AllowInsecureHttp"] = "true",
            ["PgWorker:Api:AdvertiseUrl"] = "https://localhost:9997",
            ["PgWorker:Api:EnableSeedEndpoint"] = "false",
        }));
        // hosted-сервисы НЕ выключаем: MeterProvider обязан жить для /metrics,
        // а циклы на пустом etcd-фикстуре тикают успешно и тихо.
    }
}

// Класс-фикстура метрик (per-class, параллелизм §5): свой etcd + своя
// MetricsApiFactory на класс; hosted-сервисы живы (MeterProvider для
// /metrics). Env-секреты — STATIC-ONCE (идентичны PgApiFixture, живут весь
// процесс: снятие в Dispose гонялось бы с параллельным CreateClient).
// Слот Guard'а — на всю жизнь класса, вес — по владельцам типа фикстуры.
public sealed class PgMetricsFixture : IAsyncLifetime
{
    public Etcd.EtcdFixture Etcd { get; } = new();

    public MetricsApiFactory Factory { get; }

    private static readonly Lazy<bool> EnvOnce = new(() =>
    {
        Environment.SetEnvironmentVariable("PGW_PG_SUPERUSER_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_PG_STANDBY_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_BUCKET_ADMIN_PASSWORD", "x");
        Environment.SetEnvironmentVariable("PGW_BUCKET_MOVER_PASSWORD", "x");
        return true;
    });

    public PgMetricsFixture()
    {
        _ = EnvOnce.Value;
        Factory = new MetricsApiFactory(Etcd);
    }

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await IntParallelismGuard.AcquireAsync(
            Shared.TestInfra.Parallelism.TestClassWeight.OfFixture(GetType()), "pgmet", ct);
        try
        {
            await Etcd.InitializeAsync();
        }
        catch
        {
            IntParallelismGuard.Release("pgmet");
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
            IntParallelismGuard.Release("pgmet");
        }
    }
}

// Env-флаг до первого Program.Main процесса тестов (WAF-хосты не задают серты).
internal static class TestEnv
{
    [System.Runtime.CompilerServices.ModuleInitializer]
    internal static void SetInsecureEnv()
        => Environment.SetEnvironmentVariable("PgWorker__Api__Tls__AllowInsecureHttp", "true");
}
