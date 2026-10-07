using System.Net;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Shared.Core.HealthChecks;
using Shared.Core.Hosting;
using Xunit;

namespace PgWorker.IntegrationTests.Hosting;

// Интеграционные кейсы watchdog (не docker): хост воркера с подменённой
// ILoopsVitality — устаревший тик инициирует graceful остановку в бюджет;
// свежие тики — хост живёт. Циклы не поднимаются (RemoveAll<IHostedService> +
// возврат hosted-обёртки LoopWatchdog), витальность — фейк с порогом по
// формулам от уменьшенных тестовых интервалов Loops (2/2 → healthz 21 c,
// watchdog ×2 = 42 c; тик устарел на 5 мин → firing первой проверкой).
[Collection(NonE2eCollection.Name)]
public sealed class LoopWatchdogTests
{
    private sealed class FakeVitality(bool stale) : ILoopsVitality
    {
        public IReadOnlyList<LoopHeartbeat> Snapshot()
        {
            var threshold = TimeSpan.FromSeconds(21) * 2; // FastLoops(2,2)=21 c ×2
            var at = stale ? DateTimeOffset.UtcNow - TimeSpan.FromMinutes(5)
                           : DateTimeOffset.UtcNow;
            return [new LoopHeartbeat("reconcile", at, threshold)];
        }
    }

    // Своя фабрика: циклы сняты, watchdog оставлен, витальность подменена.
    private sealed class WatchdogFactory(Etcd.EtcdFixture etcd, bool stale)
        : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["PgWorker:Etcd:Endpoints:0"] = etcd.Endpoint,
                    ["PgWorker:Docker:Hosts:0:Name"] = "local",
                    ["PgWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
                    ["PgWorker:Api:Tls:AllowInsecureHttp"] = "true",
                    ["PgWorker:Api:AdvertiseUrl"] = "https://localhost:9996",
                    ["PgWorker:Api:EnableSeedEndpoint"] = "false",
                    // Уменьшенные интервалы Loops (пороги порядка десятков секунд).
                    ["PgWorker:Loops:ScanIntervalSec"] = "2",
                    ["PgWorker:Loops:KeepaliveSec"] = "2",
                }));
            builder.ConfigureServices(services =>
            {
                services.RemoveAll<IHostedService>(); // циклы и MeterProvider не нужны
                services.RemoveAll<ILoopsVitality>();
                services.AddSingleton<ILoopsVitality>(_ => new FakeVitality(stale));
                // LoopWatchdog-синглтон уже зарегистрирован Program.cs (Enabled=true
                // из appsettings.json) — возвращаем hosted-обёртку над ним.
                services.AddHostedService(sp => sp.GetRequiredService<LoopWatchdog>());
            });
        }
    }

    // Свой etcd + своя фабрика на тест-класс (teardown при любом исходе).
    private sealed class WatchdogHost : IAsyncLifetime
    {
        public Etcd.EtcdFixture Etcd { get; } = new();
        private WebApplicationFactory<Program>? _factory;

        public WatchdogHost()
        {
            Environment.SetEnvironmentVariable("PGW_PG_SUPERUSER_PASSWORD", "x");
            Environment.SetEnvironmentVariable("PGW_PG_STANDBY_PASSWORD", "x");
            Environment.SetEnvironmentVariable("PGW_BUCKET_ADMIN_PASSWORD", "x");
            Environment.SetEnvironmentVariable("PGW_BUCKET_MOVER_PASSWORD", "x");
        }

        public WebApplicationFactory<Program> CreateFactory(bool stale)
        {
            _factory = new WatchdogFactory(Etcd, stale);
            return _factory;
        }

        public async ValueTask InitializeAsync() => await Etcd.InitializeAsync();

        public async ValueTask DisposeAsync()
        {
            if (_factory is { } factory)
                await factory.DisposeAsync();
            await Etcd.DisposeAsync();
            // env-секреты Д7 НЕ снимаем (канон RestartHost): фикстуры коллекции
            // non-e2e создаются заранее — снятие здесь ломает соседние классы.
        }
    }

    [Fact]
    public async Task StaleHeartbeat_HostStopsWithinBudget()
    {
        // Arrange: хост с устаревшим тиком; ApplicationStopping — маркер стопа
        await using var host = new WatchdogHost();
        await host.InitializeAsync();
        var factory = host.CreateFactory(stale: true);
        using var client = factory.CreateClient(); // хост собран и запущен
        var lifetime = factory.Services.GetRequiredService<IHostApplicationLifetime>();
        var stopping = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lifetime.ApplicationStopping.Register(() => stopping.TrySetResult());

        // Act: поллинг остановки — общий бюджет 30 c, шаг 1 c (AGENTS.base.md §12)
        var winner = await Task.WhenAny(stopping.Task, Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken));

        // Assert: watchdog уложил хост в бюджет самовосстановления (AC2)
        winner.Should().Be(stopping.Task);
    }

    [Fact]
    public async Task FreshHeartbeats_HostAlive_AndHealthzWatchdogSection()
    {
        // Arrange: хост со свежими тиками; ApplicationStopping — маркер стопа
        await using var host = new WatchdogHost();
        await host.InitializeAsync();
        var factory = host.CreateFactory(stale: false);
        using var client = factory.CreateClient();
        var lifetime = factory.Services.GetRequiredService<IHostApplicationLifetime>();
        var stopping = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lifetime.ApplicationStopping.Register(() => stopping.TrySetResult());

        // Act: 5 с наблюдения + GET /healthz + данные чека реального хоста
        // (грань /healthz отдаёт только статус-строку — секции читаем из
        // HealthCheckService того же DI-графа)
        await Task.Delay(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        using var response = await client.GetAsync("/healthz", TestContext.Current.CancellationToken);
        var report = await factory.Services.GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(TestContext.Current.CancellationToken);

        // Assert: хост жив; секция watchdog armed в данных /healthz (AC4)
        stopping.Task.IsCompleted.Should().BeFalse();
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        report.Entries["pgworker"].Data["watchdog"].ToString().Should().Be("armed; stale=нет");
    }
}
