using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Shared.Core.Planning;
using Shared.Etcd.Client;
using ValkeyWorker.Docker.Drivers;
using ValkeyWorker.Docker.Engine;
using ValkeyWorker.IntegrationTests.Valkey;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Api;

// Live-фикстура фактических имён словаря arch/18 §2.6 (риск M3, spec §9): WAF-хост
// ValkeyWorker с ЖИВЫМИ hosted-циклами, реальным docker-сокетом и собственным etcd.
// Сид заявки → ReconcileLoop поднимает ноду → коллектор собирает INFO → /metrics
// содержит все 15 канонических имён. Полный teardown при любом исходе + ассерт
// чистоты (AGENTS.base §11): ни контейнера vwk-<тега>, ни ключей префикса.
public sealed class ValkeyMetricsDomainFixture : IAsyncLifetime
{
    private readonly IContainer _etcd = new ContainerBuilder("quay.io/coreos/etcd:v3.5.21")
        .WithCommand(
            "etcd",
            "--name=test",
            "--data-dir=/etcd-data",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://127.0.0.1:2379")
        .WithPortBinding(2379, assignRandomHostPort: true)
        .Build();

    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };

    // Динамическое окно хост-портов публикации нод — вне стендовой зоны 17000–17999.
    private static readonly (int From, int To) HostPorts = FreePortWindow.Find();

    public string Cluster { get; } = $"mtr{Guid.NewGuid():N}"[..12];

    public MetricsLiveFactory Factory { get; private set; } = null!;

    private PlainClusterDriver? Driver { get; set; }

    private string Endpoint { get; set; } = "";

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _etcd.StartAsync(ct);
        Endpoint = $"http://localhost:{_etcd.GetMappedPublicPort(2379)}";

        // Проба готовности etcd (паттерн ValkeyClusterFixture).
        using var probe = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        for (var i = 0; i < 30; i++)
        {
            try
            {
                using var resp = await probe.PostAsync(Endpoint + "/v3/maintenance/status",
                    new StringContent("{}", Encoding.UTF8, "application/json"), ct);
                if (resp.IsSuccessStatusCode)
                    break;
            }
            catch (HttpRequestException)
            {
                // etcd ещё поднимается
            }

            await Task.Delay(1000, ct);
        }

        // Сид заявки (формат SeedClusterAsync) — до старта WAF: циклы возьмут сразу.
        var gateway = new EtcdGateway(_http);
        await gateway.PutAsync(Endpoint, $"/valkey/clusters/{Cluster}/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"NOT_INITIALIZED"}""",
            lease: null, ct);
        await gateway.PutAsync(Endpoint, $"/valkey/clusters/{Cluster}/nodes/node1/state",
            "NOT_INITIALIZED", lease: null, ct);
        await gateway.PutAsync(Endpoint, $"/valkey/clusters/{Cluster}/nodes/node1/resources",
            """{"cpu":"1","mem":"1Gi","disk":"10Gi"}""", lease: null, ct);

        Factory = new MetricsLiveFactory(Endpoint, HostPorts.From, HostPorts.To);
        Driver = new PlainClusterDriver(
            [new HostEndpoint("local", "unix:///var/run/docker.sock")],
            new DockerEngineFactory());
    }

    // Диагностика провала (телеметрия AGENTS.md): снапшот etcd-ключей домена
    // + список docker-объектов кластера — без перезапуска теста отвечает,
    // на какой стадии оборвался сценарий (сид → циклы → docker → коллектор).
    public async Task<string> DumpDiagnosticsAsync()
    {
        var sb = new StringBuilder();
        var gateway = new EtcdGateway(_http);
        foreach (var prefix in new[] { "/valkey/clusters/", "/valkeyworker/" })
        {
            var range = await gateway.RangeAsync(Endpoint, prefix, CancellationToken.None);
            sb.AppendLine($"── {prefix} (IsSuccess={range.IsSuccess}):");
            if (range.IsSuccess)
                foreach (var kv in range.Value)
                    sb.AppendLine($"  {kv.Key} = {kv.Value}");
            else
                sb.AppendLine($"  error: {range.Error!.Message}");
        }

        if (Driver is not null)
        {
            var objects = await Driver.ListNodeObjectsAsync(Cluster, CancellationToken.None);
            sb.AppendLine($"── docker-объекты vwk-{Cluster}-* (IsSuccess={objects.IsSuccess}):");
            if (objects.IsSuccess)
                foreach (var name in objects.Value)
                    sb.AppendLine($"  {name}");
            else
                sb.AppendLine($"  error: {objects.Error!.Message}");
        }

        sb.AppendLine("── логи хоста (≥ Warning, последние 40):");
        foreach (var line in MetricsLiveFactory.LogSink.TakeLast(40))
            sb.AppendLine($"  {line}");

        return sb.ToString();
    }

    public async ValueTask DisposeAsync()
    {
        // Демонтаж при ЛЮБОМ исходе: WAF (останавливает циклы), контейнеры ноды,
        // etcd, временный каталог снапшотов.
        if (Factory is not null)
        {
            var snapshotDir = Factory.SnapshotDir;
            await Factory.DisposeAsync();
            try
            {
                if (Directory.Exists(snapshotDir))
                    Directory.Delete(snapshotDir, recursive: true);
            }
            catch
            {
                // уборка tmp — не повод валить прогон
            }
        }

        var ct = CancellationToken.None;
        if (Driver is not null)
        {
            try
            {
                var objects = await Driver.ListNodeObjectsAsync(Cluster, ct);
                if (objects.IsSuccess)
                    foreach (var name in objects.Value)
                        await Driver.RemoveNodeAsync(Cluster, name[$"vwk-{Cluster}-".Length..], ct);
            }
            catch
            {
                // чистка на выходе — ошибки не всплывают
            }

            try
            {
                // TLS-volume кластера (t06): миграция в живом контуре пишет
                // vwk-<C>-tls, X1-демонтажа у сценария нет — снимаем руками.
                // Отдельный try: сбой сноса контейнеров не должен оставлять том.
                await Driver.RemoveTlsVolumeAsync(Cluster, ct);
            }
            catch
            {
                // чистка на выходе — ошибки не всплывают
            }
        }

        _http.Dispose();
        await _etcd.DisposeAsync();

        // Ассерт чистоты: ни контейнера vwk-<C>-* этого прогона.
        if (Driver is not null)
            (await Driver.ListNodeObjectsAsync(Cluster, ct)).Value.Should().BeEmpty(
                "после teardown не осталось контейнеров прогона");
    }
}

// WAF-хост с реальным docker-сокетом и быстрыми тиками: циклы поднимают ноду,
// коллектор собирает, /metrics — in-memory (AllowInsecureHttp — только WAF-транспорт).
public sealed class MetricsLiveFactory(string etcdEndpoint, int portFrom, int portTo)
    : WebApplicationFactory<Program>
{
    // Захват логов хоста (≥ Warning): консоль WAF в dotnet test не видна, а
    // причина провала provisioning-тика живёт именно в warning/error-строках.
    public static readonly System.Collections.Concurrent.ConcurrentQueue<string> LogSink = new();

    // Каталог снапшотов etcd (ValkeyWorker:Snapshots:Dir): дефолт /snapshots
    // валиден в контейнере воркера, но не на хосте WAF (read-only /) —
    // provisioning V0 (snapshot-before) обязан уметь его писать.
    public string SnapshotDir { get; } = Path.Combine(
        Path.GetTempPath(), $"vwk-mtr-snapshots-{Guid.NewGuid():N}");

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureLogging(lb =>
        {
            lb.SetMinimumLevel(LogLevel.Warning);
            lb.AddProvider(new CapturingLoggerProvider(
                (_, level) => level >= LogLevel.Warning, line => LogSink.Enqueue(line)));
        });
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ValkeyWorker:Etcd:Endpoints:0"] = etcdEndpoint,
            ["ValkeyWorker:Docker:Hosts:0:Name"] = "local",
            ["ValkeyWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/docker.sock",
            ["ValkeyWorker:Docker:PortRange:From"] = portFrom.ToString(),
            ["ValkeyWorker:Docker:PortRange:To"] = portTo.ToString(),
            ["ValkeyWorker:AdvertisedClientHost"] = "localhost",
            ["ValkeyWorker:Api:AdvertiseUrl"] = "https://localhost:9997",
            ["ValkeyWorker:Api:EnableSeedEndpoint"] = "false",
            ["ValkeyWorker:Api:Tls:AllowInsecureHttp"] = "true",
            ["ValkeyWorker:Loops:ScanIntervalSec"] = "2",
            ["ValkeyWorker:Loops:KeepaliveSec"] = "2",
            ["ValkeyWorker:Thresholds:NodeBootSec"] = "100",
            ["ValkeyWorker:Metrics:CollectIntervalSec"] = "2",
            ["ValkeyWorker:Snapshots:Dir"] = SnapshotDir,
        }));
    }
}

public sealed class MetricsDomainTests(ValkeyMetricsDomainFixture fx) : IClassFixture<ValkeyMetricsDomainFixture>
{
    // Все 15 канонических имён §2.6 — факт экспорта против словаря (риск M3).
    private static readonly string[] CanonicalNames =
    [
        "valkey_memory_used_bytes", "valkey_memory_max_bytes",
        "valkey_connected_clients", "valkey_blocked_clients",
        "valkey_evicted_keys", "valkey_expired_keys",
        "valkey_keyspace_hits", "valkey_keyspace_misses",
        "valkey_instantaneous_ops_per_sec",
        "valkey_total_connections_received", "valkey_rejected_connections",
        "valkey_total_commands_processed",
        "valkey_role", "valkey_connected_slaves",
        "valkey_collector_last_success_timestamp_seconds",
    ];

    // AAA: живой кластер (hosted-циклы подняли ноду из сида) → все 15 имён словаря
    // экспортированы фактически, scope — ValkeyWorker, role — master (standalone).
    [Fact]
    public async Task Metrics_ЖивойКластер_Все15ИмёнСловаря()
    {
        // Arrange: фикстура засеяла заявку; WAF с циклами (NodeBootSec ≤ 100).
        using var client = fx.Factory.CreateClient();

        // Act: поллинг до первого INFO-сбора (бюджет ≤ 100 с: цикл 2 с + boot + сбор 2 с).
        string body = "";
        for (var i = 0; i < 100; i++)
        {
            body = await client.GetStringAsync("/metrics", TestContext.Current.CancellationToken);
            if (body.Contains("valkey_memory_used_bytes"))
                break;
            await Task.Delay(1000, TestContext.Current.CancellationToken);
        }

        // Маркер не появился — дамп состояния домена в сообщение провала
        // (диагностика «на какой стадии оборвалось», без повторного прогона).
        if (!body.Contains("valkey_memory_used_bytes"))
            Assert.Fail($"серия valkey_memory_used_bytes не экспортирована за бюджет.\n{await fx.DumpDiagnosticsAsync()}");

        // Assert: фактические имена против словаря §2.6.
        foreach (var name in CanonicalNames)
            body.Should().Contain(name, "серия {0} обязана экспортироваться при живой ноде", name);
        body.Should().Contain("role=\"master\"", "standalone-нода — master");
        body.Should().Contain("otel_scope_name=\"ValkeyWorker\"");
    }
}

// Минимальный ILoggerProvider-захватчик: пишёт строки в очередь фикстуры
// (диагностика провала без перезапуска; консоль WAF в dotnet test не видна).
internal sealed class CapturingLoggerProvider(
    Func<string, LogLevel, bool> filter,
    Action<string> sink) : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName)
        => new CapturingLogger(categoryName, filter, sink);

    public void Dispose() { }

    private sealed class CapturingLogger(
        string category,
        Func<string, LogLevel, bool> filter,
        Action<string> sink) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => filter(category, logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
                return;
            sink($"[{logLevel}] {category}: {formatter(state, exception)}");
            if (exception is not null)
                sink(exception.ToString());
        }
    }
}
