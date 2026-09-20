using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Shared.Core.Planning;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Docker.Drivers;
using ValkeyWorker.Docker.Engine;
using Shared.Etcd.Coordination;
using Shared.Etcd.Maintenance;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// Фикстура Valkey-группы (порт KafkaClusterFixture): Testcontainers-etcd +
// ЛОКАЛЬНЫЙ docker-хост (воркер — хост-процесс теста). AdvertisedClientHost =
// localhost (host.docker.internal с macOS-хоста не резолвится; воркер и
// RESP-пробы — хост-процессы, порт опубликован на localhost). RunTag-guid в
// именах кластеров/контейнеров; NodeBootSec ≤ 100 (AGENTS.md); teardown
// демонтирует ВСЁ созданное + ассерт чистоты (ни контейнера vwk-* тега, ни
// ключа префикса).
public sealed class ValkeyClusterFixture : IAsyncLifetime
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

    public string Endpoint { get; private set; } = "";

    public EtcdGateway Gateway { get; private set; } = null!;

    public IClusterDriver Driver { get; private set; } = null!;

    // Advertised-хост клиентского порта для хост-процесса теста: localhost.
    public const string AdvertisedClientHost = "localhost";

    // Имя единственного docker-хоста рига (HostEndpoint в InitializeAsync);
    // portalloc/TLS-volume вызовы тестов ссылаются на него.
    public const string DockerHost = "local";

    // Окно хост-портов публикации нод — динамическое (вне стендовой зоны).
    private static readonly (int From, int To) HostPorts = FreePortWindow.Find();

    public int PortFrom => HostPorts.From;

    public int PortTo => HostPorts.To;

    // Уникальный тег прогона: имена кластеров (и контейнеров vwk-<C>-node<k>)
    // уникальны на каждый запуск.
    public string RunTag { get; } = Guid.NewGuid().ToString("N")[..8];

    private readonly List<string> _clusters = [];

    // Имя кластера с тегом прогона: fixture.Cluster("prov") → "prova1b2c3d4".
    public string Cluster(string name)
    {
        var cluster = $"{name}{RunTag}";
        _clusters.Add(cluster);
        return cluster;
    }

    // NodeBootSec ≤ 100 (AGENTS.md); NodeDeadSec — базовый 90 (сценарии
    // UNREACHABLE строят риг с коротким порогом).
    public ValkeyProvisioningOptions Options { get; } =
        new(HostPorts.From, HostPorts.To, NodeBootSec: 100, NodeDeadSec: 90, AdvertisedClientHost,
            "valkey/valkey:9.1.2");

    private int _portCursor;

    // Выделение host-порта из окна (тесты, поднимающие контейнер руками —
    // TlsMigrationTests): без литералов, из динамического окна фикстуры.
    public int NextPort() => HostPorts.From + 2 + Interlocked.Increment(ref _portCursor);

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _etcd.StartAsync(ct);
        Endpoint = $"http://localhost:{_etcd.GetMappedPublicPort(2379)}";
        Gateway = new EtcdGateway(_http);

        using var probeClient = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        for (var i = 0; i < 30; i++)
        {
            try
            {
                using var probe = await probeClient.PostAsync(
                    Endpoint + "/v3/maintenance/status",
                    new StringContent("{}", Encoding.UTF8, "application/json"),
                    ct);
                if (probe.IsSuccessStatusCode)
                    break;
            }
            catch (HttpRequestException)
            {
                // etcd ещё поднимается — ждём следующую попытку
            }

            await Task.Delay(1000, ct);
        }

        // docker-хост — локальный сокет хост-машины (Docker required).
        Driver = new PlainClusterDriver(
            [new HostEndpoint("local", "unix:///var/run/docker.sock")],
            new DockerEngineFactory());
    }

    public async ValueTask DisposeAsync()
    {
        // Демонтаж кластеров прогона (per-cluster сетей у домена нет; TLS-том
        // vwk-<C>-tls чистится всегда — успешный X1 снимает его сам, здесь
        // страховка для сценариев без демонтажа); 404/ошибки — лучшее усилие
        // (прогон завершён).
        var ct = CancellationToken.None;
        foreach (var cluster in _clusters)
        {
            try
            {
                var objects = await Driver.ListNodeObjectsAsync(cluster, ct);
                if (!objects.IsSuccess)
                    continue;
                foreach (var name in objects.Value)
                {
                    // vwk-<C>-node<k> → имя узла после префикса кластера.
                    var node = name[$"vwk-{cluster}-".Length..];
                    await Driver.RemoveNodeAsync(cluster, node, ct);
                }

                // TLS-volume кластера (t06, шаг 9.4): ПОСЛЕ сноса контейнеров —
                // volume «in use» docker удалять отказывается.
                await Driver.RemoveTlsVolumeAsync(cluster, ct);
            }
            catch
            {
                // чистка на выходе — ошибки не всплывают (прогон уже завершён)
            }
        }

        _http.Dispose();
        await _etcd.DisposeAsync();

        // Ассерт чистоты: после teardown ни контейнера vwk-<C>-* этого прогона.
        foreach (var cluster in _clusters)
        {
            var objects = await Driver.ListNodeObjectsAsync(cluster, ct);
            objects.IsSuccess.Should().BeTrue();
            objects.Value.Should().BeEmpty($"после teardown не осталось контейнеров {cluster}");
        }
    }

    // ── Риги процессов (все координационные типы — keyPrefix "/valkeyworker") ──

    public ClaimStore NewClaimStore() =>
        new("/valkeyworker", [Endpoint], Gateway, TimeProvider.System);

    public WorkJournal NewJournal() => new("/valkeyworker", Gateway, [Endpoint]);

    public PortAllocLock NewPortAllocLock(string instanceId) =>
        new("/valkeyworker", [Endpoint], Gateway, TimeProvider.System, instanceId);

    public PortAllocIndex NewPortAllocIndex() =>
        new(Gateway, [Endpoint], NullLogger<PortAllocIndex>.Instance);

    public ClusterSecretEnsurer NewSecretEnsurer() =>
        new(Gateway, [Endpoint]);

    public NodeTlsProvisioner NewTlsProvisioner() => new(Driver, Options.NodeImage);

    public ProvisioningProcess NewProvisioning(ClaimStore claims, WorkJournal journal,
        PortAllocLock portLock, PortAllocIndex portIndex, IClusterSecretEnsurer secrets) =>
        new(Gateway, [Endpoint], Driver, claims, journal, portLock, portIndex, secrets,
            NewTlsProvisioner(), new ValkeyConnection(TimeSpan.FromSeconds(2)), Options);

    public DeprovisioningProcess NewDeprovisioning(ClaimStore claims, WorkJournal journal) =>
        new(Gateway, [Endpoint], Driver, claims, journal);

    public NodeSupervisor NewSupervisor(ClaimStore claims, WorkJournal journal,
        PortAllocHealer healer, int nodeDeadSec = 90) =>
        new(Gateway, [Endpoint], Driver, claims, journal,
            new ValkeyConnection(TimeSpan.FromSeconds(2)),
            Options with { NodeDeadSec = nodeDeadSec }, healer, NewTlsProvisioner());

    public PortAllocHealer NewHealer(ClaimStore claims, WorkJournal journal, PortAllocLock portLock,
        PortAllocIndex portIndex) =>
        new(Gateway, [Endpoint], Driver, claims, journal, portLock, portIndex, Options);

    public ConfigConverger NewConverger() =>
        new(new ValkeyConnection(TimeSpan.FromSeconds(2)), Gateway, [Endpoint], NewJournal());

    public PasswordRotator NewRotator(ClaimStore claims, WorkJournal journal) =>
        new(Gateway, [Endpoint], claims, journal,
            new ValkeyConnection(TimeSpan.FromSeconds(2)), ValkeyPasswordGenerator.Generate);

    // Сид заявки кластера (config NOT_INITIALIZED + node1/state + resources).
    public async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        await Gateway.PutAsync(Endpoint, $"/valkey/clusters/{cluster}/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"NOT_INITIALIZED"}""",
            lease: null, ct);
        await Gateway.PutAsync(Endpoint, $"/valkey/clusters/{cluster}/nodes/node1/state",
            "NOT_INITIALIZED", lease: null, ct);
        await Gateway.PutAsync(Endpoint, $"/valkey/clusters/{cluster}/nodes/node1/resources",
            """{"cpu":"1","mem":"1Gi","disk":"10Gi"}""", lease: null, ct);
    }

    // Снапшот кластера из etcd (как ReconcileLoop).
    public async Task<ValkeyClusterSnapshot?> SnapshotAsync(string cluster)
    {
        var range = await Gateway.RangeAsync(Endpoint, "/valkey/clusters/", TestContext.Current.CancellationToken);
        if (!range.IsSuccess)
            throw new InvalidOperationException(
                $"range /valkey/clusters/ не удался (etcd фикстуры недоступен?): {range.Error?.Message}");

        return ValkeySnapshotParser.Parse(range.Value).Value.Clusters.FirstOrDefault(c => c.Cluster == cluster);
    }

    // Снапшот с гарантией присутствия (тесты сеют кластер перед вызовом).
    public async Task<ValkeyClusterSnapshot> RequireSnapshotAsync(string cluster)
        => await SnapshotAsync(cluster)
           ?? throw new InvalidOperationException($"кластер {cluster} не найден в etcd");

    public async Task<string?> GetAsync(string key)
    {
        var kv = await Gateway.GetAsync(Endpoint, key, TestContext.Current.CancellationToken);
        return kv.Value?.Value;
    }

    // Put-хелпер тестов (ключ → значение, без lease).
    public Task PutAsync(string key, string value)
        => Gateway.PutAsync(Endpoint, key, value, null, TestContext.Current.CancellationToken);

    // Del-хелпер тестов (точечное удаление — E9-сценарии).
    public Task DelAsync(string key)
        => Gateway.DeleteAsync(Endpoint, key, prefix: false, TestContext.Current.CancellationToken);

    // Поллинг условия с бюджетом ≤ 100 с (Task.Delay(500) в цикле — канон репо).
    public static async Task WaitAsync(Func<Task<bool>> condition, TimeSpan budget, string what)
    {
        var deadline = DateTimeOffset.UtcNow + budget;
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition())
                return;
            await Task.Delay(500, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException($"бюджет {budget.TotalSeconds:F0} c исчерпан: {what}");
    }
}
