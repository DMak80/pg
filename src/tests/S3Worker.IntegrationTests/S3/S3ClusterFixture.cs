using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;
using Shared.Etcd.Maintenance;
using S3Worker.Core.Model;
using S3Worker.Core.OwnS3;
using S3Worker.Docker.Drivers;
using S3Worker.Etcd.Parsing;
using S3Worker.Provisioning;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.IntegrationTests.S3;

// Фикстура S3-группы (порт ValkeyClusterFixture): Testcontainers-etcd +
// ЛОКАЛЬНЫЙ docker-хост (воркер — хост-процесс теста). AdvertisedHost =
// localhost (воркер и health-пробы — хост-процессы, порт опубликован на
// localhost). RunTag-guid в именах кластеров/контейнеров s3w-<C>-node<k>;
// NodeBootSec ≤ 100 (AGENTS.md); teardown демонтирует ВСЁ созданное + ассерт
// чистоты (ни контейнера/тома s3w-<C>-*, ни ключа префикса).
public sealed class S3ClusterFixture : IAsyncLifetime
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

    public IS3ClusterDriver Driver { get; private set; } = null!;

    // Advertised-хост S3-порта для хост-процесса теста: localhost.
    public const string AdvertisedHost = "localhost";

    // Имя единственного docker-хоста рига (HostEndpoint в InitializeAsync).
    public const string DockerHost = "local";

    // Окно хост-портов публикации нод — динамическое (вне стендовой зоны).
    private static readonly (int From, int To) HostPorts = FreePortWindow.Find();

    public int PortFrom => HostPorts.From;

    public int PortTo => HostPorts.To;

    // Уникальный тег прогона: имена кластеров (и контейнеров s3w-<C>-node<k>)
    // уникальны на каждый запуск.
    public string RunTag { get; } = Guid.NewGuid().ToString("N")[..8];

    private readonly List<string> _clusters = [];

    // Имя кластера с тегом прогона.
    public string Cluster(string name)
    {
        var cluster = $"{name}{RunTag}";
        _clusters.Add(cluster);
        return cluster;
    }

    // Образ ownS3 (owns3:e2e-it, сборка раз на процесс) + порт 9000.
    public string OwnS3Image => S3OwnS3Image.Tag;

    // NodeBootSec ≤ 100 (AGENTS.md).
    public S3ProvisioningOptions Options { get; } =
        new(HostPorts.From, HostPorts.To, NodeBootSec: 100, NodeDeadSec: 90, HealthIntervalSec: 15,
            AdvertisedHost, "owns3:e2e-it", "pgworker-backups");

    // Демо-профиль секретов рига (значения — тестовые).
    public OwnS3NodeSecrets Secrets { get; } =
        new("it-root", "it-root-password", [new OwnS3AccessKey("it-rw-key", "it-rw-secret", "read-write")]);

    public SecretProfileStore SecretStore { get; }

    public S3ClusterFixture()
    {
        var values = new Dictionary<string, string?>
        {
            ["S3W_NODE_SECRETS__backups-main__ROOT_USER"] = Secrets.RootUser,
            ["S3W_NODE_SECRETS__backups-main__ROOT_PASSWORD"] = Secrets.RootPassword,
            ["S3W_NODE_SECRETS__backups-main__ACCESS_KEYS__0__ACCESSKEY"] = "it-rw-key",
            ["S3W_NODE_SECRETS__backups-main__ACCESS_KEYS__0__SECRETKEY"] = "it-rw-secret",
            ["S3W_NODE_SECRETS__backups-main__ACCESS_KEYS__0__POLICY"] = "read-write",
        };
        SecretStore = new SecretProfileStore(
            name => name is not null && values.TryGetValue(name, out var v) ? v : null,
            values.Keys);
    }

    public HttpClient NodeHttp { get; } = new() { Timeout = TimeSpan.FromSeconds(7.5) };

    public IOwnS3NodeClient NodeClient { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        NodeClient = new OwnS3NodeClient(NodeHttp);
        var ct = TestContext.Current.CancellationToken;
        var artifactsDir = EnsureArtifactsDir();
        await S3OwnS3Image.BuildOnceAsync(artifactsDir, ct);
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
        // Телеметрия ДО удаления (docs/e2e-launch.md): списки объектов прогона.
        var artifactsDir = EnsureArtifactsDir();
        try
        {
            var lines = new List<string>();
            foreach (var cluster in _clusters)
            {
                var objects = await Driver.ListNodeObjectsAsync(cluster, CancellationToken.None);
                lines.Add($"{cluster}: {(objects.IsSuccess ? string.Join(",", objects.Value) : objects.Error?.Message)}");
            }

            await File.WriteAllLinesAsync(Path.Combine(artifactsDir, "objects-final.txt"), lines);
        }
        catch
        {
            // телеметрия — лучшее усилие
        }

        // Демонтаж кластеров прогона (тома включительно); ошибки — лучшее усилие.
        var ct = CancellationToken.None;
        foreach (var cluster in _clusters)
        {
            try
            {
                var objects = await Driver.ListNodeObjectsAsync(cluster, ct);
                if (!objects.IsSuccess)
                    continue;
                var prefix = $"s3w-{cluster}-";
                var nodes = objects.Value
                    .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
                    .Select(n => n[prefix.Length..])
                    .Select(n => n.EndsWith("-data", StringComparison.Ordinal) ? n[..^5] : n)
                    .Distinct()
                    .ToList();
                foreach (var node in nodes)
                    await Driver.RemoveNodeAsync(cluster, node, removeVolume: true, ct);
            }
            catch
            {
                // чистка на выходе — ошибки не всплывают (прогон уже завершён)
            }
        }

        NodeHttp.Dispose();
        _http.Dispose();
        await _etcd.DisposeAsync();

        // Ассерт чистоты: после teardown ни контейнера/тома s3w-<C>-* прогона.
        foreach (var cluster in _clusters)
        {
            var objects = await Driver.ListNodeObjectsAsync(cluster, ct);
            objects.IsSuccess.Should().BeTrue();
            objects.Value.Should().BeEmpty($"после teardown не осталось объектов {cluster}");
        }
    }

    // ── Риги процессов (координация — keyPrefix "/s3worker") ──

    public ClaimStore NewClaimStore() =>
        new("/s3worker", [Endpoint], Gateway, TimeProvider.System);

    public WorkJournal NewJournal() => new("/s3worker", Gateway, [Endpoint]);

    public PortAllocLock NewPortAllocLock(string instanceId) =>
        new("/s3worker", [Endpoint], Gateway, TimeProvider.System, instanceId);

    public PortAllocIndex NewPortAllocIndex() =>
        new(Gateway, [Endpoint], NullLogger<PortAllocIndex>.Instance);

    public PortAllocHealer NewHealer(ClaimStore claims, WorkJournal journal,
        PortAllocLock portLock, PortAllocIndex portIndex) =>
        new(Gateway, [Endpoint], Driver, claims, journal, portLock, portIndex, Options);

    public PkiStore NewPki() => new(Gateway, [Endpoint], TimeProvider.System);

    public MeshGovernanceProcess NewMesh(ClaimStore claims, WorkJournal journal) =>
        new(Gateway, [Endpoint], claims, journal, SecretStore, NewPki(), NodeClient, Options,
            new System.Diagnostics.Metrics.Meter("S3Worker-tests"));

    public ProvisioningProcess NewProvisioning(ClaimStore claims, WorkJournal journal,
        PortAllocLock portLock, PortAllocIndex portIndex) =>
        new(Gateway, [Endpoint], Driver, claims, journal, portLock, portIndex,
            SecretStore, NewPki(), new InstanceEnvComposer(),
            NewMesh(claims, journal), NodeClient, Options, new BootWaitBudget(TimeProvider.System));

    public DeprovisioningProcess NewDeprovisioning(ClaimStore claims, WorkJournal journal) =>
        new(Gateway, [Endpoint], Driver, claims, journal, new BootWaitBudget(TimeProvider.System));

    public NodeSupervisor NewSupervisor(ClaimStore claims, WorkJournal journal,
        PortAllocHealer healer, int nodeDeadSec = 90) =>
        new(Gateway, [Endpoint], Driver, claims, journal, NodeClient, SecretStore,
            NewPki(), new InstanceEnvComposer(),
            Options with { NodeDeadSec = nodeDeadSec }, healer);

    // Сид заявки кластера (config NOT_INITIALIZED + instances[] с именами —
    // arch/24 §1.1: placement/resources/state на каждый инстанс).
    public async Task SeedClusterAsync(string cluster, params string[] instances)
    {
        var ct = TestContext.Current.CancellationToken;
        if (instances.Length == 0)
            instances = ["node1"];
        await Gateway.PutAsync(Endpoint, $"/s3/clusters/{cluster}/config",
            $$"""{"platform":"owns3","nodes":{{instances.Length}},"secret_profile":"backups-main","created_unix":1756500000,"state":"NOT_INITIALIZED"}""",
            lease: null, ct);
        foreach (var instance in instances)
        {
            await Gateway.PutAsync(Endpoint, $"/s3/clusters/{cluster}/nodes/{instance}/state",
                "NOT_INITIALIZED", lease: null, ct);
            await Gateway.PutAsync(Endpoint, $"/s3/clusters/{cluster}/nodes/{instance}/placement",
                $$"""{"docker_host":"{{DockerHost}}"}""", lease: null, ct);
            await Gateway.PutAsync(Endpoint, $"/s3/clusters/{cluster}/nodes/{instance}/resources",
                """{"cpu":"1","mem":"1Gi","disk":"5Gi"}""", lease: null, ct);
        }
    }

    // Снапшот кластера из etcd (как ReconcileLoop).
    public async Task<S3ClusterSnapshot?> SnapshotAsync(string cluster)
    {
        var range = await Gateway.RangeAsync(Endpoint, "/s3/", TestContext.Current.CancellationToken);
        if (!range.IsSuccess)
            throw new InvalidOperationException(
                $"range /s3/ не удался (etcd фикстуры недоступен?): {range.Error?.Message}");

        return S3SnapshotParser.Parse(range.Value).Value.Clusters.FirstOrDefault(c => c.Cluster == cluster);
    }

    public async Task<S3ClusterSnapshot> RequireSnapshotAsync(string cluster)
        => await SnapshotAsync(cluster)
           ?? throw new InvalidOperationException($"кластер {cluster} не найден в etcd");

    public async Task<string?> GetAsync(string key)
    {
        var kv = await Gateway.GetAsync(Endpoint, key, TestContext.Current.CancellationToken);
        return kv.Value?.Value;
    }

    public Task PutAsync(string key, string value)
        => Gateway.PutAsync(Endpoint, key, value, null, TestContext.Current.CancellationToken);

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

    private static string EnsureArtifactsDir()
    {
        var dir = Path.Combine("/tmp", $"pgw-e2e-artifacts-{Guid.NewGuid().ToString("N")[..8]}");
        Directory.CreateDirectory(dir);
        return dir;
    }
}

[CollectionDefinition(Name)]
public sealed class S3ClusterCollection : ICollectionFixture<S3ClusterFixture>
{
    public const string Name = "s3-cluster";
}
