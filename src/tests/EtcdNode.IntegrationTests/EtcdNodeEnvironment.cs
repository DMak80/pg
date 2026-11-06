using System.Text;
using Amazon.S3;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using EtcdNode.Core;
using Xunit;

namespace EtcdNode.IntegrationTests;

// Окружение EtcdNode-интеграций (arch/etcdnode/01-agent.md): контур ИЗ ОБРАЗА
// pgworker/etcdnode:dev — ДВА узла (etcd + supervisord + живой агент) в
// testcontainers-сети etcdn-<guid>-net, общий initial-cluster, host-порт
// клиентской грани — только node1, динамический (AGENTS.md). Агентам —
// БЫСТРЫЕ интервалы (MIN/RETRY 1 с) для поведенческих кейсов фильтра.
// ownS3-нода цели — лениво (образ owns3:dev внешний, порт 9000 динамический).
// Полный teardown при любом исходе: ключи домена/посева → контейнеры →
// именованные тома → сеть; успех dispose — ассерт чистоты (канон
// arch/testing/02-e2e-isolation.md).
public sealed class EtcdNodeEnvironment : IAsyncLifetime
{
    // Уникальный тег прогона: имена сети/томов уникальны на каждый запуск.
    public string RunTag { get; } = Guid.NewGuid().ToString("N")[..8];

    public string NetworkName => $"etcdn-{RunTag}-net";

    private readonly INetwork _network;
    private IContainer _node1 = null!;
    private IContainer _node2 = null!;

    // ownS3-нода цели слепков: поднимается ЛЕНИВО (только кейсам выгрузки
    // нужна); образ owns3:dev внешний (репозиторий ../OwnS3).
    private IContainer? _ownS3;

    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };

    // Клиентская грань node1 (динамический host-порт).
    public string Endpoint { get; private set; } = "";

    public EtcdGateway Gateway { get; }

    public EtcdNodeEnvironment()
    {
        _network = new NetworkBuilder()
            .WithName(NetworkName)
            .Build();
        Gateway = new EtcdGateway(_http);
    }

    private const string InitialCluster = "node1=http://node1:2380,node2=http://node2:2380";

    // Узел контура: образ агента, ETCD_ARGS общим составом, агент —
    // ускоренные интервалы (значения остальных — дефолты env узла,
    // arch/etcdnode/01-agent.md §3). withClientHostPort — публикация
    // динамического host-порта client-грани (только node1, AGENTS.md).
    private IContainer BuildNode(string name, bool withClientHostPort)
    {
        var builder = new ContainerBuilder("pgworker/etcdnode:dev")
            .WithNetwork(_network)
            .WithNetworkAliases(name)
            .WithEnvironment("ETCD_ARGS",
                $"--name={name} --data-dir=/etcd-data"
                + $" --listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://{name}:2379"
                + $" --listen-peer-urls=http://0.0.0.0:2380 --initial-advertise-peer-urls=http://{name}:2380"
                + $" --initial-cluster={InitialCluster} --initial-cluster-state=new")
            .WithEnvironment("ETCDNODE_NODE_NAME", name)
            .WithEnvironment("ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC", "1")
            .WithEnvironment("ETCDNODE_SNAPSHOTS__RETRY_INTERVAL_SEC", "1")
            .WithEnvironment("ETCDNODE_SNAPSHOTS__TIMEOUT_SEC", "10")
            .WithEnvironment("ETCDNODE_SNAPSHOTS__INTERVAL_MIN", "360")
            .WithEnvironment("ETCDNODE_SNAPSHOTS__RETENTION_FILES", "10")
            .WithEnvironment("ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS", "28")
            .WithEnvironment("ETCDNODE_SNAPSHOTS__MAINTENANCE_INTERVAL_MIN", "60")
            // Именованные тома (не анонимные VOLUME-декларации образа): явная
            // зачистка в teardown, ассерт чистоты.
            .WithVolumeMount($"{NetworkName}-{name}-data", "/etcd-data")
            .WithVolumeMount($"{NetworkName}-{name}-snapshots", "/snapshots");
        if (withClientHostPort)
            builder = builder.WithPortBinding(2379, assignRandomHostPort: true);
        return builder.Build();
    }

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await EnsureEtcdNodeImageAsync(ct);
        await _network.CreateAsync(ct);
        _node1 = BuildNode("node1", withClientHostPort: true);
        _node2 = BuildNode("node2", withClientHostPort: false);
        // Кворум 2: стартуем оба, готовность — по клиентской грани node1.
        await _node1.StartAsync(ct);
        await _node2.StartAsync(ct);
        Endpoint = $"http://localhost:{_node1.GetMappedPublicPort(2379)}";
        await WaitReadyAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        // Teardown при любом исходе: ключи → контейнеры → тома → сеть
        // (arch/testing/02-e2e-isolation.md; порядок — лучшее усилие, падение
        // dispose валит тест — это ассерт чистоты окружения).
        if (Endpoint.Length > 0)
        {
            // Домен агентов и посев цели — целиком нашего контура.
            await Gateway.DeleteAsync(Endpoint, "/etcdnode/", prefix: true, CancellationToken.None);
            await Gateway.DeleteAsync(Endpoint, "/s3/clusters/", prefix: true, CancellationToken.None);
        }

        _http.Dispose();
        await _node1.DisposeAsync();
        await _node2.DisposeAsync();
        if (_ownS3 is { } ownS3)
            await ownS3.DisposeAsync();
        foreach (var suffix in new[] { "node1-data", "node1-snapshots", "node2-data", "node2-snapshots" })
            await RemoveVolumeAsync($"{NetworkName}-{suffix}");
        await _network.DeleteAsync();
    }

    // Именованный том прогона — docker CLI (testcontainers-обёртки томов
    // не создавали: тома смонтированы builder'ом контейнера).
    private static async Task RemoveVolumeAsync(string name)
    {
        var psi = new System.Diagnostics.ProcessStartInfo("docker", $"volume rm {name}")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = System.Diagnostics.Process.Start(psi)!;
        await process.WaitForExitAsync(CancellationToken.None);
        // 0 — удалён; нетомный exit — упадёт ассертом ниже в CleanupAssert.
    }

    // Гейт статики: контур поднимается из образа агента — без него серия
    // падает пачкой непонятных ошибок; fail-fast с рецептом.
    private static async Task EnsureEtcdNodeImageAsync(CancellationToken ct)
    {
        var psi = new System.Diagnostics.ProcessStartInfo("docker",
            "image inspect pgworker/etcdnode:dev")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = System.Diagnostics.Process.Start(psi)!;
        await process.WaitForExitAsync(ct);
        if (process.ExitCode != 0)
            throw new ApplicationException(
                "pgworker/etcdnode:dev image is missing — static assets are not prepared. "
                + "Run: DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release");
    }

    // ownS3-нода (лениво): контейнер owns3:dev в сети контура, порт 9000 —
    // динамический host-порт; root-креды тестовые.
    public string OwnS3Endpoint { get; private set; } = "";

    public async Task StartOwnS3Async()
    {
        if (_ownS3 is not null)
            return;
        var ct = TestContext.Current.CancellationToken;
        _ownS3 = new ContainerBuilder("owns3:dev")
            .WithNetwork(_network)
            .WithNetworkAliases("owns3")
            .WithEnvironment("OWNS3_ROOT_USER", "it-root")
            .WithEnvironment("OWNS3_ROOT_PASSWORD", "it-root-password")
            // Ключ backups — тот же, что сидится в /s3/clusters/eb/secrets
            // (формат env ноды arch/s3-clusters/06-secrets.md): экспортёр ходит этим ключом.
            .WithEnvironment("OWNS3_ACCESS_KEYS__0__ACCESSKEY", "eb-key")
            .WithEnvironment("OWNS3_ACCESS_KEYS__0__SECRETKEY", "eb-secret")
            .WithEnvironment("OWNS3_ACCESS_KEYS__0__POLICY", "read-write")
            .WithPortBinding(9000, assignRandomHostPort: true)
            .Build();
        await _ownS3.StartAsync(ct);
        OwnS3Endpoint = $"http://localhost:{_ownS3.GetMappedPublicPort(9000)}";
        await WaitOwnS3ReadyAsync(ct);
    }

    // Сид etcd-backup контура в etcd окружения (arch/s3-clusters/05-platform.md §6):
    // config с признаком etcd_backup, endpoints, secrets с ключом backups;
    // фиксированный слепковый bucket создаётся root-кредами. Endpoints ДВА:
    // сетевой alias owns3:9000 первым — S3-клиент агента (в контейнере узла;
    // localhost агенту не резолвит ownS3), host-порт вторым — хостовые
    // экспортёр-кейсы выбирают его из списка.
    public async Task SeedEtcdBackupClusterAsync()
    {
        await StartOwnS3Async();
        var ct = TestContext.Current.CancellationToken;
        await EnsureSnapshotBucketAsync();
        var port = _ownS3!.GetMappedPublicPort(9000);
        await Gateway.PutAsync(Endpoint, "/s3/clusters/eb/config",
            $$"""{"platform":"owns3","nodes":1,"created_unix":1756500000,"etcd_backup":true}""",
            lease: null, ct);
        await Gateway.PutAsync(Endpoint, "/s3/clusters/eb/endpoints",
            $"owns3:9000,localhost:{port}", lease: null, ct);
        await Gateway.PutAsync(Endpoint, "/s3/clusters/eb/secrets",
            """{"root_user":"it-root","root_password":"it-root-password","access_keys":[{"name":"backups","access_key":"eb-key","secret_key":"eb-secret","policy":"read-write"}]}""",
            lease: null, ct);
    }

    // Сид с битыми endpoints (кейс транзиента цели): цель резолвится, но
    // недостижима И агенту (localhost:1 его контейнера), И хосту — FAILED.
    public async Task SeedEtcdBackupClusterBrokenEndpointsAsync()
    {
        await StartOwnS3Async();
        var ct = TestContext.Current.CancellationToken;
        await Gateway.PutAsync(Endpoint, "/s3/clusters/eb/endpoints",
            "localhost:1", lease: null, ct);
    }

    // S3-endpoint хостовых кейсов: из списка цели берём host-порт (localhost);
    // первый элемент списка — сетевой alias owns3:9000 для АГЕНТА в контейнере
    // (хостом не резолвится).
    public static string HostS3Endpoint(EtcdBackupTarget target)
        => target.Endpoints.FirstOrDefault(
               e => e.Contains("localhost", StringComparison.Ordinal))
           ?? target.Endpoints[0];

    // Фиксированный слепковый bucket на ноде (root-креды, идемпотентно).
    private async Task EnsureSnapshotBucketAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        var config = new AmazonS3Config
        {
            ServiceURL = OwnS3Endpoint,
            ForcePathStyle = true,
            Timeout = TimeSpan.FromSeconds(7.5),
            MaxErrorRetry = 0,
        };
        using var s3 = new AmazonS3Client(
            new Amazon.Runtime.BasicAWSCredentials("it-root", "it-root-password"), config);
        try
        {
            await s3.PutBucketAsync(new Amazon.S3.Model.PutBucketRequest
            {
                BucketName = Shared.Core.EtcdSnapshotBucket.Name,
                UseClientRegion = false,
            }, ct);
        }
        catch (AmazonS3Exception ex) when (ex.Message.Contains("already", StringComparison.OrdinalIgnoreCase))
        {
            // bucket уже существует — идемпотентно
        }
    }

    private async Task WaitOwnS3ReadyAsync(CancellationToken ct)
    {
        using var probeClient = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        for (var i = 0; i < 30; i++)
        {
            try
            {
                using var probe = await probeClient.GetAsync(OwnS3Endpoint + "/healthz", ct);
                if (probe.IsSuccessStatusCode)
                    return;
            }
            catch (HttpRequestException)
            {
                // нода ещё поднимается — ждём следующей попытки
            }

            await Task.Delay(1000, ct);
        }

        throw new InvalidOperationException($"ownS3 at {OwnS3Endpoint} did not come up within 30 s");
    }

    // Put-хелпер интеграций: абсолютный ключ контура → значение.
    public Task PutAsync(string key, string value)
        => Gateway.PutAsync(Endpoint, key, value, null, TestContext.Current.CancellationToken);

    // Get-хелпер интеграций: абсолютный ключ → Kv?.
    public Task<Result<Kv?>> GetAsync(string key)
        => Gateway.GetAsync(Endpoint, key, TestContext.Current.CancellationToken);

    // Готовность контура: статус client-грани node1 (кворум обоих узлов).
    private async Task WaitReadyAsync(CancellationToken ct)
    {
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
                    return;
            }
            catch (HttpRequestException)
            {
                // контур ещё поднимается — ждём следующей попытки
            }

            await Task.Delay(1000, ct);
        }

        throw new InvalidOperationException(
            $"etcd at {Endpoint} did not come up within 30 probes");
    }
}

// Одно окружение на EtcdNode-интеграции сборки: контур (2 узла с живыми
// агентами) поднимается один, dispose фикстуры — полная зачистка серии.
// Parallelization ВЫКЛЮЧЕНА: статус-ключ и том лидера — общие для кейсов.
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class EtcdNodeCollection : ICollectionFixture<EtcdNodeEnvironment>
{
    public const string Name = "etcdnode-env";
}
