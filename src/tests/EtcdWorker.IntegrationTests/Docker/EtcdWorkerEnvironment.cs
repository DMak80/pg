using System.Text;
using Amazon.S3;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Xunit;

namespace EtcdWorker.IntegrationTests.Docker;

// Окружение EtcdWorker-интеграций (порт S3EtcdFixture/S3ClusterFixture):
// testcontainers-сеть etcdw-<guid>-net + единственный seed-узел etcd 3.5
// (initial-cluster-state=new). Клиентская грань — динамический host-порт
// (AGENTS.md); peer-грань — имена в сети контура, host-публикаций нет
// (arch/etcdworker/01-placement.md). Готовность — POST-ретрай (встроенные HTTP-wait шлют GET,
// /v3/* требует POST). Полный teardown при любом исходе: свои ключи →
// контейнер → сеть; успех DisposeAsync сети/контейнера — ассерт чистоты
// (удаление несуществующих объектов testcontainers падает).
public sealed class EtcdWorkerEnvironment : IAsyncLifetime
{
    // Уникальный тег прогона: имя сети уникально на каждый запуск.
    public string RunTag { get; } = Guid.NewGuid().ToString("N")[..8];

    public string NetworkName => $"etcdw-{RunTag}-net";

    private readonly INetwork _network;

    private IContainer _etcd;

    // ownS3-нода цели слепков (шаг 9): поднимается ЛЕНИВО (только тестам
    // слепков нужна; member-API-тесты образ не грузят) — образ owns3:dev
    // внешний (репозиторий ../OwnS3), порт 9000 динамический, та же сеть.
    private IContainer? _ownS3;

    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };

    // Уникальный префикс сценария в etcd (изоляция ключей серий).
    public string TestPrefix { get; } = "/etcdworker-it-" + Guid.NewGuid().ToString("N")[..8];

    public string Endpoint { get; private set; } = "";

    public EtcdGateway Gateway { get; }

    public EtcdWorkerEnvironment()
    {
        _network = new NetworkBuilder()
            .WithName(NetworkName)
            .Build();
        _etcd = new ContainerBuilder("quay.io/coreos/etcd:v3.5.21")
            .WithNetwork(_network)
            .WithNetworkAliases("seed")
            .WithCommand(
                "etcd",
                "--name=seed",
                "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://localhost:2379",
                // Peer — по имени сети (alias seed): managed-узлы (шаг 13)
                // резолвят seed контура внутри etcdw-<guid>-net.
                "--listen-peer-urls=http://0.0.0.0:2380",
                "--initial-advertise-peer-urls=http://seed:2380",
                "--initial-cluster=seed=http://seed:2380",
                "--initial-cluster-state=new")
            .WithPortBinding(2379, assignRandomHostPort: true)
            .Build();
        Gateway = new EtcdGateway(_http);
    }

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await EnsureEtcdNodeImageAsync(ct);
        await _network.CreateAsync(ct);
        await _etcd.StartAsync(ct);
        Endpoint = $"http://localhost:{_etcd.GetMappedPublicPort(2379)}";
        await WaitReadyAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        // Teardown при любом исходе: свои ключи → контейнеры → сеть
        // (arch/testing/02-e2e-isolation.md; порядок — лучшее усилие, падение dispose
        // сети/контейнера валит тест — это ассерт чистоты окружения).
        if (Endpoint.Length > 0)
            await Gateway.DeleteAsync(Endpoint, TestPrefix, prefix: true, CancellationToken.None);
        _http.Dispose();
        await _etcd.DisposeAsync();
        if (_ownS3 is { } ownS3)
            await ownS3.DisposeAsync();
        await _network.DeleteAsync();
    }

    // Гейт статики (arch/etcdnode/01-agent.md): membership-тесты создают
    // managed-узлы из образа агента — без него серия падает пачкой
    // непонятных ошибок; fail-fast с рецептом.
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

    // Пересоздание seed-контура (анти-отравление серии): свежий etcd без
    // managed-членов и ключей — cleanup кейса зовёт при упорном (raft-
    // транзиент) отказе вывода членов; следующий кейс серии строит rig по
    // свежему env.Endpoint и получает чистый raft-состав. Host-порт меняется
    // на новый динамический (Endpoint обновляется).
    public async Task ResetSeedAsync(CancellationToken ct)
    {
        await _etcd.DisposeAsync();
        _etcd = new ContainerBuilder("quay.io/coreos/etcd:v3.5.21")
            .WithNetwork(_network)
            .WithNetworkAliases("seed")
            .WithCommand(
                "etcd",
                "--name=seed",
                "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://localhost:2379",
                "--listen-peer-urls=http://0.0.0.0:2380",
                "--initial-advertise-peer-urls=http://seed:2380",
                "--initial-cluster=seed=http://seed:2380",
                "--initial-cluster-state=new")
            .WithPortBinding(2379, assignRandomHostPort: true)
            .Build();
        await _etcd.StartAsync(ct);
        Endpoint = $"http://localhost:{_etcd.GetMappedPublicPort(2379)}";
        await WaitReadyAsync(ct);
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

    // Сид etcd-backup контура в etcd окружения (arch/s3-clusters/05-platform.md/§6): config с
    // признаком etcd_backup, endpoints хост:порт, secrets с ключом backups;
    // фиксированный слепковый bucket создаётся root-кредами (в проде его
    // создаёт S3Worker P3 — здесь нода «сырая»).
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
            $"localhost:{port}", lease: null, ct);
        await Gateway.PutAsync(Endpoint, "/s3/clusters/eb/secrets",
            """{"root_user":"it-root","root_password":"it-root-password","access_keys":[{"name":"backups","access_key":"eb-key","secret_key":"eb-secret","policy":"read-write"}]}""",
            lease: null, ct);
    }

    // Сид с битыми endpoints (кейс транзиента цели): цель резолвится, но
    // недостижима — FAILED-статус с маркером цели.
    public async Task SeedEtcdBackupClusterBrokenEndpointsAsync()
    {
        await StartOwnS3Async();
        var ct = TestContext.Current.CancellationToken;
        await Gateway.PutAsync(Endpoint, "/s3/clusters/eb/endpoints",
            "localhost:1", lease: null, ct);
    }

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
        catch (Amazon.S3.AmazonS3Exception ex) when (ex.Message.Contains("already", StringComparison.OrdinalIgnoreCase))
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
                // нода ещё поднимается — ждём следующую попытку
            }

            await Task.Delay(1000, ct);
        }

        throw new InvalidOperationException($"ownS3 at {OwnS3Endpoint} did not come up within 30 s");
    }

    // Put-хелпер интеграций: ключ → значение (свой префикс сценария).
    public Task PutAsync(string key, string value)
        => Gateway.PutAsync(Endpoint, TestPrefix + key, value, null, TestContext.Current.CancellationToken);

    // Get-хелпер интеграций: ключ префикса сценария → Kv?.
    public Task<Result<Kv?>> GetAsync(string key)
        => Gateway.GetAsync(Endpoint, TestPrefix + key, TestContext.Current.CancellationToken);

    // Готовность seed-etcd: 30 попыток × (проба с 3-с таймаутом + пауза 1 с),
    // бюджет ≈ 120 с в худшем случае (отказы соединения до поднятия ноды).
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
                // etcd ещё поднимается — ждём следующую попытку
            }

            await Task.Delay(1000, ct);
        }

        throw new InvalidOperationException(
            $"etcd at {Endpoint} did not come up within 30 probes (≈120 s worst case)");
    }
}

// Одно окружение на EtcdWorker-интеграции сборки: сеть+seed поднимаются один,
// dispose фикстуры — полная зачистка серии. Parallelization ВЫКЛЮЧЕНА: состав
// кластера — глобальное состояние, etcd 3.5 допускает ровно ОДИН unstarted
// learner (повторный add того же peer-URL → "too many learner members").
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class EtcdWorkerCollection : ICollectionFixture<EtcdWorkerEnvironment>
{
    public const string Name = "etcdworker-env";
}
