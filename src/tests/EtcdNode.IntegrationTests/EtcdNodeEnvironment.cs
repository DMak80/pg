using System.Text;
using Amazon.S3;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using EtcdNode.Core;
using Xunit;

namespace EtcdNode.IntegrationTests;

// Окружение EtcdNode-интеграций (arch/etcdnode/01-agent.md): контур ИЗ ОБРАЗА
// pgworker/etcdnode:dev — ТРИ узла (etcd + supervisord + живой агент) в
// testcontainers-сети etcdn-<guid>-net, общий initial-cluster, host-порт
// клиентской грани — только node1, динамический (AGENTS.md). Три узла —
// канонический managed-контур: кворум 2 переживает остановку одного члена
// (кейс ветки F: смерть лидера не рушит запись). Агентам — БЫСТРЫЕ
// интервалы (MIN/RETRY 1 с) для поведенческих кейсов фильтра. ownS3-нода
// цели — лениво (образ owns3:dev внешний, порт 9000 динамический).
// Полный teardown при любом исходе: ключи домена/посева → контейнеры →
// именованные тома → сеть; завершается АССЕРТОМ чистоты прогона
// (канон arch/testing/02-e2e-isolation.md).
public sealed class EtcdNodeEnvironment : IAsyncLifetime
{
    // Уникальный тег прогона: имена сети/томов уникальны на каждый запуск.
    public string RunTag { get; } = Guid.NewGuid().ToString("N")[..8];

    public string NetworkName => $"etcdn-{RunTag}-net";

    private readonly INetwork _network;
    private IContainer? _node1;
    private IContainer? _node2;
    private IContainer? _node3;

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

    private const string InitialCluster =
        "node1=http://node1:2380,node2=http://node2:2380,node3=http://node3:2380";

    // Узел контура: образ агента, ETCD_ARGS общим составом, агент —
    // ускоренные интервалы (значения остальных — дефолты env узла,
    // arch/etcdnode/01-agent.md §3). Client-грань КАЖДОГО узла получает
    // свой динамический host-порт (AGENTS.md: без литералов): Endpoint —
    // node1; EndpointOf — любой живой узел (кейс ветки F читает контур,
    // когда лидер node1 остановлен).
    private IContainer BuildNode(string name)
    {
        return new ContainerBuilder("pgworker/etcdnode:dev")
            .WithNetwork(_network)
            .WithNetworkAliases(name)
            .WithEnvironment("ETCD_ARGS",
                $"--name={name} --data-dir=/etcd-data"
                + $" --listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://{name}:2379"
                + $" --listen-peer-urls=http://0.0.0.0:2380 --initial-advertise-peer-urls=http://{name}:2380"
                + $" --initial-cluster={InitialCluster} --initial-cluster-state=new")
            .WithEnvironment("ETCDNODE_NODE_NAME", name)
            // MinIntervalSec: канон-дефолт 5; поведенческим кейсам нужно ускорение 1
            .WithEnvironment("ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC", "1")
            // Retry 5 c (не 1): при битой/отсутствующей цели агент-лидер
            // перезапускает слепок каждый RetryIntervalSec — секудная молотьба
            // перегружала серию; 5 с достаточно для бюджетов кейсов.
            .WithEnvironment("ETCDNODE_SNAPSHOTS__RETRY_INTERVAL_SEC", "5")
            .WithEnvironment("ETCDNODE_SNAPSHOTS__TIMEOUT_SEC", "10")
            .WithEnvironment("ETCDNODE_SNAPSHOTS__INTERVAL_MIN", "360")
            .WithEnvironment("ETCDNODE_SNAPSHOTS__RETENTION_FILES", "10")
            .WithEnvironment("ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS", "3")
            .WithEnvironment("ETCDNODE_SNAPSHOTS__MAINTENANCE_INTERVAL_MIN", "60")
            // Именованные тома (не анонимные VOLUME-декларации образа): явная
            // зачистка в teardown, ассерт чистоты.
            .WithVolumeMount($"{NetworkName}-{name}-data", "/etcd-data")
            .WithVolumeMount($"{NetworkName}-{name}-snapshots", "/snapshots")
            .WithPortBinding(2379, assignRandomHostPort: true)
            .Build();
    }

    // Client-грань узла по имени (динамический host-порт).
    public string EndpointOf(string name)
        => $"http://localhost:{Node(name).GetMappedPublicPort(2379)}";

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await EnsureEtcdNodeImageAsync(ct);
        await _network.CreateAsync(ct);
        _node1 = BuildNode("node1");
        _node2 = BuildNode("node2");
        _node3 = BuildNode("node3");
        // Кворум 2 из 3: стартуем все, готовность — по клиентской грани node1.
        await _node1.StartAsync(ct);
        await _node2.StartAsync(ct);
        await _node3.StartAsync(ct);
        Endpoint = EndpointOf("node1");
        await WaitReadyAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeCoreAsync();
    }

    private async ValueTask DisposeCoreAsync()
    {
        // Teardown при любом исходе: ключи → контейнеры → тома → сеть
        // (arch/testing/02-e2e-isolation.md). Null-гварды полей: fail-fast
        // гейта образа (до присвоений) не должен маскироваться NRE —
        // исходное ApplicationException с рецептом остаётся единственной
        // ошибкой dispose.
        if (Endpoint.Length > 0)
        {
            // Домен агентов и посев цели — целиком нашего контура.
            await Gateway.DeleteAsync(Endpoint, "/etcdnode/", prefix: true, CancellationToken.None);
            await Gateway.DeleteAsync(Endpoint, "/s3/clusters/", prefix: true, CancellationToken.None);
        }

        // Телеметрия (канон arch/testing/01-e2e-launch.md): docker-логи всех
        // контейнеров прогона снимаются ДО удаления — разбор падений без
        // перезапуска тестов.
        var telemetryDir = $"/tmp/pgw-etcdn-it-{RunTag}";
        foreach (var name in new[] { "node1", "node2", "node3" })
        {
            try
            {
                var containerName = Node(name).Name.TrimStart('/');
                var logs = await DockerAsync($"logs --tail 20000 {containerName}", okWhenMissing: true);
                if (logs.Length > 0)
                {
                    Directory.CreateDirectory(telemetryDir);
                    await File.WriteAllTextAsync($"{telemetryDir}/{name}.log", logs);
                }
            }
            catch (Exception)
            {
                // телеметрия — лучшее усилие, teardown продолжается
            }
        }

        _http.Dispose();
        if (_node1 is { } node1)
            await node1.DisposeAsync();
        if (_node2 is { } node2)
            await node2.DisposeAsync();
        if (_node3 is { } node3)
            await node3.DisposeAsync();
        if (_ownS3 is { } ownS3)
            await ownS3.DisposeAsync();
        foreach (var suffix in new[]
                 {
                     "node1-data", "node1-snapshots", "node2-data", "node2-snapshots",
                     "node3-data", "node3-snapshots",
                 })
            await RemoveVolumeAsync($"{NetworkName}-{suffix}");
        await _network.DeleteAsync();

        // АССЕРТ ЧИСТОТЫ (arch/testing/02-e2e-isolation.md): в сети нет
        // контейнеров, тома/сеть прогона удалены — иначе dispose падает.
        await AssertCleanAsync();
    }

    // Ассерт чистоты прогона: (1) в сети нет подключённых контейнеров
    // (отсутствие сети — тоже чисто: testcontainers мог удалить её раньше);
    // (2) сеть и все тома тега не существуют.
    private async Task AssertCleanAsync()
    {
        var attached = await DockerAsync(
            $"network inspect {NetworkName} --format \"{{{{len .Containers}}}}\"", okWhenMissing: true);
        var attachedCount = attached.Trim();
        attachedCount.Should().BeOneOf("", "0",
            $"containers of {NetworkName} must be disposed before network teardown");

        var networkGone = await DockerAsync($"network inspect {NetworkName}", okWhenMissing: true);
        networkGone.Should().BeEmpty($"network {NetworkName} must be deleted by teardown");
        foreach (var name in new[]
                 {
                     $"{NetworkName}-node1-data", $"{NetworkName}-node1-snapshots",
                     $"{NetworkName}-node2-data", $"{NetworkName}-node2-snapshots",
                     $"{NetworkName}-node3-data", $"{NetworkName}-node3-snapshots",
                 })
        {
            var volumeGone = await DockerAsync($"volume inspect {name}", okWhenMissing: true);
            volumeGone.Should().BeEmpty($"volume {name} must be deleted by teardown");
        }
    }

    // docker CLI: вывод stdout; okWhenMissing — ненулевой exit даёт пустую
    // строку (объект не существует; docker CLI кодит «not found» как 1),
    // прочие ошибки — исключение. Оба перенаправленных потока читаются
    // ПАРАЛЛЕЛЬНО ожиданию выхода: иначе пайп (64 КБ) заполняется выводом
    // (docker logs контейнера с сотнями МБ логов) и процесс блокируется
    // на записи навсегда — WaitForExitAsync не завершается.
    private static async Task<string> DockerAsync(string args, bool okWhenMissing)
    {
        var psi = new System.Diagnostics.ProcessStartInfo("docker", args)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = System.Diagnostics.Process.Start(psi)!;
        var stdoutTask = process.StandardOutput.ReadToEndAsync(CancellationToken.None);
        var stderrTask = process.StandardError.ReadToEndAsync(CancellationToken.None);
        await process.WaitForExitAsync(CancellationToken.None);
        if (process.ExitCode == 0)
            return await stdoutTask;
        if (okWhenMissing)
            return "";
        throw new ApplicationException($"docker {args} exited {process.ExitCode}: {await stderrTask}");
    }

    // Именованный том прогона — docker CLI с проверкой exit (testcontainers-
    // обёртки томов не создавали: тома смонтированы builder'ом контейнера).
    private static async Task RemoveVolumeAsync(string name)
        => await DockerAsync($"volume rm {name}", okWhenMissing: false);

    // Гейт статики: контур поднимается из образа агента — без него серия
    // падает пачкой непонятных ошибок; fail-fast с рецептом. Копия гейта
    // EtcdWorkerEnvironment (прецедент планов интеграции; вынос в общий
    // хелпер — отдельная задача, не рефакторим).
    private static async Task EnsureEtcdNodeImageAsync(CancellationToken ct)
    {
        try
        {
            await DockerAsync("image inspect pgworker/etcdnode:dev", okWhenMissing: false);
        }
        catch (ApplicationException)
        {
            throw new ApplicationException(
                "pgworker/etcdnode:dev image is missing — static assets are not prepared. "
                + "Run: DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release");
        }
    }

    // Смерть узла контура (кейс ветки F): docker kill — SIGKILL, БЕЗ
    // graceful-стопа: хост агента не успевает отозвать lease лидерства —
    // ключ гаснет только по TTL (штатная остановка ревокает мгновенно и
    // это ДРУГОЙ сценарий — takeover сразу).
    public async Task KillNodeAsync(string name)
    {
        var containerName = Node(name).Name.TrimStart('/');
        await DockerAsync($"kill {containerName}", okWhenMissing: false);
    }

    // Возврат убитого узла: docker start (CLI) — testcontainers StartAsync
    // на exited-контейнере пересоздаёт контейнер и host-порт: хостовый
    // Endpoint окружения умирает навсегда (docker start маппинг сохраняет).
    public async Task StartNodeAsync(string name)
        => await DockerAsync($"start {Node(name).Name.TrimStart('/')}", okWhenMissing: false);

    // Список файлов в томе /snapshots узла (docker exec ls; пустой том —
    // пустая строка): ассерт «слепки снимает только лидер».
    public async Task<string> SnapshotFilesOfAsync(string name)
        => await DockerAsync(
            $"exec {Node(name).Name.TrimStart('/')} ls /snapshots", okWhenMissing: false);

    private IContainer Node(string name) => name switch
    {
        "node1" => _node1 ?? throw new InvalidOperationException("node1 is not built"),
        "node2" => _node2 ?? throw new InvalidOperationException("node2 is not built"),
        "node3" => _node3 ?? throw new InvalidOperationException("node3 is not built"),
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, null),
    };

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

    // Put-хелпер интеграций: абсолютный ключ контура → значение (результат
    // ассертится кейсами — молчаливая потеря put'а недопустима).
    public Task<Result> PutAsync(string key, string value)
        => Gateway.PutAsync(Endpoint, key, value, null, TestContext.Current.CancellationToken);

    // Get-хелпер интеграций: абсолютный ключ → Kv?.
    public Task<Result<Kv?>> GetAsync(string key)
        => Gateway.GetAsync(Endpoint, key, TestContext.Current.CancellationToken);

    // Готовность контура: статус client-грани node1 (кворум всех узлов).
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

// Одно окружение на EtcdNode-интеграции сборки: контур (3 узла с живыми
// агентами) поднимается один, dispose фикстуры — полная зачистка серии.
// Parallelization ВЫКЛЮЧЕНА: статус-ключ и том лидера — общие для кейсов.
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class EtcdNodeCollection : ICollectionFixture<EtcdNodeEnvironment>
{
    public const string Name = "etcdnode-env";
}
