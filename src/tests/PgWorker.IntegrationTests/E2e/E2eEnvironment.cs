using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using FluentAssertions;
using PgWorker.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>
/// Изолированное e2e-окружение (docker даёт полностью независимые окружения —
/// используем): собственная docker-сеть, свой etcd-контейнер, опционально свой
/// MinIO — на КАЖДЫЙ сценарий (Fact). PgWorker-инстансы (хост-процессы) стартует
/// сам сценарий через StartHostAsync; они смотрят ТОЛЬКО в etcd этого окружения
/// и обслуживают только его Active-кластеры — джобы/операции одного сценария
/// физически не могут попасть в ресурсы другого (инцидент Release: E2eBackup
/// падали от чужих джобов в общем etcd). Смерть etcd-контейнера снимает и
/// проблему чистки ключей: контейнер удалён — ключи удалены.
/// DisposeAsync при ЛЮБОМ исходе (await using в теле Fact): kill воркеров →
/// stop/rm контейнеров окружения → rm томов → rm сети → ассерт чистоты (ни
/// одного pgw-* артефакта окружения не осталось).
/// Статические ассеты (PKI-пакет, образы pgworker-node/pgworker-backup,
/// Release-бинарь PgWorker.App) собираются один раз на процесс — это не
/// мутабельный рантайм-стейт; всё, что сценарий меняет в рантайме, живёт и
/// умирает внутри своего окружения.
/// </summary>
public sealed class E2eEnvironment : IAsyncDisposable
{
    private const string EtcdImage = "quay.io/coreos/etcd:v3.5.21";

    // mc запинен существующим docker-тегом (ближайший к MC_VERSION образа
    // pgworker-backup, arch/19 §10; у docker-тегов и архивных тегов dl.min.io
    // метки времени расходятся).
    private const string MinioImage = "minio/minio:RELEASE.2025-09-07T16-13-09Z";

    // mc запинен существующим docker-тегом (ближайший к MC_VERSION образа
    // pgworker-backup, arch/19 §10; у docker-тегов и архивных тегов dl.min.io
    // метки времени расходятся).
    internal const string McImage = "minio/mc:RELEASE.2025-08-13T08-35-41Z";

    // Образ узла (задача 25): без DOORMAN_URL — узел без пулера, PgWorker
    // запускается с EnableDoorman=false (Д4, R1).
    public const string NodeImage = "pgworker-node:e2e";

    public const string JobImage = "pgworker-backup:e2e";

    private const string MinioUser = "minioadmin";
    private const string MinioPassword = "minioadmin";
    private const string BucketName = "pgworker-backups";

    private readonly List<HostInstance> _hosts = [];
    private readonly HttpClient _gatewayHttp = new();
    private readonly IContainer _etcd;
    private readonly INetwork _net;
    private readonly IContainer? _minio;

    private E2eEnvironment(
        string slug,
        string netName,
        string etcdEndpoint,
        IContainer etcd,
        INetwork net,
        IContainer? minio)
    {
        Slug = slug;
        NetName = netName;
        EtcdEndpoint = etcdEndpoint;
        _etcd = etcd;
        _net = net;
        _minio = minio;
        Gateway = new EtcdGateway(_gatewayHttp);
        S3Endpoint = minio is null
            ? ""
            : $"http://host.docker.internal:{minio.GetMappedPublicPort(9000)}";
    }

    public string Slug { get; }

    /// <summary>etcd окружения: published порт на хосте (зонд свободного порта),
    /// advertise для контейнеров — host.docker.internal на тот же порт.</summary>
    public string EtcdEndpoint { get; }

    /// <summary>Имя docker-сети окружения (уникально на прогон) — удаляется в
    /// teardown'е самими (ryuk не гарант), отсутствие проверяется ассертом.</summary>
    public string NetName { get; }

    public EtcdGateway Gateway { get; }

    /// <summary>S3-endpoint для джобов/воркера: published порт MinIO на хосте,
    /// из docker-контейнеров — host.docker.internal (Docker Desktop/host-gateway).
    /// Пустая строка, если окружение поднято без MinIO.</summary>
    public string S3Endpoint { get; }

    // ===== Статические ассеты процесса (не мутабельный рантайм-стейт) =====

    private static readonly SemaphoreSlim StaticGate = new(1, 1);

    private static bool _staticReady;
    private static string _root = "";
    private static string _appDll = "";
    private static (string CaPem, string CaKeyPem) _installCa;
    private static string _serverCertPem = "";
    private static string _serverKeyPem = "";
    private static HttpClient _healthHttp = null!;
    private static bool _jobImageReady;

    /// <summary>Подъём окружения: сеть → etcd (wait-стратегия /health) →
    /// опционально MinIO + bucket. Хост-порты — зонд свободного порта; advertise
    /// строится от фактического published порта. Гейт PGW_TEST_DOCKER и скип —
    /// ответственность сценария (DockerTrait.SkipIfUnavailable до StartAsync).</summary>
    public static async Task<E2eEnvironment> StartAsync(
        string slug, bool withMinio = false, CancellationToken ct = default)
    {
        await EnsureStaticAsync(ct);

        // Сеть окружения: уникальное имя на прогон (случайный суффикс) —
        // создайте/удалите сами, ryuk не гарант; отсутствие после teardown — ассерт.
        var netName = $"pgw-e2e-net-{Slugify(slug)}-{Guid.NewGuid():N}"[..20];
        var net = new NetworkBuilder().WithName(netName).Build();
        await net.CreateAsync(ct);

        // etcd (внешний слой стенда). Хост-порт — зонд свободного порта (правило
        // динамических портов); advertise host.docker.internal:<порт>: Patroni-ноды
        // узнают адреса членов из advertise-client-urls — обязаны быть достижимы
        // ИЗ контейнеров. Готовность — wait-стратегия etcd /health (блокирующий
        // StartAsync, без sleep-поллинга), бюджет ≤ 100 с.
        var etcdPort = E2eFixture.FreePort();
        var etcd = new ContainerBuilder(EtcdImage)
            .WithName($"pgw-e2e-etcd-{Slugify(slug)}-{Guid.NewGuid():N}"[..30])
            .WithCommand(
                "etcd", "--name=e2e", "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                $"--advertise-client-urls=http://host.docker.internal:{etcdPort}")
            .WithPortBinding(etcdPort, 2379) // (hostPort, containerPort)
            .WithNetwork(net)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(
                    request => request.ForPort(2379).ForPath("/health"),
                    wait => wait.WithTimeout(TimeSpan.FromSeconds(100))))
            .Build();
        await etcd.StartAsync(ct);
        var etcdEndpoint = $"http://localhost:{etcd.GetMappedPublicPort(2379)}";

        IContainer? minio = null;
        if (withMinio)
        {
            // MinIO в сети окружения (mc ходит по алиасу; джобы — через published
            // порт, сеть им не нужна). Готовность — wait-стратегия health/live.
            minio = new ContainerBuilder(MinioImage)
                .WithName($"pgw-e2e-minio-{Slugify(slug)}-{Guid.NewGuid():N}"[..30])
                .WithCommand("server", "/data")
                .WithEnvironment("MINIO_ROOT_USER", MinioUser)
                .WithEnvironment("MINIO_ROOT_PASSWORD", MinioPassword)
                .WithNetwork(net)
                .WithNetworkAliases("e2e-minio")
                .WithPortBinding(9000, assignRandomHostPort: true) // AGENTS.md: порты динамические
                .WithWaitStrategy(Wait.ForUnixContainer()
                    .UntilHttpRequestIsSucceeded(
                        request => request.ForPort(9000).ForPath("/minio/health/live"),
                        wait => wait.WithTimeout(TimeSpan.FromSeconds(100))))
                .Build();
            await minio.StartAsync(ct);

            // Bucket pgworker-backups (mc из той же сети — по алиасу): блокирующий
            // docker run, без поллинга.
            await E2eFixture.RunProcessAsync("docker",
            [
                "run", "--rm", "--network", netName, "--entrypoint", "/bin/sh", McImage,
                "-c", $"mc alias set t http://e2e-minio:9000 {MinioUser} {MinioPassword} >/dev/null"
                      + $" && mc mb --ignore-existing t/{BucketName}",
            ], ct);

            // Образ джоба: сборка из корня репо (контекст — корень: COPY docker/backup/…),
            // один раз на процесс (статический ассет, docker-cache инкрементален).
            await EnsureJobImageAsync(ct);
        }

        return new E2eEnvironment(slug, netName, etcdEndpoint, etcd, net, minio);
    }

    /// <summary>Запуск инстанса PgWorker.App с e2e-конфигурацией (быстрые тики),
    /// смотрящего ТОЛЬКО в etcd этого окружения. extraEnv — дополнительные
    /// env-пары поверх базовых (t02: Backups-комплект).</summary>
    public async Task<HostInstance> StartHostAsync(
        string name, int snapshotIntervalMin = 360,
        IReadOnlyDictionary<string, string>? extraEnv = null, CancellationToken ct = default)
    {
        // Ретрай выбора порта: зонд FreePort закрывает листенер до старта процесса —
        // в окне эфемерный порт может занять исходящее соединение (AddressInUse,
        // инцидент гейта t03-merge). Повторяем зонд, максимум 3 попытки.
        for (var attempt = 1; ; attempt++)
            try
            {
                return await StartHostOnPortAsync(name, snapshotIntervalMin, extraEnv, ct);
            }
            catch (ApplicationException e) when (attempt < 3
                && e.Message.Contains("address already in use", StringComparison.OrdinalIgnoreCase))
            {
                // следующий заход возьмёт новый зонд
            }
    }

    private async Task<HostInstance> StartHostOnPortAsync(
        string name, int snapshotIntervalMin,
        IReadOnlyDictionary<string, string>? extraEnv, CancellationToken ct)
    {
        var port = E2eFixture.FreePort();
        var snapshotsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-{name}-{port}");
        Directory.CreateDirectory(snapshotsDir);

        var env = new Dictionary<string, string>
        {
            // Секреты установки (Д7).
            ["PGW_PG_SUPERUSER_PASSWORD"] = E2eFixture.SuPassword,
            ["PGW_PG_STANDBY_PASSWORD"] = E2eFixture.StandbyPassword,
            ["PGW_BUCKET_ADMIN_PASSWORD"] = E2eFixture.BucketAdminPassword,
            ["PGW_BUCKET_MOVER_PASSWORD"] = E2eFixture.MoverPassword,

            // Конфигурация (env-оверрайды appsettings): один docker-хост plain.
            // AdvertisedEndpoints: контейнеры нод ходят в etcd этого окружения
            // через host.docker.internal, сам PgWorker — по localhost.
            ["PgWorker__Etcd__Endpoints__0"] = EtcdEndpoint,
            ["PgWorker__Etcd__AdvertisedEndpoints__0"] = EtcdEndpoint.Replace(
                "localhost:", "host.docker.internal:", StringComparison.Ordinal),
            ["PgWorker__Docker__Mode"] = "Plain",
            // Имя docker-хоста = advertised-имя для КОНТЕЙНЕРОВ (portalloc host,
            // DSN бэкап-джобов arch/19 §2/§6): джобы ходят к нодам через
            // host.docker.internal:<published pg-порт> (extra_hosts host-gateway),
            // как контейнеры нод — к etcd (AdvertisedEndpoints ниже).
            ["PgWorker__Docker__Hosts__0__Name"] = "host.docker.internal",
            ["PgWorker__Docker__Hosts__0__Endpoint"] = "unix:///var/run/docker.sock",
            ["PgWorker__Docker__PortRange__From"] = "15100",
            ["PgWorker__Docker__PortRange__To"] = "15200",
            ["PgWorker__Docker__Images__Node"] = NodeImage,
            ["PgWorker__Docker__EnableDoorman"] = "false",

            // Ускоренные циклы/пороги для e2e (критерии ждут секунды, не минуты).
            ["PgWorker__Loops__ScanIntervalSec"] = "1",
            ["PgWorker__Loops__KeepaliveSec"] = "1",
            ["PgWorker__Loops__ErrorDelayMs"] = "500",
            ["PgWorker__Loops__SnapshotIntervalMin"] = snapshotIntervalMin.ToString(),
            ["PgWorker__Thresholds__NodeDeadSec"] = "6",
            ["PgWorker__Thresholds__ShardDeadSec"] = "5",
            ["PgWorker__Thresholds__PatroniBootSec"] = "600",

            // Переезды (t01): spilo-18 → FailoverSlots=true (штатный путь PG17+,
            // R1/Д11); короткие паузы заморозки/поллинга — окно FROZEN в e2e
            // измеряется секундами; AbortMinAgeSec=3 — abort-сценарий без долгого
            // ожидания свежести. AdvertisedPublisherHost: подписки ходят ИЗ
            // контейнеров приёмников — на single-host стенде издатель виден
            // как host.docker.internal.
            ["PgWorker__Moves__FailoverSlots"] = "true",
            ["PgWorker__Moves__FreezeWaitSec"] = "1",
            ["PgWorker__Moves__PollIntervalSec"] = "1",
            ["PgWorker__Moves__AbortMinAgeSec"] = "3",
            ["PgWorker__Moves__AdvertisedPublisherHost"] = "host.docker.internal",
            ["PgWorker__Thresholds__CutoverTimeoutSec"] = "60",
            ["PgWorker__Thresholds__ConnFailBudgetSec"] = "15",
            ["PgWorker__Parallelism__MaxClusters"] = "2",
            ["PgWorker__Snapshots__Dir"] = snapshotsDir,
            ["PgWorker__Snapshots__RetentionFiles"] = "10",

            // mTLS HTTP API (t03, arch/14 §1.1): статический per-process пакет —
            // PEM-дуализм env освобождает от файлов; порт — свободный зонд.
            ["PGW_API_TLS_CERT"] = _serverCertPem,
            ["PGW_API_TLS_KEY"] = _serverKeyPem,
            ["PGW_API_TLS_CLIENT_CA"] = _installCa.CaPem,
            ["PgWorker__Api__AdvertiseUrl"] = $"https://127.0.0.1:{port}",

            ["ASPNETCORE_URLS"] = $"https://127.0.0.1:{port}",
            ["DOTNET_ENVIRONMENT"] = "Production",
        };

        var process = new Process
        {
            StartInfo = new ProcessStartInfo("dotnet", [_appDll])
            {
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                WorkingDirectory = snapshotsDir,
            },
        };
        foreach (var (key, value) in env)
            process.StartInfo.Environment[key] = value;

        foreach (var (key, value) in extraEnv ?? new Dictionary<string, string>())
            process.StartInfo.Environment[key] = value;

        if (!process.Start())
            throw new ApplicationException($"не удалось запустить инстанс {name}");

        // Читаем вывод в фоне (иначе буфер пайпа переполнится и процесс зависнет);
        // последние строки попадают в диагностику при неудачном старте, полный
        // лог — в host.log каталога снапшотов (writer живёт столько же, сколько
        // инстанс — закрывается в HostInstance.DisposeAsync).
        var tail = new Queue<string>();
        var logWriter = new StreamWriter(Path.Combine(snapshotsDir, "host.log"), append: false) { AutoFlush = true };
        process.OutputDataReceived += (_, e) => { Collect(tail, e.Data); if (e.Data is not null) logWriter.WriteLine(e.Data); };
        process.ErrorDataReceived += (_, e) => { Collect(tail, e.Data); if (e.Data is not null) logWriter.WriteLine(e.Data); };
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        var instance = new HostInstance(name, process, snapshotsDir, _healthHttp, logWriter);
        _hosts.Add(instance);

        // Готовность: /healthz отвечает (любой статус, кроме 404 = маршрут жив).
        var ready = await E2eFixture.WaitForAsync(async () =>
        {
            if (process.HasExited)
                throw new ApplicationException(
                    $"инстанс {name} упал при старте (exit {process.ExitCode}):\n{string.Join("\n", tail)}");
            try
            {
                using var response = await _healthHttp.GetAsync(
                    $"https://127.0.0.1:{port}/healthz", CancellationToken.None);
                return response.StatusCode != System.Net.HttpStatusCode.NotFound;
            }
            catch (HttpRequestException)
            {
                return false;
            }
            catch (OperationCanceledException)
            {
                // 3-с таймаут клиента: mTLS-хендшейк при буте инстанса под нагрузкой
                // дольше plain-http до t03 — проба честно повторится в следующем
                // такте WaitForAsync, нода ещё не готова.
                return false;
            }
        }, TimeSpan.FromSeconds(30), ct);
        if (!ready)
        {
            await instance.DisposeAsync();
            throw new ApplicationException($"инстанс {name} не поднялся за 30 с:\n{string.Join("\n", tail)}");
        }

        return instance;
    }

    /// <summary>
    /// Пароль app-роли кластера из etcd (spec §3.1): e2e-сценарии читают секрет
    /// тем же путём, что и приложение — /clusters/&lt;C&gt;/app_password.
    /// </summary>
    public async Task<string> GetAppPasswordAsync(string cluster, CancellationToken ct = default)
    {
        var result = await Gateway.GetAsync(EtcdEndpoint, $"/clusters/{cluster}/app_password", ct);
        result.IsSuccess.Should().BeTrue("app-секрет обязан появиться после provisioning");
        return result.Value!.Value;
    }

    public Task<string> RunDockerAsync(string[] args, CancellationToken ct = default)
        => E2eFixture.RunProcessAsync("docker", args, ct);

    /// <summary>
    /// Полный teardown при ЛЮБОМ исходе: kill воркеров → stop/rm контейнеров
    /// → rm томов → rm сети окружения → prune осиротевших сетей (pgw-net движка)
    /// → АССЕРТ ЧИСТОТЫ: не осталось ни одного контейнера/тома/сети окружения.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        var problems = new List<string>();

        // 1) Воркеры окружения (идемпотентно: сценарий мог уже disposed-ить).
        foreach (var host in _hosts)
            try
            {
                await host.DisposeAsync();
            }
            catch (Exception e)
            {
                problems.Add($"воркер {host.Name}: {e.Message}");
            }

        // 2) etcd и MinIO (testcontainers: stop + rm; ключи умирают вместе с etcd).
        try
        {
            if (_minio is not null)
                await _minio.DisposeAsync();
        }
        catch (Exception e)
        {
            problems.Add($"minio: {e.Message}");
        }

        try
        {
            await _etcd.DisposeAsync();
        }
        catch (Exception e)
        {
            problems.Add($"etcd: {e.Message}");
        }

        _gatewayHttp.Dispose();

        // 3) Контейнеры, созданные воркерами окружения (pg-ноды pgw-<C>-*,
        // backup-джобы pgw-backup-*): прогон последовательный, в этот момент всё
        // pgw-* — артефакты ТОЛЬКО этого окружения (контейнеры dev-стенда
        // as-*/deploy-* фильтр не задевает).
        try
        {
            var ids = (await E2eFixture.RunProcessAsync("docker", ["ps", "-aq", "--filter", "name=pgw-"]))
                .Split(['\n', ' '], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (ids.Length > 0)
                await E2eFixture.RunProcessAsync("docker", ["rm", "-f", .. ids]);
        }
        catch (Exception e)
        {
            problems.Add($"rm контейнеров pgw-*: {e.Message}");
        }

        // 4) Тома окружения: pg-данные pgw-<C>-* и staging pgw-backup-*.
        // docker rm -f возвращает управление до фактического освобождения
        // volume-ссылки демоном (гонка Docker Desktop) — ретраим с бюджетом ~20 с.
        // Фильтр docker — SUBSTRING: чужой deploy_pgw-snapshots стенда не удаляем
        // (якорим префикс pgw- в C#).
        try
        {
            foreach (var id in (await E2eFixture.RunProcessAsync(
                         "docker", ["volume", "ls", "-q", "--filter", "name=pgw-"]))
                     .Split(['\n', ' '], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                     .Where(v => v.StartsWith("pgw-", StringComparison.Ordinal)))
                for (var attempt = 0; ; attempt++)
                {
                    try
                    {
                        await E2eFixture.RunProcessAsync("docker", ["volume", "rm", "-f", id]);
                        break;
                    }
                    catch (ApplicationException) when (attempt < 10)
                    {
                        await Task.Delay(2000, TestContext.Current.CancellationToken);
                    }
                }
        }
        catch (Exception e)
        {
            problems.Add($"rm томов pgw-*: {e.Message}");
        }

        // 5) Сеть окружения (контейнеры уже отвязаны).
        try
        {
            await _net.DeleteAsync();
        }
        catch (Exception e)
        {
            problems.Add($"сеть {NetName}: {e.Message}");
        }

        // 6) Осиротевшие сети движка (pgw-net, ryuk их не подбирает): prune
        // трогает только свободные сети — живые сети dev-стенда не пострадают.
        try
        {
            await E2eFixture.RunProcessAsync("docker", ["network", "prune", "-f"]);
        }
        catch (Exception e)
        {
            problems.Add($"network prune: {e.Message}");
        }

        // 7) АССЕРТ ЧИСТОТЫ: окружение не оставляет следов.
        var leftContainers = await E2eFixture.RunProcessAsync("docker", ["ps", "-aq", "--filter", "name=pgw-"]);
        if (leftContainers.Length > 0)
            problems.Add($"остались контейнеры pgw-*: {leftContainers.Replace('\n', ' ')}");
        var leftVolumes = (await E2eFixture.RunProcessAsync(
                "docker", ["volume", "ls", "-q", "--filter", "name=pgw-"]))
            .Split(['\n', ' '], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(v => v.StartsWith("pgw-", StringComparison.Ordinal))
            .ToList();
        if (leftVolumes.Count > 0)
            problems.Add($"остались тома pgw-*: {string.Join(' ', leftVolumes)}");
        var leftNet = await E2eFixture.RunProcessAsync(
            "docker", ["network", "ls", "-q", "--filter", $"name={NetName}"]);
        if (leftNet.Length > 0)
            problems.Add($"осталась сеть окружения {NetName}");

        if (problems.Count > 0)
            throw new ApplicationException(
                $"{Slug}: teardown окружения неполный:\n- " + string.Join("\n- ", problems));
    }

    // ===== Статические ассеты: PKI, бинарь, образы =====

    private static async Task EnsureStaticAsync(CancellationToken ct)
    {
        if (_staticReady)
            return;
        await StaticGate.WaitAsync(ct);
        try
        {
            if (_staticReady)
                return;

            // mTLS-пакет (t03): CA + серверный серт инстансов (SAN 127.0.0.1/localhost
            // — ASPNETCORE_URLS https://127.0.0.1) + клиентская пара health-клиента.
            // PFX round-trip клиентского серта: эфемерный ключ CreateFromPem не годится
            // для SslStream (паттерн MtlsApiTests).
            _installCa = E2eTestPki.GenerateCa("e2e");
            (_serverCertPem, _serverKeyPem) = E2eTestPki.Issue(
                _installCa.CaPem, _installCa.CaKeyPem, "pgworker", ["localhost", "127.0.0.1"], ip: null);
            var (clientPem, clientKeyPem) = E2eTestPki.Issue(
                _installCa.CaPem, _installCa.CaKeyPem, "e2e-healthcheck", ["e2e-healthcheck"], ip: null);
            var pemPair = X509Certificate2.CreateFromPem(clientPem, clientKeyPem);
            var clientCert = X509CertificateLoader.LoadPkcs12(pemPair.Export(X509ContentType.Pkcs12), null);
            _healthHttp = new HttpClient(new SocketsHttpHandler
            {
                SslOptions = new System.Net.Security.SslClientAuthenticationOptions
                {
                    // TLS 1.2: macOS SslStream не шлёт клиентские серты в TLS 1.3 (runtime#37961).
                    EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                    ClientCertificates = [clientCert],
                    RemoteCertificateValidationCallback = (_, _, _, _) => true, // тест доверяет фикстурной CA
                },
            })
            { Timeout = TimeSpan.FromSeconds(3) };

            // Корень репозитория и артефакты: от каталога тестовой сборки вверх.
            _root = E2eFixture.FindRoot(AppContext.BaseDirectory);
            // Автосборка Release до docker-сборок (быстрый fail); NOBUILD — лазейка t09.
            await E2eFixture.EnsureAppDllAsync(
                _root, Environment.GetEnvironmentVariable("PGW_TEST_E2E_NOBUILD") == "1");
            _appDll = Path.Combine(_root, "src", "PgWorker.App", "bin", "Release", "net10.0", "PgWorker.App.dll");

            // Образ узла (задача 25): собирается ДО запуска процессов; docker-cache
            // делает повторные сборки инкрементальными.
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-q", "-f", $"{_root}/docker/node/Dockerfile", "-t", NodeImage, _root,
            ], ct);

            _staticReady = true;
        }
        finally
        {
            StaticGate.Release();
        }
    }

    private static async Task EnsureJobImageAsync(CancellationToken ct)
    {
        if (_jobImageReady)
            return;
        await StaticGate.WaitAsync(ct);
        try
        {
            if (_jobImageReady)
                return;
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-q", "-f", $"{_root}/docker/PgWorker.Backup.Dockerfile", "-t", JobImage, _root,
            ], ct);
            _jobImageReady = true;
        }
        finally
        {
            StaticGate.Release();
        }
    }

    private static void Collect(Queue<string> tail, string? line)
    {
        if (line is null)
            return;
        lock (tail)
        {
            tail.Enqueue(line);
            while (tail.Count > 200)
                tail.Dequeue();
        }
    }

    private static string Slugify(string slug)
    {
        var chars = slug.ToLowerInvariant().Select(c => char.IsAsciiLetterOrDigit(c) ? c : '-').ToArray();
        return new string(chars);
    }
}
