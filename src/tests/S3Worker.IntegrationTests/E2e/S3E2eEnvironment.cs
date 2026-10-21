using System.Diagnostics;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using S3Worker.IntegrationTests.S3;
using Shared.Etcd.Client;
using Xunit;

namespace S3Worker.IntegrationTests.E2e;

/// <summary>
/// Изолированное docker-E2E окружение S3Worker (docs/e2e-isolation.md +
/// docs/e2e-launch.md): собственная docker-сеть s3w-en-{runId}, свой etcd
/// s3w-ee-{runId}, свой воркер s3w-ew(1|2)-{runId} (образ s3worker:e2e-{runId}
/// — свежий Release, publish НА ХОСТЕ + узкий COPY-контекст артефактов; канон
/// E2E-образов AGENTS.md) + owns3:dev для нод — ВНЕШНИЙ статический ассет
/// (репозиторий ../OwnS3, freshness контролирует разработчик пересборкой
/// deploy/build-image.sh там; pg его не собирает — fail-fast с командой).
/// Идентификатор прогона — полный guid во всех именах; чистка и ассерт чистоты
/// — строго по нему. Телеметрия: docker-логи/inspect всех контейнеров и списки
/// docker ps/volume — в /tmp/pgw-e2e-artifacts-{runId}/ ДО удаления;
/// MarkFailed() → teardown ОСТАНАВЛИВАЕТ контейнеры, но не удаляет
/// (README-cleanup.txt с own-only командами). Retry старта против гонок prune.
/// Гейт запуска: PGW_TEST_DOCKER=1 (нет — Skip в тесте); PGW_TEST_E2E_NOBUILD=1
/// — пропустить сборку образов (бисект: s3worker:e2e — ручной тег; owns3:dev
/// всегда внешний — отсутствие = fail-fast с командой сборки в ../OwnS3).
/// </summary>
public sealed class S3E2eEnvironment : IAsyncDisposable
{
    private const string EtcdImage = "quay.io/coreos/etcd:v3.5.21";
    private const string OwnS3Image = "owns3:dev";
    private const string S3WPublishDir = "artifacts/e2e/s3worker";
    private const string WorkerImageFallback = "s3worker:e2e"; // NOBUILD-бисект

    // Креды кластеров — per-cluster secrets-ключ etcd /s3/clusters/<C>/secrets
    // (генерирует воркер; arch/23 §2/§6): сценарии читают root-пару хелпером
    // RootPairOfAsync — значения ТОЛЬКО в память теста, БЕЗ печати.
    public const string BucketName = "pgworker-backups";

    private readonly IContainer _etcd;
    private readonly IContainer _worker;
    private readonly IContainer? _worker2; // двухинстансный контур (StartTwoAsync)
    private IContainer[] _workers; // воркеры окружения: 1 или 2 (teardown/телеметрия)
    private readonly string[] _workerNames; // docker-имена воркеров (ассерт чистоты)
    private readonly INetwork _net;
    private readonly HttpClient _gatewayHttp = new();

    private bool _failed;

    private readonly string _runId;
    private readonly int _api1Port;
    private readonly int? _api2Port; // двухинстансный контур; null — одиночный

    private S3E2eEnvironment(
        string slug, string runId, string netName, INetwork net, IContainer etcd, IContainer worker,
        string etcdEndpoint, string artifactsDir, string apiBaseUrl, string clientPem, string clientKeyPem,
        IContainer? worker2 = null, int? api2Port = null)
    {
        Slug = slug;
        _runId = runId;
        NetName = netName;
        _net = net;
        _etcd = etcd;
        _worker = worker;
        _worker2 = worker2;
        _workers = worker2 is null ? [worker] : [worker, worker2];
        _workerNames = worker2 is null
            ? [$"s3w-ew-{runId}"]
            : [$"s3w-ew1-{runId}", $"s3w-ew2-{runId}"];
        EtcdEndpoint = etcdEndpoint;
        ArtifactsDir = artifactsDir;
        ApiBaseUrl = apiBaseUrl;
        Gateway = new EtcdGateway(_gatewayHttp);
        (ClientPem, ClientKeyPem) = (clientPem, clientKeyPem);
        _api1Port = new Uri(apiBaseUrl).Port;
        _api2Port = api2Port;
    }

    public string Slug { get; }

    public string ArtifactsDir { get; }

    /// <summary>etcd окружения: published порт на хосте.</summary>
    public string EtcdEndpoint { get; }

    /// <summary>База API воркера (https://localhost:{динамический порт}).</summary>
    public string ApiBaseUrl { get; }

    // ── Двухинстансный контур (StartTwoAsync): базовые URL/порты/имена ОБИХ
    // воркеров — основа резолва держателя клэйма (claims.instance → api-ключ →
    // порт → сопоставление с Api1/Api2Port). В одиночном контуре Api1* дублирует
    // ApiBaseUrl, вторые члены — исключение.

    /// <summary>База API первого воркера (s3w-ew1): в одиночном — сам контур.</summary>
    public string Api1BaseUrl => ApiBaseUrl;

    /// <summary>Хост-порт API первого воркера (резолв держателя клэйма).</summary>
    public int Api1Port => _api1Port;

    /// <summary>База API второго воркера (s3w-ew2) — только StartTwoAsync.</summary>
    public string Api2BaseUrl
        => _api2Port is { } port
            ? $"https://localhost:{port}"
            : throw new InvalidOperationException("no second API: environment was started with StartAsync (single-instance contour)");

    /// <summary>Хост-порт API второго воркера — только StartTwoAsync.</summary>
    public int Api2Port
        => _api2Port
           ?? throw new InvalidOperationException("no second API: environment was started with StartAsync (single-instance contour)");

    /// <summary>Docker-имя первого воркера (s3w-ew1-{runId}; одиночный — s3w-ew).</summary>
    public string Worker1Name => _workerNames[0];

    /// <summary>Docker-имя второго воркера (s3w-ew2-{runId}) — только StartTwoAsync.</summary>
    public string Worker2Name
        => _worker2 is null
                ? throw new InvalidOperationException("no second worker: environment was started with StartAsync (single-instance contour)")
            : _workerNames[1];

    public string ClusterTag => _runId[..8];

    public string NetName { get; }

    public string ClientPem { get; }

    public string ClientKeyPem { get; }

    public EtcdGateway Gateway { get; }

    public void MarkFailed() => _failed = true;

    /// <summary>Root-пара кластера из per-cluster secrets-ключа etcd
    /// (arch/23 §2); значения — только в память теста, БЕЗ печати.</summary>
    public async Task<(string RootUser, string RootPassword)> RootPairOfAsync(
        string cluster, CancellationToken ct)
    {
        var secrets = (await Gateway.GetAsync(EtcdEndpoint,
            $"/s3/clusters/{cluster}/secrets", ct)).Value!.Value;
        using var doc = System.Text.Json.JsonDocument.Parse(secrets);
        return (doc.RootElement.GetProperty("root_user").GetString()!,
            doc.RootElement.GetProperty("root_password").GetString()!);
    }

    /// <summary>Подъём окружения (3 попытки — против гонок глобального prune).</summary>
    public static async Task<S3E2eEnvironment> StartAsync(string slug)
    {
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
            throw new InvalidOperationException("E2E requires PGW_TEST_DOCKER=1 (otherwise Skip in the test)");

        var runId = Guid.NewGuid().ToString("N");
        var artifactsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-artifacts-{runId}");
        Directory.CreateDirectory(Path.Combine(artifactsDir, "tls"));

        // Статические ассеты процесса: owns3:dev — ВНЕШНИЙ образ (репозиторий
        // ../OwnS3, freshness контролирует разработчик пересборкой там) и
        // s3worker:e2e-{runId} (свежий Release, узкий COPY-контекст).
        var workerImage = await BuildImagesAsync(runId);

        // TLS-пакет окружения: self-signed CA + server + client (PEM в артефакты).
        var (caPem, caKeyPem) = GenerateCa("s3w-e2e");
        var (serverCert, serverKey) = IssueCert(caPem, caKeyPem, "s3worker");
        var (clientCert, clientKey) = IssueCert(caPem, caKeyPem, "e2e-client");
        var tlsDir = Path.Combine(artifactsDir, "tls");
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "ca.pem"), caPem);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "server.crt"), serverCert);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "server.key"), serverKey);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "client.key"), clientKey);

        for (var attempt = 1; ; attempt++)
        {
            var netName = $"s3w-en-{runId}";
            var net = new NetworkBuilder().WithName(netName).Build();
            var etcdPort = FreePort();
            var etcd = new ContainerBuilder(EtcdImage)
                .WithName($"s3w-ee-{runId}")
                .WithPortBinding(etcdPort, 2379)
                .WithCommand("etcd", "--name=test", "--data-dir=/etcd-data",
                    "--listen-client-urls=http://0.0.0.0:2379",
                    "--advertise-client-urls=http://127.0.0.1:2379")
                .Build();
            var apiPort = FreePort();
            var portRange = FreePortWindow.Find();
            var worker = new ContainerBuilder(workerImage)
                .WithName($"s3w-ew-{runId}")
                .WithNetwork(net)
                .WithPortBinding(apiPort, 8080)
                .WithBindMount("/var/run/docker.sock", "/var/run/docker.sock")
                .WithBindMount(tlsDir, "/tls")
                .WithEnvironment(WorkerEnv(etcdPort, apiPort, portRange))
                .WithExtraHost("host.docker.internal", "host-gateway")
                .WithExtraHost("local", "host-gateway")
                .Build();

            try
            {
                await net.CreateAsync();
                await etcd.StartAsync();
                await worker.StartAsync();
                var environment = new S3E2eEnvironment(slug, runId, netName, net, etcd, worker,
                    $"http://localhost:{etcdPort}", artifactsDir,
                    $"https://localhost:{apiPort}", clientCert, clientKey);
                await environment.WriteAsync("README-cleanup.txt",
                    "# Run environment cleanup (own-only):\n"
                    + $"docker rm -f s3w-ew-{runId} s3w-ee-{runId}\n"
                    + $"docker ps -aq --filter name={environment.ClusterTag} | xargs -r docker rm -f\n"
                    + $"docker volume ls -q --filter name={environment.ClusterTag} | xargs -r docker volume rm -f\n"
                    + $"docker network rm {netName}\n");
                return environment;
            }
            catch (Exception ex) when (attempt < 3
                && ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase))
            {
                // ретрай только на распознанную гонку сети — подъём заново
                await SafeStopAsync(worker, remove: true);
                await SafeStopAsync(etcd, remove: true);
                try { await net.DeleteAsync(); } catch { /* уже нет */ }
                continue;
            }
            catch (Exception)
            {
                await SafeStopAsync(worker, remove: true);
                await SafeStopAsync(etcd, remove: true);
                try { await net.DeleteAsync(); } catch { /* уже нет */ }
                throw;
            }
        }
    }

    /// <summary>Подъём ДВУХИНСТАНСНОГО контура (spec §5.4 takeover): тот же
    /// каркас, что StartAsync (TLS-пакет, сеть, etcd, образы, retry), но ДВА
    /// контейнера s3w-ew1/ew2-{runId} с общими TLS/etcd/PortRange (гонки
    /// portalloc закрывает клэйм кластера) и УНИКАЛЬНЫМ Api__AdvertiseUrl
    /// каждому (одинаковые URL гасят оба дискавери-ключа). Ускоренные циклы —
    /// env-оверрайдами. Поведение одиночного StartAsync не меняется.</summary>
    public static async Task<S3E2eEnvironment> StartTwoAsync(string slug)
    {
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
            throw new InvalidOperationException("E2E requires PGW_TEST_DOCKER=1 (otherwise Skip in the test)");

        var runId = Guid.NewGuid().ToString("N");
        var artifactsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-artifacts-{runId}");
        Directory.CreateDirectory(Path.Combine(artifactsDir, "tls"));

        var workerImage = await BuildImagesAsync(runId);

        // TLS-пакет окружения общий для обоих инстансов — один mTLS-клиент
        // годит обоим (takeover: mTLS-клиент к выжившему).
        var (caPem, caKeyPem) = GenerateCa("s3w-e2e");
        var (serverCert, serverKey) = IssueCert(caPem, caKeyPem, "s3worker");
        var (clientCert, clientKey) = IssueCert(caPem, caKeyPem, "e2e-client");
        var tlsDir = Path.Combine(artifactsDir, "tls");
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "ca.pem"), caPem);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "server.crt"), serverCert);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "server.key"), serverKey);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "client.key"), clientKey);

        for (var attempt = 1; ; attempt++)
        {
            var netName = $"s3w-en-{runId}";
            var net = new NetworkBuilder().WithName(netName).Build();
            var etcdPort = FreePort();
            var etcd = new ContainerBuilder(EtcdImage)
                .WithName($"s3w-ee-{runId}")
                .WithPortBinding(etcdPort, 2379)
                .WithCommand("etcd", "--name=test", "--data-dir=/etcd-data",
                    "--listen-client-urls=http://0.0.0.0:2379",
                    "--advertise-client-urls=http://127.0.0.1:2379")
                .Build();
            var api1Port = FreePort();
            var api2Port = FreePort();
            var portRange = FreePortWindow.Find();

            IContainer WorkerContainer(int apiPort, string name)
                => new ContainerBuilder(workerImage)
                    .WithName(name)
                    .WithNetwork(net)
                    .WithPortBinding(apiPort, 8080)
                    .WithBindMount("/var/run/docker.sock", "/var/run/docker.sock")
                    .WithBindMount(tlsDir, "/tls")
                    .WithEnvironment(new Dictionary<string, string>(WorkerEnv(etcdPort, apiPort, portRange))
                    {
                        // Ускоренные циклы takeover-сценария (spec §5.4): тик
                        // держателя 1 с — клэйм живёт короткими keepalive.
                        ["S3Worker__Loops__KeepaliveSec"] = "1",
                        ["S3Worker__Loops__ErrorDelayMs"] = "500",
                    })
                    .WithExtraHost("host.docker.internal", "host-gateway")
                    .WithExtraHost("local", "host-gateway")
                    .Build();

            var worker1 = WorkerContainer(api1Port, $"s3w-ew1-{runId}");
            var worker2 = WorkerContainer(api2Port, $"s3w-ew2-{runId}");

            try
            {
                await net.CreateAsync();
                await etcd.StartAsync();
                await worker1.StartAsync();
                await worker2.StartAsync();
                var environment = new S3E2eEnvironment(slug, runId, netName, net, etcd, worker1,
                    $"http://localhost:{etcdPort}", artifactsDir,
                    $"https://localhost:{api1Port}", clientCert, clientKey,
                    worker2, api2Port);
                await environment.WriteAsync("README-cleanup.txt",
                    "# Run environment cleanup (own-only):\n"
                    + $"docker rm -f s3w-ew1-{runId} s3w-ew2-{runId} s3w-ee-{runId}\n"
                    + $"docker ps -aq --filter name={environment.ClusterTag} | xargs -r docker rm -f\n"
                    + $"docker volume ls -q --filter name={environment.ClusterTag} | xargs -r docker volume rm -f\n"
                    + $"docker network rm {netName}\n");
                return environment;
            }
            catch (Exception ex) when (attempt < 3
                && ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase))
            {
                // ретрай только на распознанную гонку сети — подъём заново
                await SafeStopAsync(worker2, remove: true);
                await SafeStopAsync(worker1, remove: true);
                await SafeStopAsync(etcd, remove: true);
                try { await net.DeleteAsync(); } catch { /* уже нет */ }
                continue;
            }
            catch (Exception)
            {
                await SafeStopAsync(worker2, remove: true);
                await SafeStopAsync(worker1, remove: true);
                await SafeStopAsync(etcd, remove: true);
                try { await net.DeleteAsync(); } catch { /* уже нет */ }
                throw;
            }
        }
    }

    // Env воркера окружения (arch/24 §8): etcd/advertise/порты из динамических
    // значений; bucket per-install (креды кластеров — per-cluster в etcd, env
    // секретов не несёт); ускоренные циклы/пороги (ScanIntervalSec=1,
    // NodeBootSec=100, NodeDeadSec=15, HealthIntervalSec=3); mTLS-серты из
    // bind /tls.
    private static Dictionary<string, string> WorkerEnv(int etcdPort, int apiPort, (int From, int To) portRange)
        => new()
        {
            ["S3Worker__Etcd__Endpoints__0"] = $"http://host.docker.internal:{etcdPort}",
            ["S3Worker__Docker__AdvertisedHost"] = "host.docker.internal",
            ["S3Worker__Api__AdvertiseUrl"] = $"https://host.docker.internal:{apiPort}",
            ["S3Worker__Docker__PortRange__From"] = portRange.From.ToString(),
            ["S3Worker__Docker__PortRange__To"] = portRange.To.ToString(),
            ["S3Worker__Docker__Images__OwnS3"] = OwnS3Image,
            ["S3Worker__Bucket"] = BucketName,
            ["S3Worker__Loops__ScanIntervalSec"] = "1",
            ["S3Worker__OwnS3__ReplicationReconcileIntervalSec"] = "10",
            ["S3Worker__Thresholds__NodeBootSec"] = "100",
            ["S3Worker__Thresholds__NodeDeadSec"] = "15",
            ["S3Worker__Thresholds__HealthIntervalSec"] = "3",
            ["S3W_API_TLS_CERT_PATH"] = "/tls/server.crt",
            ["S3W_API_TLS_KEY_PATH"] = "/tls/server.key",
            ["S3W_API_TLS_CLIENT_CA_PATH"] = "/tls/ca.pem",
        };

    // Сборка образов ([PHASE]-метки и тайминг — канон E2E-образов AGENTS.md):
    // owns3:dev — ВНЕШНИЙ статический ассет (репозиторий ../OwnS3, freshness
    // контролирует разработчик пересборкой deploy/build-image.sh там; pg его
    // не собирает — fail-fast с командой); s3worker:e2e-{runId} — self-build:
    // publish S3Worker.App + build (тег per-прогон — свежесть каждого прогона).
    // NOBUILD=1 — только бисект: s3worker берётся из ручного тега s3worker:e2e.
    private static async Task<string> BuildImagesAsync(string runId)
    {
        await EnsureImageAsync(OwnS3Image, "bash ../OwnS3/deploy/build-image.sh");

        var noBuild = Environment.GetEnvironmentVariable("PGW_TEST_E2E_NOBUILD") == "1";
        if (noBuild)
        {
            await EnsureImageAsync(WorkerImageFallback,
                $"dotnet publish src/S3Worker.App/S3Worker.App.csproj -c Release -o {S3WPublishDir} && "
                + $"docker build -f docker/S3Worker.Dockerfile -t {WorkerImageFallback} {S3WPublishDir}");
            return WorkerImageFallback;
        }

        var root = FindRepoRoot();
        var ct = CancellationToken.None;

        var workerImage = $"s3worker:e2e-{runId}";
        var s3wT0 = Stopwatch.GetTimestamp();
        Console.WriteLine($">>> [PHASE] e2e publish S3Worker.App ({DateTime.Now:HH:mm:ss})");
        await RunHostAsync(root, "dotnet",
            ["publish", "src/S3Worker.App/S3Worker.App.csproj", "-c", "Release", "-o", S3WPublishDir, "--nologo"], ct);
        await RunHostAsync(root, "docker",
            ["build", "-q", "-f", "docker/S3Worker.Dockerfile", "-t", workerImage, S3WPublishDir], ct);
        Console.WriteLine($">>> [PHASE] e2e {workerImage} ready ({Elapsed(s3wT0)})");
        return workerImage;
    }

    private static string Elapsed(long startedTimestamp)
        => $"{Stopwatch.GetElapsedTime(startedTimestamp).TotalSeconds:F1}s";

    // Fail-fast: образа нет — исключение с командой сборки (owns3:dev — внешний
    // ассет ../OwnS3; s3worker в NOBUILD — ручной тег).
    private static async Task EnsureImageAsync(string image, string buildCommand)
    {
        var inspect = new ProcessStartInfo("docker", $"image inspect {image}")
        { RedirectStandardOutput = true, RedirectStandardError = true };
        using var process = Process.Start(inspect)!;
        await process.WaitForExitAsync();
        if (process.ExitCode != 0)
            throw new InvalidOperationException($"image {image} is missing — build it: {buildCommand}");
    }

    private static async Task RunHostAsync(
        string workingDir, string file, string[] args, CancellationToken ct)
    {
        var psi = new ProcessStartInfo(file, string.Join(' ', args))
        {
            WorkingDirectory = workingDir,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        using var process = Process.Start(psi)
            ?? throw new ApplicationException($"failed to start {file}");
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromMinutes(5));
        var outTask = process.StandardOutput.ReadToEndAsync();
        var errTask = process.StandardError.ReadToEndAsync();
        try
        {
            await process.WaitForExitAsync(timeoutCts.Token);
            if (process.ExitCode != 0)
            {
                // В ошибку — ОБА потока: MSBuild/dotnet пишут ошибки в STDOUT
                // (stderr пуст) — без него отказ publish неразличим («→ 1: »).
                var output = await outTask;
                throw new ApplicationException(
                    $"{file} {string.Join(' ', args)} → {process.ExitCode}: {await errTask}\nstdout: {output}");
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            try { process.Kill(entireProcessTree: true); } catch { /* уже вышел */ }
            throw new ApplicationException($"{file} {string.Join(' ', args)} did not finish within 5 min — killed");
        }
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "docker", "S3Worker.Dockerfile")))
            dir = dir.Parent;
        return dir?.FullName
            ?? throw new InvalidOperationException("repo root (docker/S3Worker.Dockerfile) not found");
    }

    // ── Хелперы docker-CLI (канон KafkaE2eEnvironment.RunProcessAsync:
    // таймаут убивает дерево, вывод — в исключение) ──

    /// <summary>docker-CLI: вывод stdout; ненулевой exit/таймаут — исключение
    /// с выводом процесса (зависший процесс умирает по бюджету 2 мин).</summary>
    public Task<string> RunDockerAsync(IReadOnlyList<string> args, CancellationToken ct)
        => RunProcessAsync("docker", [.. args], ct, TimeSpan.FromMinutes(2));

    private static async Task<string> RunProcessAsync(
        string file, string[] args, CancellationToken ct, TimeSpan timeout)
    {
        var psi = new ProcessStartInfo(file, args)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        using var process = Process.Start(psi)
            ?? throw new ApplicationException($"failed to start {file}");
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);
        // Оба потока параллельно и БЕЗ токена бюджета: при kill по бюджету пайпы
        // закрываются и накопленный вывод доступен (канон E2eFixture).
        var outTask = process.StandardOutput.ReadToEndAsync();
        var errTask = process.StandardError.ReadToEndAsync();
        try
        {
            await process.WaitForExitAsync(timeoutCts.Token);
            var output = await outTask;
            var error = await errTask;
            if (process.ExitCode != 0)
                throw new ApplicationException($"{file} {string.Join(' ', args)} → {process.ExitCode}: {error.Trim()}");
            return output.Trim();
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // Бюджет исчерпан: убиваем дерево, вывод — в диагностику.
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch
            {
                // процесс мог уже выйти
            }

            var drained = "";
            try
            {
                drained = string.Join("", await Task.WhenAll(outTask, errTask).WaitAsync(TimeSpan.FromSeconds(5)));
            }
            catch (TimeoutException)
            {
                // пайп завис — вывод недоступен
            }

            throw new ApplicationException(
                $"{file} {string.Join(' ', args)} did not finish within {timeout.TotalSeconds:0} s — killed; tail:\n{drained}");
        }
    }

    // mTLS-клиент API (клиентский серт окружения — от per-install CA;
    // PFX round-trip — macOS SslStream требует экспортируемый ключ).
    public HttpClient CreateApiHttpClient()
    {
        var pemCert = X509Certificate2.CreateFromPem(ClientPem, ClientKeyPem);
        var clientCert = X509CertificateLoader.LoadPkcs12(
            pemCert.Export(X509ContentType.Pkcs12), null);
        return new HttpClient(new SocketsHttpHandler
        {
            SslOptions = new()
            {
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                ClientCertificates = [clientCert],
                RemoteCertificateValidationCallback = (_, _, _, _) => true, // self-signed e2e-CA
            },
        })
        { BaseAddress = new Uri(ApiBaseUrl) };
    }

    // Перегрузка для двухинстансного контура (takeover — mTLS-клиент к
    // выжившему): тот же mTLS-клиент (TLS-пакет общий), заданный BaseAddress.
    public HttpClient CreateApiHttpClient(string baseUrl)
    {
        var pemCert = X509Certificate2.CreateFromPem(ClientPem, ClientKeyPem);
        var clientCert = X509CertificateLoader.LoadPkcs12(
            pemCert.Export(X509ContentType.Pkcs12), null);
        return new HttpClient(new SocketsHttpHandler
        {
            SslOptions = new()
            {
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                ClientCertificates = [clientCert],
                RemoteCertificateValidationCallback = (_, _, _, _) => true, // self-signed e2e-CA
            },
        })
        { BaseAddress = new Uri(baseUrl) };
    }

    // docker-факт: имя живого контейнера окружения (префикс/тег прогона).
    public async Task<bool> ContainerAliveAsync(string name)
    {
        var inspect = new ProcessStartInfo("docker", $"inspect --format {{{{.State.Running}}}} {name}")
        {
            RedirectStandardOutput = true,
        };
        using var process = Process.Start(inspect)!;
        var output = await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        return process.ExitCode == 0 && output.Trim() == "true";
    }

    // Docker-Id контейнера (идемпотентность: пересоздание меняет Id);
    // объекта нет — пустая строка.
    public async Task<string> ContainerIdAsync(string name)
    {
        var inspect = new ProcessStartInfo("docker", $"inspect --format {{{{.Id}}}} {name}")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = Process.Start(inspect)!;
        var output = await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        return process.ExitCode == 0 ? output.Trim() : string.Empty;
    }

    // Env контейнера ({{json .Config.Env}}) — сверка кредов нод с env воркера
    // (ротация): сравнение В КОДЕ теста, значения в лог не печатаются.
    public async Task<string> ContainerEnvJsonAsync(string name)
    {
        var inspect = new ProcessStartInfo("docker",
            $"inspect --format \"{{{{json .Config.Env}}}}\" {name}")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = Process.Start(inspect)!;
        var output = await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        return process.ExitCode == 0 ? output.Trim() : string.Empty;
    }

    // Docker-логи контейнера (stdout+stderr) — проверка ОТСУТСТВИЯ значений
    // секретов; снимок живёт только в памяти теста, без печати.
    public async Task<string> ContainerLogsAsync(string name)
    {
        var info = new ProcessStartInfo("docker", $"logs {name}")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = Process.Start(info)!;
        var output = await process.StandardOutput.ReadToEndAsync();
        var errors = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        return output + errors;
    }

    // Факт существования named volume (точное имя/префикс filter).
    public async Task<bool> VolumeExistsAsync(string name)
    {
        var inspect = new ProcessStartInfo("docker", $"volume inspect {name}")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = Process.Start(inspect)!;
        await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        return process.ExitCode == 0;
    }

    // Телеметрия (docs/e2e-launch.md §1): docker-логи+inspect всех контейнеров
    // окружения (воркеры — 1 или 2 + etcd), списки docker ps/volume ls и ноды
    // кластеров сценария (s3w-<tag>-*) — ДО любого удаления; вызывается в
    // teardown и на slow-phase.
    public async Task CollectDiagnosticsAsync(string mark)
    {
        foreach (var name in _workerNames.Append($"s3w-ee-{_runId}"))
        {
            await ShellToFileAsync("docker", $"logs --tail 2000 {name}",
                Path.Combine(ArtifactsDir, $"container-{name}-{mark}.log"));
            await ShellToFileAsync("docker", $"inspect {name}",
                Path.Combine(ArtifactsDir, $"container-{name}-{mark}.json"));
        }

        await ShellToFileAsync("docker", "ps -a --format '{{.Names}}\t{{.Status}}'",
            Path.Combine(ArtifactsDir, $"docker-ps-{mark}.txt"));
        await ShellToFileAsync("docker", "volume ls --format '{{.Name}}'",
            Path.Combine(ArtifactsDir, $"docker-volumes-{mark}.txt"));

        // Ноды кластеров сценария (s3w-<C>-*) — по тегу прогона.
        var ps = new ProcessStartInfo("docker",
            $"ps -a --format {{{{.Names}}}} --filter name={ClusterTag}")
        { RedirectStandardOutput = true };
        using var listing = Process.Start(ps)!;
        var names = (await listing.StandardOutput.ReadToEndAsync())
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        await listing.WaitForExitAsync();
        await File.WriteAllLinesAsync(Path.Combine(ArtifactsDir, $"nodes-{mark}.txt"), names);
        foreach (var name in names)
            await ShellToFileAsync("docker", $"logs --tail 2000 {name}",
                Path.Combine(ArtifactsDir, $"node-{name}-{mark}.log"));

        // etcd-факты домена (разбор «почему не сошлось» БЕЗ перезапуска):
        // config/endpoints/state нод/portalloc/journal — лучшее усилие.
        try
        {
            var facts = new List<string>();
            foreach (var prefix in new[] { "/s3/clusters/", "/s3worker/" })
            {
                var range = await Gateway.RangeAsync(EtcdEndpoint, prefix, CancellationToken.None);
                if (range.IsSuccess)
                    foreach (var kv in range.Value)
                        facts.Add($"{kv.Key} = {kv.Value}");
            }

            await File.WriteAllLinesAsync(Path.Combine(ArtifactsDir, $"etcd-facts-{mark}.txt"), facts);
        }
        catch
        {
            // телеметрия — лучшее усилие
        }
    }

    /// <summary>Отчёт по фазе (docs/e2e-launch.md §2): замер + строка [PHASE];
    /// фаза дольше 60 с — немедленный сбор логов в артефакты.</summary>
    public async Task<TimeSpan> WaitPhaseAsync(string phase, Func<Task<bool>> condition,
        TimeSpan budget, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        var deadline = DateTimeOffset.UtcNow + budget;
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition())
            {
                var elapsed = Stopwatch.GetElapsedTime(started);
                await WriteAsync($"phase-{phase}.txt",
                    $"[PHASE] {phase}: ok=true, elapsed={elapsed.TotalSeconds:F1}s");
                if (elapsed.TotalSeconds > 60)
                    await CollectDiagnosticsAsync($"slow-phase-{phase}");
                return elapsed;
            }

            await Task.Delay(500, ct);
        }

        await CollectDiagnosticsAsync($"timeout-{phase}");
        throw new TimeoutException($"[PHASE] {phase}: budget of {budget.TotalSeconds:F0} s exhausted");
    }

    public async ValueTask DisposeAsync()
    {
        // Телеметрия ДО любого удаления (docs/e2e-launch.md §1).
        try
        {
            await CollectDiagnosticsAsync("teardown");
        }
        catch
        {
            // телеметрия — лучшее усилие
        }

        if (_failed)
        {
            // docs/e2e-launch.md §3: упавший сценарий — контейнеры ОСТАНОВИТЬ,
            // не удалить (тома/сети/etcd остаются для разбора).
            foreach (var worker in _workers)
                await SafeStopAsync(worker, remove: false);
            await SafeStopAsync(_etcd, remove: false);
            return;
        }

        // Свои etcd-префиксы сносим ДО остановки etcd — ассерт чистоты ключей:
        // после teardown в окружении не остаётся НИ ОДНОГО ключа /s3/ /s3worker/
        // (etcd контура свой — там нет чужих данных).
        var ct = CancellationToken.None;
        await Gateway.DeleteAsync(EtcdEndpoint, "/s3/", prefix: true, ct);
        await Gateway.DeleteAsync(EtcdEndpoint, "/s3worker/", prefix: true, ct);
        var s3Left = await Gateway.RangeAsync(EtcdEndpoint, "/s3/", ct);
        var workerLeft = await Gateway.RangeAsync(EtcdEndpoint, "/s3worker/", ct);
        s3Left.Value.Should().BeEmpty("/s3/ keys removed by environment teardown");
        workerLeft.Value.Should().BeEmpty("/s3worker/ keys removed by environment teardown");

        foreach (var worker in _workers)
            await SafeStopAsync(worker, remove: true);
        await SafeStopAsync(_etcd, remove: true);
        try { await _net.DeleteAsync(); } catch { /* уже нет */ }
        _gatewayHttp.Dispose();

        // Ассерт чистоты: ни контейнера, ни тома, ни сети своего окружения
        // (двухинстансный контур проверяет ОБА имени воркеров; ноды кластеров
        // сценариев — по тегу прогона: воркер снёс их delete'ами сценариев,
        // при живом кластере демонтаж — обязанность воркера, остаток = брак).
        foreach (var name in _workerNames.Append($"s3w-ee-{_runId}"))
        {
            var inspect = new ProcessStartInfo("docker", $"inspect {name}")
            { RedirectStandardOutput = true, RedirectStandardError = true };
            using var process = Process.Start(inspect)!;
            await process.WaitForExitAsync();
            process.ExitCode.Should().NotBe(0, $"container {name} removed by teardown");
        }

        await AssertNoObjectsAsync($"name={ClusterTag}", "containers/volumes of the scenario tag");
        await AssertNoObjectsAsync($"name=s3w-{_runId}", $"containers/volumes of run {ClusterTag}");

        var networkInspect = new ProcessStartInfo("docker", $"network inspect {NetName}")
        { RedirectStandardOutput = true, RedirectStandardError = true };
        using var networkProcess = Process.Start(networkInspect)!;
        await networkProcess.WaitForExitAsync();
        networkProcess.ExitCode.Should().NotBe(0, $"network {NetName} removed by teardown");
    }

    // Нет ни контейнеров, ни томов по filter — иначе ассерт чистоты падает.
    private static async Task AssertNoObjectsAsync(string filter, string what)
    {
        var ps = new ProcessStartInfo("docker",
            $"ps -aq --filter {filter}")
        { RedirectStandardOutput = true };
        using var containers = Process.Start(ps)!;
        var containerOutput = (await containers.StandardOutput.ReadToEndAsync()).Trim();
        await containers.WaitForExitAsync();
        containerOutput.Should().BeEmpty($"no {what} left after teardown");

        var volumePs = new ProcessStartInfo("docker",
            $"volume ls -q --filter {filter}")
        { RedirectStandardOutput = true };
        using var volumes = Process.Start(volumePs)!;
        var volumeOutput = (await volumes.StandardOutput.ReadToEndAsync()).Trim();
        await volumes.WaitForExitAsync();
        volumeOutput.Should().BeEmpty($"no {what} left after teardown");
    }

    private static async Task SafeStopAsync(IContainer container, bool remove)
    {
        try
        {
            await container.StopAsync();
        }
        catch
        {
            // уже остановлен/не стартовал
        }

        if (remove)
        {
            try
            {
                await container.DisposeAsync();
            }
            catch
            {
                // уже удалён
            }
        }
    }

    public async Task WriteAsync(string fileName, string content)
        => await File.WriteAllTextAsync(Path.Combine(ArtifactsDir, fileName), content);

    private async Task ShellToFileAsync(string program, string args, string path)
    {
        try
        {
            var startInfo = new ProcessStartInfo(program, args)
            { RedirectStandardOutput = true, RedirectStandardError = true };
            using var process = Process.Start(startInfo)!;
            var output = await process.StandardOutput.ReadToEndAsync();
            await process.WaitForExitAsync();
            await File.WriteAllTextAsync(path, output);
        }
        catch
        {
            // телеметрия — лучшее усилие
        }
    }

    private static int FreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    // ── TLS-генерация окружения (self-signed, минимум для e2e) ──

    private static (string CertPem, string KeyPem) GenerateCa(string cn)
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var request = new System.Security.Cryptography.X509Certificates.CertificateRequest(
            $"CN={cn} CA", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(
            new System.Security.Cryptography.X509Certificates.X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(5));
        return (Pem(ca), Key(rsa));
    }

    private static (string CertPem, string KeyPem) IssueCert(string caPem, string caKeyPem, string cn)
    {
        // PFX round-trip (macOS): эфемерный ключ CreateFromPem не годится для
        // подписи сертификата — пере-импорт через PKCS#12.
        using var ca = System.Security.Cryptography.X509Certificates.X509CertificateLoader.LoadPkcs12(
            System.Security.Cryptography.X509Certificates.X509Certificate2
                .CreateFromPem(caPem, caKeyPem)
                .Export(System.Security.Cryptography.X509Certificates.X509ContentType.Pkcs12), null);
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var request = new System.Security.Cryptography.X509Certificates.CertificateRequest(
            $"CN={cn}", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        using var cert = request.Create(ca, DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(3), Guid.NewGuid().ToByteArray());
        return (Pem(cert), Key(rsa));
    }

    private static string Pem(System.Security.Cryptography.X509Certificates.X509Certificate2 cert)
        => cert.ExportCertificatePem().ReplaceLineEndings("\n").TrimEnd() + "\n";

    private static string Key(System.Security.Cryptography.RSA rsa)
        => rsa.ExportPkcs8PrivateKeyPem().ReplaceLineEndings("\n").TrimEnd() + "\n";
}
