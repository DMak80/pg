using System.Diagnostics;
using System.Net.Sockets;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using DotNet.Testcontainers.Volumes;
using FluentAssertions;
using Xunit;

namespace OwnS3.IntegrationTests.E2e;

/// <summary>
/// Изолированное docker-E2E окружение ownS3 (t39, docs/e2e-isolation.md +
/// docs/e2e-launch.md; модель KafkaE2eEnvironment — компактный standalone):
/// своя сеть os3-en-{runId}, свой контейнер os3-es-{runId} (образ owns3:e2e),
/// свой том os3-vd-{runId}. Готовность — wait-стратегия GET /healthz
/// (внутренний 9000, бюджет 30 с). Телеметрия: ВСЕ фазы — [PHASE]-метки с
/// таймингом в Console + UTC-записи в phases.log (ArtifactsDir); docker
/// logs/inspect ДО удалений (причины: "failed" — падение сценария,
/// "teardown", "slow-phase-*" — любая фаза дольше 60 с; к сборочным фазам
/// publish/build docker-диагностика неприменима — контейнера ещё нет, их
/// полные логи — process-build-*.log в ArtifactsDir). MarkFailed() → teardown
/// ОСТАНАВЛИВАЕТ контейнер, но не удаляет (README-cleanup.txt; own-only по
/// runId). Retry старта ×3 против гонок сети. Гейты: PGW_TEST_DOCKER=1
/// (Skip — DockerTrait), PGW_TEST_E2E_NOBUILD=1 — бисект.
/// </summary>
public sealed class OwnS3E2eEnvironment : IAsyncDisposable
{
    private const string ServiceImage = "owns3:e2e";

    /// <summary>Образ mc (клиент оператора; запинен, в images.txt).</summary>
    public const string McImage = "minio/mc:RELEASE.2025-08-13T08-35-41Z";

    // Литералы e2e-секретов (одноразовый контур; роли — клиентские кейсы).
    public const string RootUser = "os3e2e-root";
    public const string RootPassword = "os3e2e-root-secret";
    public const string WriterAccessKey = "os3e2e-writer";
    public const string WriterSecretKey = "os3e2e-writer-secret";
    public const string ReaderAccessKey = "os3e2e-reader";
    public const string ReaderSecretKey = "os3e2e-reader-secret";
    public const string AdminAccessKey = "os3e2e-admin";
    public const string AdminSecretKey = "os3e2e-admin-secret";

    // Статическая сборка образа — раз на процесс (прецедент E2eEnvironment
    // PgWorker): publish+build платит только первый Fact серии.
    private static readonly SemaphoreSlim StaticGate = new(1, 1);
    private static bool _staticReady;

    // Env вложенных dotnet-сборок: build-server/nodeReuse держат stdout-пайп
    // после завершения publish (e2e-факт) — реюз отключаем (канон E2eFixture).
    private static readonly IReadOnlyDictionary<string, string> NoMsBuildReuseEnv =
        new Dictionary<string, string>
        {
            ["DOTNET_CLI_DO_NOT_USE_MSBUILD_SERVER"] = "1",
            ["MSBUILDDISABLENODEREUSE"] = "1",
        };

    private readonly INetwork _net;
    private readonly IContainer _service;
    private readonly string _runId;
    private readonly string _containerName;
    private readonly string _volumeName;
    private readonly int _hostPort;
    private bool _failed;

    private OwnS3E2eEnvironment(string slug, string runId, INetwork net,
        string containerName, string volumeName, IContainer service,
        string artifactsDir, int hostPort)
    {
        Slug = slug;
        _runId = runId;
        _net = net;
        _containerName = containerName;
        _volumeName = volumeName;
        _service = service;
        ArtifactsDir = artifactsDir;
        _hostPort = hostPort;
    }

    public string Slug { get; }

    /// <summary>Артефакты телеметрии прогона (логи/inspect/phases.log).</summary>
    public string ArtifactsDir { get; }

    /// <summary>Docker-имя сети окружения (os3-en-{runId}).</summary>
    public string NetName => $"os3-en-{_runId}";

    /// <summary>Docker-имя контейнера сервиса (os3-es-{runId}).</summary>
    public string ContainerName => _containerName;

    /// <summary>S3-грань с хоста (SDK/HttpClient): динамический порт.</summary>
    public string S3BaseUrl => $"http://localhost:{_hostPort}";

    /// <summary>S3-грань изнутри docker-сети (mc: docker run --network).</summary>
    public string S3InternalUrl => $"http://{_containerName}:9000";

    /// <summary>Тег прогона (8 hex) — компонент имён бакетов сценариев.</summary>
    public string RunTag => _runId[..8];

    /// <summary>Полный guid прогона (ассерт чистоты, README-cleanup).</summary>
    public string RunId => _runId;

    public void MarkFailed() => _failed = true;

    /// <summary>Подъём окружения: статическая сборка образа → сеть → том →
    /// контейнер (wait /healthz ≤ 30 с); 3 попытки против распознанных гонок —
    /// подъём заново ЦЕЛИКОМ (полное удаление попытки).</summary>
    public static async Task<OwnS3E2eEnvironment> StartAsync(
        string slug, CancellationToken ct = default)
    {
        if (Environment.GetEnvironmentVariable(DockerTrait.EnvVar) != "1")
            throw new InvalidOperationException(
                $"E2E требует {DockerTrait.EnvVar}=1 (иначе Skip в тесте DockerTrait'ом)");

        var runId = Guid.NewGuid().ToString("N");
        var artifactsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-artifacts-{runId}");
        Directory.CreateDirectory(artifactsDir);

        // Статический ассет процесса: owns3:e2e (publish на хосте + CLI-build).
        await BuildImageOnceAsync(artifactsDir, ct);

        for (var attempt = 1; ; attempt++)
        {
            var net = new NetworkBuilder().WithName($"os3-en-{runId}").Build();
            var volumeName = $"os3-vd-{runId}";
            var volume = new VolumeBuilder().WithName(volumeName).Build();
            var containerName = $"os3-es-{runId}";
            var hostPort = FreePort();
            var service = new ContainerBuilder(ServiceImage)
                .WithName(containerName)
                .WithNetwork(net)
                .WithPortBinding(hostPort, 9000)
                .WithVolumeMount(volumeName, "/data")
                .WithEnvironment("OWNS3_ROOT_USER", RootUser)
                .WithEnvironment("OWNS3_ROOT_PASSWORD", RootPassword)
                .WithEnvironment("OWNS3_ACCESS_KEYS__0__ACCESSKEY", WriterAccessKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__0__SECRETKEY", WriterSecretKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__0__POLICY", "read-write")
                .WithEnvironment("OWNS3_ACCESS_KEYS__1__ACCESSKEY", ReaderAccessKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__1__SECRETKEY", ReaderSecretKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__1__POLICY", "read-only")
                .WithEnvironment("OWNS3_ACCESS_KEYS__2__ACCESSKEY", AdminAccessKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__2__SECRETKEY", AdminSecretKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__2__POLICY", "admin")
                .WithEnvironment("OWNS3_HOST_ID", $"os3-e2e-{runId[..8]}")
                .WithWaitStrategy(Wait.ForUnixContainer()
                    .UntilHttpRequestIsSucceeded(
                        request => request.ForPort(9000).ForPath("/healthz"),
                        wait => wait.WithTimeout(TimeSpan.FromSeconds(30))))
                .Build();

            async Task RemoveAttemptAsync()
            {
                await SafeStopAsync(service, remove: true);
                await TryDockerAsync(["volume", "rm", "-f", volumeName]);
                try { await net.DeleteAsync(ct); } catch { /* уже нет */ }
            }

            try
            {
                var sw = Stopwatch.StartNew();
                await net.CreateAsync(ct);
                await volume.CreateAsync(ct);
                await service.StartAsync(ct);
                var env = new OwnS3E2eEnvironment(slug, runId, net, containerName,
                    volumeName, service, artifactsDir, hostPort);
                await env.LogPhaseAsync("start", sw);
                await File.WriteAllTextAsync(Path.Combine(artifactsDir, "README-cleanup.txt"),
                    "# Зачистка окружения прогона (own-only, по runId):\n"
                    + $"docker rm -f {containerName}\n"
                    + $"docker volume rm -f {volumeName}\n"
                    + $"docker network rm {env.NetName}\n", ct);
                return env;
            }
            catch (Exception ex) when (attempt < 3
                && (ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase)
                    || ex.Message.Contains("bind", StringComparison.OrdinalIgnoreCase)
                    || ex.Message.Contains("port", StringComparison.OrdinalIgnoreCase)))
            {
                Console.WriteLine($"[PHASE] start: попытка {attempt} не удалась ({ex.Message.Split('\n')[0]}) — ретрай с чистого листа");
                await RemoveAttemptAsync();
                continue;
            }
            catch
            {
                await RemoveAttemptAsync();
                throw;
            }
        }
    }

    /// <summary>Сценарий под политикой телеметрии (docs/e2e-launch.md §3):
    /// исключение тела → телеметрия "failed" ДО остановки → MarkFailed →
    /// rethrow; teardown — DisposeAsync при любом исходе. Упавший прогон
    /// оставляет контейнер остановленным.</summary>
    public static async Task RunScenarioAsync(
        string slug, Func<OwnS3E2eEnvironment, Task> body, CancellationToken ct = default)
    {
        var fx = await StartAsync(slug, ct);
        try
        {
            await body(fx);
        }
        catch
        {
            try { await fx.CollectDiagnosticsAsync("failed"); }
            catch { /* телеметрия — лучшее усилие */ }
            fx.MarkFailed();
            throw;
        }
        finally
        {
            await fx.DisposeAsync();
        }
    }

    /// <summary>docker-CLI: stdout; ненулевой exit/таймаут — исключение с
    /// выводом процесса (бюджет 2 мин — зависший процесс умирает).</summary>
    public Task<string> RunDockerAsync(IReadOnlyList<string> args, CancellationToken ct)
        => RunProcessAsync("docker", [.. args], ct, TimeSpan.FromMinutes(2));

    /// <summary>Телеметрия ДО любых удалений (docs/e2e-launch.md §1).</summary>
    public async Task CollectDiagnosticsAsync(string mark)
    {
        await ShellToFileAsync($"logs --tail 2000 {_containerName}",
            Path.Combine(ArtifactsDir, $"container-{_containerName}-{mark}.log"));
        await ShellToFileAsync($"inspect {_containerName}",
            Path.Combine(ArtifactsDir, $"container-{_containerName}-{mark}.json"));
    }

    /// <summary>Финал фазы (docs/e2e-launch.md §2): [PHASE]-метка с таймингом
    /// в Console + UTC-запись в phases.log; фаза > 60 с — немедленный сбор
    /// docker-диагностики (slow-phase-*) независимо от исхода фазы.</summary>
    private async Task LogPhaseAsync(string name, Stopwatch sw)
    {
        sw.Stop();
        var line = $"phase {name}: ok, {sw.Elapsed.TotalSeconds:F1}s";
        Console.WriteLine($"[PHASE] {line}");
        await File.AppendAllTextAsync(Path.Combine(ArtifactsDir, "phases.log"),
            $"{DateTime.UtcNow:O} {line}\n");
        if (sw.Elapsed > TimeSpan.FromSeconds(60))
        {
            try { await CollectDiagnosticsAsync($"slow-phase-{name}-{(int)sw.Elapsed.TotalSeconds}s"); }
            catch { /* телеметрия — лучшее усилие */ }
        }
    }

    /// <summary>Полный teardown при любом исходе: телеметрия → dispose
    /// контейнера → volume rm с ретраями (демон освобождает асинхронно,
    /// бюджет ~30 с; превышение — громкий фейл ассерта) → сеть → ассерт
    /// чистоты по runId (own-only).</summary>
    public async ValueTask DisposeAsync()
    {
        var sw = Stopwatch.StartNew();
        try { await CollectDiagnosticsAsync("teardown"); }
        catch { /* телеметрия не маскирует исход сценария */ }

        if (_failed)
        {
            // docs/e2e-launch.md §3: контейнер ОСТАНОВИТЬ, не удалить (том/
            // сеть остаются для разбора; чистка — README-cleanup.txt вручную).
            await SafeStopAsync(_service, remove: false);
            await LogPhaseAsync("teardown", sw);
            return;
        }

        await SafeStopAsync(_service, remove: true);

        // volume: ретраи до ~30 с (полл 2 с).
        for (var i = 0; ; i++)
        {
            var left = await RunDockerOrNullAsync(
                ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name={_volumeName}"],
                CancellationToken.None);
            if (string.IsNullOrWhiteSpace(left) || i >= 15)
                break;
            await TryDockerAsync(["volume", "rm", "-f", _volumeName]);
            await Task.Delay(TimeSpan.FromSeconds(2));
        }

        try { await _net.DeleteAsync(); } catch { /* уже нет */ }

        // Ассерт чистоты: ни контейнера, ни тома, ни сети с runId прогона.
        var containers = await RunDockerOrNullAsync(
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name={_runId}"],
            CancellationToken.None);
        containers.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.Contains(_runId, StringComparison.Ordinal))
            .ToList()
            .Should().BeEmpty($"teardown убрал все контейнеры прогона {_runId}");
        (await RunDockerOrNullAsync(
                ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name={_volumeName}"],
                CancellationToken.None))
            .Trim().Should().BeEmpty($"после teardown не осталось тома {_volumeName}");
        (await RunDockerOrNullAsync(
                ["network", "ls", "--format", "{{.Name}}", "--filter", $"name={NetName}"],
                CancellationToken.None))
            .Trim().Should().BeEmpty($"после teardown не осталось сети {NetName}");

        // Фаза teardown — с таймингом и slow-сбором > 60 c (спец §6:
        // [PHASE] на publish/build/старт/teardown). После удалений docker-
        // диагностика недоступна — LogPhaseAsync глотает её отказ (контейнер
        // уже удалён), метка и UTC-запись пишутся всегда.
        await LogPhaseAsync("teardown", sw);
    }

    // ── Статическая сборка образа (раз на процесс): publish на хосте +
    // docker build CLI-режимом (узкий контекст артефактов; канон
    // E2E-образов). Фазы — с UTC-метками в phases.log окружения и
    // [PHASE]-метками в Console; docker-диагностика к сборочным фазам
    // НЕ применима (контейнера ещё нет) — «тихая фаза» исключена метками
    // и полными логами процессов в ArtifactsDir. ──

    private static async Task BuildImageOnceAsync(string artifactsDir, CancellationToken ct)
    {
        if (_staticReady || Environment.GetEnvironmentVariable("PGW_TEST_E2E_NOBUILD") == "1")
            return;
        await StaticGate.WaitAsync(ct);
        try
        {
            if (_staticReady)
                return;
            var root = FindRepoRoot();
            var outDir = Path.Combine(root, "artifacts", "e2e", "owns3");
            var sw = Stopwatch.StartNew();
            PhaseStatic(artifactsDir, "build owns3:e2e: publish (host) — старт");
            await RunProcessAsync("dotnet",
                ["publish", Path.Combine(root, "src", "OwnS3.App", "OwnS3.App.csproj"),
                 "-c", "Release", "-o", outDir, "--nologo"],
                ct, TimeSpan.FromMinutes(10), env: NoMsBuildReuseEnv,
                logFile: Path.Combine(artifactsDir, "process-build-publish.log"));
            PhaseStatic(artifactsDir,
                $"build owns3:e2e: publish готов за {sw.Elapsed.TotalSeconds:F0} c — docker build…");
            await RunProcessAsync("docker",
                ["build", "-f", Path.Combine(root, "docker", "OwnS3.Dockerfile"),
                 "-t", ServiceImage, outDir],
                ct, TimeSpan.FromSeconds(120),
                logFile: Path.Combine(artifactsDir, "process-build-docker.log"));
            PhaseStatic(artifactsDir,
                $"build owns3:e2e: готов за {sw.Elapsed.TotalSeconds:F0} c (publish+build)");
            _staticReady = true;
        }
        finally
        {
            StaticGate.Release();
        }
    }

    // [PHASE]-метка сборочной фазы: Console + phases.log (UTC) окружения.
    private static void PhaseStatic(string artifactsDir, string message)
    {
        Console.WriteLine($"[PHASE] {message}");
        File.AppendAllText(Path.Combine(artifactsDir, "phases.log"),
            $"{DateTime.UtcNow:O} {message}\n");
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "docker", "OwnS3.Dockerfile")))
            dir = dir.Parent;
        return dir?.FullName
            ?? throw new InvalidOperationException("корень репо (docker/OwnS3.Dockerfile) не найден");
    }

    // ── Процесс-хелперы (канон RunProcessAsync: таймаут убивает дерево,
    //    вывод — в исключение, полный лог — в logFile) ──

    private static async Task<string> RunProcessAsync(
        string file, string[] args, CancellationToken ct, TimeSpan timeout,
        IReadOnlyDictionary<string, string>? env = null, string? logFile = null)
    {
        var psi = new ProcessStartInfo(file, args)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        if (env is not null)
            foreach (var (key, value) in env)
                psi.Environment[key] = value;
        using var process = Process.Start(psi)
            ?? throw new ApplicationException($"не удалось запустить {file}");
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);
        var outTask = process.StandardOutput.ReadToEndAsync();
        var errTask = process.StandardError.ReadToEndAsync();
        try
        {
            await process.WaitForExitAsync(timeoutCts.Token);
            var output = await outTask;
            var error = await errTask;
            if (logFile is not null)
                await File.AppendAllTextAsync(logFile, output + error, ct);
            if (process.ExitCode != 0)
                throw new ApplicationException($"{file} {string.Join(' ', args)} → {process.ExitCode}: {error.Trim()}");
            return output.Trim();
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            try { process.Kill(entireProcessTree: true); }
            catch { /* процесс мог уже выйти */ }
            string drained = "";
            try
            {
                drained = string.Join("", await Task.WhenAll(outTask, errTask).WaitAsync(TimeSpan.FromSeconds(5)));
            }
            catch (TimeoutException)
            {
                // пайп завис — вывод недоступен
            }
            if (logFile is not null)
                await File.AppendAllTextAsync(logFile, drained, CancellationToken.None);
            throw new ApplicationException(
                $"{file} {string.Join(' ', args)} не завершился за {timeout.TotalSeconds:0} c — убит; хвост:\n{drained}");
        }
    }

    private static async Task TryDockerAsync(IReadOnlyList<string> args)
    {
        // static: зовётся и из статического StartAsync (RemoveAttemptAsync);
        // семантика прежняя — чистка лучшим усилием, ошибки глотаются.
        try { await RunProcessAsync("docker", [.. args], CancellationToken.None, TimeSpan.FromMinutes(2)); }
        catch { /* чистка — лучшее усилие */ }
    }

    private async Task<string> RunDockerOrNullAsync(IReadOnlyList<string> args, CancellationToken ct)
    {
        try { return await RunDockerAsync(args, ct); }
        catch { return ""; }
    }

    private async Task ShellToFileAsync(string args, string path)
    {
        try
        {
            var output = await RunDockerAsync(args.Split(' '), CancellationToken.None);
            await File.WriteAllTextAsync(path, output);
        }
        catch
        {
            // телеметрия — лучшее усилие
        }
    }

    private static async Task SafeStopAsync(IContainer container, bool remove)
    {
        try { await container.StopAsync(); }
        catch { /* уже остановлен/не стартовал */ }
        if (remove)
        {
            try { await container.DisposeAsync(); }
            catch { /* уже удалён */ }
        }
    }

    // Зонд свободного порта (динамические порты везде; прецедент FreePort).
    private static int FreePort()
    {
        var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
