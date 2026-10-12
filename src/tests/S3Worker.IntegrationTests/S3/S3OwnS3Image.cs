using System.Diagnostics;

namespace S3Worker.IntegrationTests.S3;

// Статическая сборка образа owns3:e2e-it — раз на процесс тест-хоста (канон
// E2E-образов: publish на хосте, docker build — runtime-COPY артефактов,
// БЕЗ исходников в контексте; порт OwnS3E2eImage, тег per-серия).
internal static class S3OwnS3Image
{
    public const string Tag = "owns3:e2e-it";

    private static readonly SemaphoreSlim Gate = new(1, 1);
    private static bool _ready;

    // Env вложенных dotnet-сборок: build-server/nodeReuse держат stdout-пайп —
    // реюз отключаем (канон E2eFixture).
    private static readonly IReadOnlyDictionary<string, string> NoMsBuildReuseEnv =
        new Dictionary<string, string>
        {
            ["DOTNET_CLI_DO_NOT_USE_MSBUILD_SERVER"] = "1",
            ["MSBUILDDISABLENODEREUSE"] = "1",
        };

    public static async Task BuildOnceAsync(string artifactsDir, CancellationToken ct)
    {
        if (_ready || Environment.GetEnvironmentVariable("PGW_TEST_E2E_NOBUILD") == "1")
            return;
        await Gate.WaitAsync(ct);
        try
        {
            if (_ready)
                return;
            var root = FindRepoRoot();
            var outDir = Path.Combine(root, "artifacts", "e2e", "owns3");
            var sw = Stopwatch.StartNew();
            Phase(artifactsDir, "build owns3:e2e-it: publish (host) — старт");
            await RunAsync("dotnet",
                ["publish", Path.Combine(root, "src", "OwnS3.App", "OwnS3.App.csproj"),
                 "-c", "Release", "-o", outDir, "--nologo"],
                ct, TimeSpan.FromMinutes(10), env: NoMsBuildReuseEnv,
                logFile: Path.Combine(artifactsDir, "process-build-publish.log"));
            Phase(artifactsDir,
                $"build owns3:e2e-it: publish готов за {sw.Elapsed.TotalSeconds:F0} c — docker build…");
            await RunAsync("docker",
                ["build", "-f", Path.Combine(root, "docker", "OwnS3.Dockerfile"),
                 "-t", Tag, outDir],
                ct, TimeSpan.FromSeconds(120),
                logFile: Path.Combine(artifactsDir, "process-build-docker.log"));
            Phase(artifactsDir, $"build owns3:e2e-it: готов за {sw.Elapsed.TotalSeconds:F0} c (publish+build)");
            _ready = true;
        }
        finally
        {
            Gate.Release();
        }
    }

    private static void Phase(string artifactsDir, string message)
    {
        Console.WriteLine($"[PHASE] {message}");
        File.AppendAllText(Path.Combine(artifactsDir, "phases.log"), $"{DateTime.UtcNow:O} {message}\n");
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "docker", "OwnS3.Dockerfile")))
            dir = dir.Parent;
        return dir?.FullName
            ?? throw new InvalidOperationException("корень репо (docker/OwnS3.Dockerfile) не найден");
    }

    // Таймаут убивает дерево процессов; полный вывод — в logFile.
    private static async Task RunAsync(
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
}
