using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>
/// Статический слой E2E-стенда: секреты e2e-установки, хелперы ожиданий и
/// процессов, правило пересборки Release (t09). Рантайм-окружение (docker-сеть,
/// etcd, MinIO, инстансы PgWorker) — ИЗОЛИРОВАННОЕ per-сценарий, владелец
/// жизненного цикла — <see cref="E2eEnvironment"/> (смерть etcd-контейнера
/// уносит и ключи: per-environment etcd снимает проблему чистки ключей).
/// </summary>
public static class E2eFixture
{
    // Секреты e2e-установки (Д7): передаются обоим процессам и SQL-пробам теста.
    public const string SuPassword = "pgw-e2e-su";
    public const string StandbyPassword = "pgw-e2e-standby";
    public const string BucketAdminPassword = "pgw-e2e-admin";
    public const string MoverPassword = "pgw-e2e-mover";

    // Хелпер ожидания условия (полл 500 мс).
    public static async Task<bool> WaitForAsync(
        Func<Task<bool>> condition, TimeSpan timeout, CancellationToken ct = default)
    {
        var deadline = Stopwatch.StartNew();
        while (deadline.Elapsed < timeout)
        {
            if (await condition())
                return true;
            await Task.Delay(500, ct);
        }

        return false;
    }

    public static Task<string> RunDockerAsync(string[] args, CancellationToken ct = default)
        => RunProcessAsync("docker", args, ct);

    internal static async Task<string> RunProcessAsync(string file, string[] args, CancellationToken ct = default)
    {
        var psi = new ProcessStartInfo(file, args)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        using var process = Process.Start(psi)
            ?? throw new ApplicationException($"не удалось запустить {file}");
        var output = await process.StandardOutput.ReadToEndAsync(ct);
        var error = await process.StandardError.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);
        if (process.ExitCode != 0)
            throw new ApplicationException($"{file} {string.Join(' ', args)} → {process.ExitCode}: {error.Trim()}");
        return output.Trim();
    }

    /// <summary>
    /// Правило пересборки Release (t09, spec «фаза Г»): автосборка при каждом
    /// прогоне E2E — устаревший зелёный бинарь маскировал регрессию 29.08–02.09.
    /// PGW_TEST_E2E_NOBUILD=1 — обход для бисекта/отладки конкретного бинаря.
    /// </summary>
    internal static async Task EnsureAppDllAsync(string root, bool noBuild)
    {
        var appDll = Path.Combine(
            root, "src", "PgWorker.App", "bin", "Release", "net10.0", "PgWorker.App.dll");
        if (noBuild)
        {
            if (!File.Exists(appDll))
                throw new ApplicationException(
                    $"нет {appDll} — соберите решение: dotnet build src/PgWorker.slnx -c Release"
                    + " (или снимите PGW_TEST_E2E_NOBUILD для автосборки)");
            return;
        }

        // Инкрементальный msbuild: no-op при актуальном бинаре (секунды).
        // Не RunProcessAsync: ошибки msbuild идут в stdout, а он включает в
        // исключение только stderr — здесь нужен хвост полного вывода в
        // сообщении (spec «фаза Г»), а не молчаливый запуск старого бинаря.
        var psi = new ProcessStartInfo(
            "dotnet", ["build", Path.Combine(root, "src", "PgWorker.slnx"), "-c", "Release"])
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        using var build = Process.Start(psi)
            ?? throw new ApplicationException("не удалось запустить dotnet build");
        var stdoutTask = build.StandardOutput.ReadToEndAsync();
        var stderrTask = build.StandardError.ReadToEndAsync();
        await build.WaitForExitAsync();
        var output = await stdoutTask + await stderrTask;
        if (build.ExitCode != 0)
            throw new ApplicationException(
                $"сборка Release не удалась (exit {build.ExitCode}):\n{OutputTail(output)}");
        if (!File.Exists(appDll))
            throw new ApplicationException(
                $"после автосборки нет {appDll} — проверьте сборку решения\n{OutputTail(output)}");
    }

    // Последние строки вывода — для диагностики в исключении (полный лог сборки
    // в исключение не влезает, ошибки msbuild обычно в конце).
    private static string OutputTail(string text, int lines = 30)
    {
        var all = text.Split(
            ['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return string.Join("\n", all.TakeLast(lines));
    }

    // Свободный хост-порт (зонд): слушаем :0 → отдаём; docker/процесс заберёт
    // его при bind (окно гонки между release и bind ничтожно).
    internal static int FreePort()
    {
        var listener = TcpListener.Create(0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    // Корень репозитория: первый каталог вверх с docker/node/Dockerfile.
    internal static string FindRoot(string start)
    {
        var dir = new DirectoryInfo(start);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "docker", "node", "Dockerfile")))
            dir = dir.Parent;
        return dir?.FullName
            ?? throw new ApplicationException("корень репозитория (docker/node/Dockerfile) не найден");
    }
}

/// <summary>Запущенный инстанс PgWorker.App (имя, процесс, каталог снапшотов).
/// Dispose идемпотентен: сценарий dispose-ит хост сам, а затем teardown
/// окружения может попытаться ещё раз (финальная страховка).</summary>
public sealed class HostInstance(
    string name,
    Process process,
    string snapshotsDir,
    HttpClient healthHttp,
    StreamWriter? logWriter = null) : IAsyncDisposable
{
    private bool _disposed;

    public string Name { get; } = name;

    public Process Process { get; } = process;

    public string SnapshotsDir { get; } = snapshotsDir;

    /// <summary>Мгновенный kill (смерть контроллера, AC3) — lease истекают ≤15 с.</summary>
    public void Kill()
    {
        try
        {
            Process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException)
        {
            // уже завершён
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;
        Kill();
        try
        {
            await Process.WaitForExitAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10), CancellationToken.None);
        }
        catch (TimeoutException)
        {
            // не дождались — Process.Dispose разорвёт дескрипторы
        }
        catch (InvalidOperationException)
        {
            // процесс уже завершился сам (крах/kill ранее) — дескрипторы чистит Dispose
        }

        logWriter?.Dispose();
        Process.Dispose();
        _ = healthHttp; // dispose делает владелец статического клиента
    }
}
