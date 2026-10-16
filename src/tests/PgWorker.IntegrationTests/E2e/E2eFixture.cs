using System.Diagnostics;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>
/// Статический слой E2E-контура: секреты e2e-установки, хелперы ожиданий и
/// процессов, сборка статических ассетов (publish-выводы app/тестов + docker
/// образы pgworker:e2e и pgw-e2e-runner:e2e). Рантайм-окружение (docker-сеть,
/// etcd, ownS3, SUT-воркер-контейнер) — ИЗОЛИРОВАННОЕ per-сценарий, владелец
/// жизненного цикла — <see cref="E2eEnvironment"/> (фаза 1) и
/// <see cref="E2eContour"/> (двухфазный запуск).
/// </summary>
public static class E2eFixture
{
    // Секреты e2e-установки (Д7): передаются воркеру и SQL-пробам теста.
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
        // Дефолтный бюджет docker-CLI (ревью Фазы 7): зависший процесс обязан
        // умирать по таймауту, а не висеть вечно; обычные команды — секунды.
        // Долгие операции (docker build/run раннера) передают бюджет явно.
        => await RunProcessAsync(file, args, ct, timeout: TimeSpan.FromMinutes(2), env: null);

    /// <summary>Env вложенных dotnet-сборок (publish из фикстур): долгоживущие
    /// MSBuild/Roslyn build-server'ы и nodeReuse-ноды переживают процесс сборки и
    /// удерживают stdout-пайп навсегда — ReadToEndAsync не получает EOF (e2e-факт:
    /// wal:e2e publish висел 11 мин, app-dll — 10 мин). Отключаем обе формы
    /// реюза: сборка живёт ровно столько, сколько её процесс.</summary>
    internal static readonly IReadOnlyDictionary<string, string> NoMsBuildReuseEnv = new Dictionary<string, string>
    {
        ["DOTNET_CLI_DO_NOT_USE_MSBUILD_SERVER"] = "1",
        ["MSBUILDDISABLENODEREUSE"] = "1",
    };

    /// <summary>Процесс с бюджетом (t27): docker-CLI изредка виснет (daemon-флэйм,
    /// e2e-факт: teardown замер на docker logs навсегда) — по истечении дерево
    /// процесса убивается, наружу ApplicationException (вызывающий телеметрии
    /// глотает — «лучшими усилиями», канон e2e-launch). logFile — полный вывод
    /// процесса (успех и таймаут — единый журнал build-вызовов, t29 §4.5);
    /// при kill по бюджету хвост вывода (~40 строк) попадает в исключение.</summary>
    internal static async Task<string> RunProcessAsync(
        string file, string[] args, CancellationToken ct, TimeSpan? timeout,
        IReadOnlyDictionary<string, string>? env = null,
        string? logFile = null)
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
                psi.EnvironmentVariables[key] = value;
        using var process = Process.Start(psi)
            ?? throw new ApplicationException($"не удалось запустить {file}");
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (timeout is { } budget)
            timeoutCts.CancelAfter(budget);
        // Оба потока — ПАРАЛЛЕЛЬНО и БЕЗ токена бюджета (t29 §4.5): при kill
        // по бюджету дерево умирает, пайпы закрываются, чтения завершаются
        // сами — накопленный вывод доступен (ReadToEndAsync(ct) при отмене
        // терял вывод: фейл build > 120 c не оставлял следов, на каком шаге
        // зависло). Последовательное чтение при полном stderr и пустом
        // stdout дедлокает пайп до бюджета (docker build пишет в stderr).
        var outTask = process.StandardOutput.ReadToEndAsync();
        var errTask = process.StandardError.ReadToEndAsync();
        try
        {
            await process.WaitForExitAsync(timeoutCts.Token);
            var output = await outTask;
            var error = await errTask;
            await WriteLogAsync(logFile, output + error);
            if (process.ExitCode != 0)
                throw new ApplicationException($"{file} {string.Join(' ', args)} → {process.ExitCode}: {error.Trim()}");
            return output.Trim();
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // Бюджет исчерпан (не остановка host'а): убиваем дерево, вывод — в
            // диагностику: хвост в исключение, полный — в logFile (если задан).
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch
            {
                // процесс мог уже выйти
            }

            var killed = await DrainAsync(outTask, errTask);
            await WriteLogAsync(logFile, killed);
            var tail = OutputTail(killed, lines: 40);
            throw new ApplicationException(
                $"{file} {string.Join(' ', args)} не завершился за {timeout!.Value.TotalSeconds:0} c — убит"
                + (tail.Length == 0 ? "" : $"; хвост вывода:\n{tail}"));
        }
    }

    // Дренаж вывода убитого процесса: после Kill(entireProcessTree) пайпы
    // закрываются операционкой и оба чтения завершаются естественно; страховка
    // от зависшего пайпа — короткий бюджет (диагностика уже убитого процесса,
    // НЕ бюджет выполняемой команды — таймауты команд не меняются).
    private static async Task<string> DrainAsync(Task<string> outTask, Task<string> errTask)
    {
        try
        {
            var drained = await Task.WhenAll(outTask, errTask).WaitAsync(TimeSpan.FromSeconds(5));
            return drained[0] + drained[1];
        }
        catch (TimeoutException)
        {
            return "";
        }
    }

    // Полный вывод процесса в logFile (успех и таймаут — единый журнал
    // build-вызовов); файловая телеметрия не роняет вызов.
    private static async Task WriteLogAsync(string? logFile, string output)
    {
        if (logFile is null)
            return;
        try
        {
            await File.AppendAllTextAsync(logFile, output);
        }
        catch
        {
            // телеметрия — «лучшими усилиями»
        }
    }

    /// <summary>
    /// dotnet publish проекта в каталог артефактов E2E (инкрементально — секунды).
    /// NoMsBuildReuseEnv: build-server/ноды переживают сборку и держат stdout-пайп
    /// навсегда (e2e-факт: slnx-прогон висел 10+ мин). Бюджет 10 мин: полная
    /// пересборка — минуты, тишина дольше бюджета = зависание (fail-fast).
    /// </summary>
    internal static async Task PublishProjectAsync(
        string projectPath, string outDir, string tag, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        StaticPhase($"e2e-publish {tag}: dotnet publish (host, инкрементально)…");
        await RunProcessAsync("dotnet",
        [
            "publish", projectPath, "-c", "Release", "-o", outDir, "--nologo",
        ], ct, timeout: TimeSpan.FromMinutes(10), env: NoMsBuildReuseEnv,
            logFile: $"/tmp/pgw-e2e-static-process-{tag}-publish.log");
        StaticPhase($"e2e-publish {tag}: готов за {sw.Elapsed.TotalSeconds:F0} с");
    }

    /// <summary>
    /// Правило пересборки Release (t09, spec «фаза Г»): автосборка при каждом
    /// прогоне E2E — устаревший зелёный бинарь маскировал регрессию.
    /// PGW_TEST_E2E_NOBUILD=1 — обход для бисекта/отладки конкретного бинаря:
    /// publish-шаг пропускается, docker-образы строятся из существующих каталогов
    /// артефактов (пустые — fail-fast с подсказкой).
    /// </summary>
    internal static void EnsurePublishedDir(string dir, string hint)
    {
        if (!Directory.Exists(dir) || Directory.EnumerateFiles(dir, "*.dll").Any() is false)
            throw new ApplicationException(
                $"нет publish-артефактов в {dir} — соберите их (dotnet publish) и повторите"
                + $" (или снимите PGW_TEST_E2E_NOBUILD для автосборки). {hint}");
    }

    // [PHASE]-метка статических фаз подготовки: Console (в лог факта) + ФАЙЛ
    // /tmp/pgw-e2e-static-phase.log — xUnit буферизует Console до конца факта,
    // файл виден наблюдателю во время «тихих» фаз (канон e2e-launch).
    public static void StaticPhase(string message)
    {
        var line = $"{DateTime.UtcNow:HH:mm:ss} [PHASE] e2e-static: {message}";
        Console.WriteLine(line);
        try
        {
            File.AppendAllText("/tmp/pgw-e2e-static-phase.log", line + Environment.NewLine);
        }
        catch
        {
            // файловая телеметрия — «лучшими усилиями», не роняет подготовку
        }
    }

    /// <summary>Таймер для [PHASE]-меток статических фаз (тайминг в итоговой строке).</summary>
    public static System.Diagnostics.Stopwatch StaticSw()
        => System.Diagnostics.Stopwatch.StartNew();

    // Последние строки вывода — для диагностики в исключении (полный лог сборки
    // в исключение не влезает, ошибки msbuild обычно в конце).
    internal static string OutputTail(string text, int lines = 30)
    {
        var all = text.Split(
            ['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return string.Join("\n", all.TakeLast(lines));
    }

    // Свободный хост-порт (зонд): слушаем :0 → отдаём. Тестовой инфраструктуре
    // E2E-контура не нужен (публикаций на хост нет) — остаётся для не-E2E
    // тестов (OwnS3-хелпер Backups/*).
    internal static int FreePort()
    {
        var listener = System.Net.Sockets.TcpListener.Create(0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
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

/// <summary>Запущенный SUT-воркер — КОНТЕЙНЕР в сети контура (Вариант A,
/// docs/e2e-isolation.md §3). Имя класса сохранено (HostInstance) — сценарии
/// не переписываются на новый тип. Dispose идемпотентен: сценарий dispose-ит
/// воркер сам, teardown окружения может попытаться ещё раз (страховка).</summary>
public sealed class HostInstance : IAsyncDisposable
{
    private readonly E2eEnvironment _env;
    private bool _disposed;

    public HostInstance(E2eEnvironment env, string name, string alias, string containerName)
    {
        _env = env;
        Name = name;
        Alias = alias;
        ContainerName = containerName;
    }

    public string Name { get; }

    /// <summary>DNS-алиас воркера в сети контура (mTLS SAN, адрес API).</summary>
    public string Alias { get; }

    public string ContainerName { get; }

    /// <summary>mTLS-адрес HTTP API воркера (внутрисетевой, раннер в той же сети).</summary>
    public string ApiUrl => $"https://{Alias}:8443";

    /// <summary>Каталог etcd-снапшотов воркера (Snapshots__Dir в контейнере
    /// /snapshots — bind на каталог артефактов хоста; в раннере виден как
    /// /snaps/&lt;name&gt;): сценарии читают snapshot-*.db напрямую.</summary>
    public string SnapshotsDir => $"/snaps/{Name}";

    /// <summary>Жив ли воркер (контейнер running) — семантика «процесс жив»
    /// старого хост-инстанса (смерть контроллера, cert-сценарии).</summary>
    public async Task<bool> IsRunningAsync(CancellationToken ct = default)
    {
        var ps = await E2eFixture.RunDockerAsync(
        [
            "ps", "--filter", $"name=^{ContainerName}$", "--format", "{{.State}}",
        ], ct);
        return ps.Contains("running", StringComparison.Ordinal);
    }

    /// <summary>Хвост логов воркера (docker logs) — семантика host.log старого
    /// хост-инстанса (диагностика сценариев «что произошло» на живом воркере).</summary>
    public async Task<string> ReadLogTailAsync(int lines = 100, CancellationToken ct = default)
        => await E2eFixture.RunDockerAsync(["logs", "--tail", lines.ToString(), ContainerName], ct);

    /// <summary>Состояние контейнера воркера (docker inspect): Status и ExitCode
    /// (graceful self-stop сценария — exit 0; null — контейнер жив).</summary>
    public async Task<(string Status, int? ExitCode)> InspectStateAsync(CancellationToken ct = default)
    {
        var raw = await E2eFixture.RunDockerAsync(
        [
            "inspect", "--format", "{{.State.Status}}:{{.State.ExitCode}}", ContainerName,
        ], ct);
        var parts = raw.Split(':', 2);
        return parts.Length == 2 && int.TryParse(parts[1], out var code)
            ? (parts[0], code)
            : (raw, null);
    }

    /// <summary>Мгновенный kill (смерть контроллера, AC3) — lease истекают ≤15 с.</summary>
    public void Kill()
    {
        try
        {
            E2eFixture.RunDockerAsync(["kill", ContainerName]).GetAwaiter().GetResult();
        }
        catch (ApplicationException)
        {
            // уже завершён/удалён
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;
        try
        {
            await E2eFixture.RunDockerAsync(["rm", "-f", ContainerName]);
        }
        catch (ApplicationException e) when (e.Message.Contains("No such container", StringComparison.OrdinalIgnoreCase))
        {
            // уже удалён (teardown окружения)
        }
        catch (ApplicationException e) when (e.Message.Contains("already in progress", StringComparison.OrdinalIgnoreCase))
        {
            // удаляется демоном — исход тот же
        }
    }
}
