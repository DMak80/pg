using System.Collections.Concurrent;
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
    // Генерируются в рантайме (криптостойкий RNG) — секреты не зашиваются в
    // исходник; инициализируются один раз на процесс и переиспользуются всеми
    // сценариями контура.
    public static readonly string SuPassword = GenerateSecret("su");
    public static readonly string StandbyPassword = GenerateSecret("standby");
    public static readonly string BucketAdminPassword = GenerateSecret("admin");
    public static readonly string MoverPassword = GenerateSecret("mover");

    private static string GenerateSecret(string role)
    {
        // Буквы/цифры (без спецсимволов — безопасно для DSN/env/YAML-квотинга).
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var bytes = System.Security.Cryptography.RandomNumberGenerator.GetBytes(24);
        return $"pgw-e2e-{role}-"
            + new string(bytes.Select(b => alphabet[b % alphabet.Length]).ToArray());
    }

    // ===== [TIMING]: авто-хронометраж ожиданий ВСЕХ серий =====
    // Каждое WaitForAsync пишет метку (условие/длительность/исход), ручные
    // шаги — через TimedAsync. stderr testhost буферизует — метки копятся в
    // очередь и докачиваются в timing.log артефактов факта при телеметрии
    // (CollectDiagnosticsAsync); для живых хост-прогонов дублируются в stderr.
    private static readonly ConcurrentQueue<string> TimingQueue = new();

    internal static void Timing(string line)
    {
        Console.Error.WriteLine(line);
        TimingQueue.Enqueue(line);
    }

    /// <summary>Забрать накопленные метки (докачка в timing.log телеметрии).</summary>
    internal static IReadOnlyList<string> DrainTiming()
    {
        var drained = new List<string>();
        while (TimingQueue.TryDequeue(out var line))
            drained.Add(line);
        return drained;
    }

    /// <summary>[TIMING]-хронометраж явного шага (ручная разметка тестов).</summary>
    public static async Task TimedAsync(string step, Func<Task> action)
    {
        var sw = Stopwatch.StartNew();
        await action();
        Timing($"{DateTime.UtcNow:HH:mm:ss'Z'} [TIMING] {step}: {sw.Elapsed.TotalSeconds:F1}s");
    }

    public static async Task<T> TimedAsync<T>(string step, Func<Task<T>> action)
    {
        var sw = Stopwatch.StartNew();
        var result = await action();
        Timing($"{DateTime.UtcNow:HH:mm:ss'Z'} [TIMING] {step}: {sw.Elapsed.TotalSeconds:F1}s");
        return result;
    }

    // Хелпер ожидания условия (полл 500 мс). Авто-[TIMING]: выражение условия
    // (CallerArgumentExpression) + длительность + исход — все серии получают
    // хронометраж ожиданий без правки вызовов.
    public static async Task<bool> WaitForAsync(
        Func<Task<bool>> condition, TimeSpan timeout, CancellationToken ct = default,
        [System.Runtime.CompilerServices.CallerArgumentExpression(nameof(condition))] string? what = null)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            while (sw.Elapsed < timeout)
            {
                if (await condition())
                    return true;
                await Task.Delay(500, ct);
            }

            return false;
        }
        finally
        {
            var head = (what ?? "wait").Replace("\r", "").Replace("\n", " ").Trim();
            if (head.Length > 90)
                head = head[..90] + "…";
            Timing($"{DateTime.UtcNow:HH:mm:ss'Z'} [TIMING] wait({head}): {sw.Elapsed.TotalSeconds:F1}s");
        }
    }

    /// <summary>Ожидание завершения провижининга кластера с fail-fast
    /// «застрял»: config без state + dsn + ноды RUNNING (+ buckets/status пуст,
    /// если передан range). Если хоть одна нода RUNNING дольше 60 с, а
    /// config.state не снят — воркер не завершает заявку (ретраи): телеметрия
    /// + ранний фейл вместо молчаливого выживания бюджета (прогон 2026-10-16
    /// AC3: 310 с ожидания при живом лидере патрони). Строгая семантика серий:
    /// shards = ["shard1","shard2"] по умолчанию; RestTls-подобные — ["shard1"].</summary>
    public static async Task<bool> WaitProvisionedAsync(
        E2eEnvironment fx,
        Func<string, Task<Shared.Etcd.Client.Kv?>> getOrNull,
        string cluster, CancellationToken ct,
        string[]? shards = null,
        Func<string, Task<System.Collections.Generic.IReadOnlyList<Shared.Etcd.Client.Kv>>>? range = null,
        TimeSpan? budget = null)
    {
        shards ??= ["shard1", "shard2"];
        var runningSince = (DateTimeOffset?)null;
        return await WaitForAsync(async () =>
        {
            var config = await getOrNull($"/clusters/{cluster}/config");
            if (config is null
                || System.Text.Json.JsonSerializer
                    .Deserialize<Dictionary<string, System.Text.Json.JsonElement>>(config.Value)!
                    .ContainsKey("state"))
            {
                var anyRunning = false;
                foreach (var shard in shards)
                {
                    foreach (var suffix in new[] { "a", "b" })
                    {
                        var st = await getOrNull(
                            $"/clusters/{cluster}/shards/{shard}/nodes/{shard}{suffix}/state");
                        if (st is { Value: "RUNNING" })
                            anyRunning = true;
                    }
                }

                if (anyRunning)
                {
                    runningSince ??= DateTimeOffset.UtcNow;
                    if (DateTimeOffset.UtcNow - runningSince > TimeSpan.FromSeconds(60))
                    {
                        await fx.CollectDiagnosticsAsync("provision-stuck-fail-fast");
                        throw new ApplicationException(
                            $"provisioning is stuck: nodes {cluster} ({string.Join(',', shards)}) RUNNING "
                            + ">60 s, config.state not lifted (the worker does not complete the request — see "
                            + "container-pgw-ew-*.log in the fail-fast telemetry)");
                    }
                }
                else
                {
                    runningSince = null;
                }

                return false;
            }

            foreach (var shard in shards)
            {
                if (await getOrNull($"/clusters/{cluster}/shards/{shard}/dsn") is null)
                    return false;
                foreach (var suffix in new[] { "a", "b" })
                {
                    var ready = await getOrNull(
                        $"/clusters/{cluster}/shards/{shard}/nodes/{shard}{suffix}/state");
                    if (ready?.Value != "RUNNING")
                        return false;
                }
            }

            return range is null || (await range($"/clusters/{cluster}/buckets/status/")).Count == 0;
        }, budget ?? TimeSpan.FromSeconds(360), ct);
    }

    /// <summary>Fail-fast-декоратор условия провижининга для перевода серий:
    /// оборачивает ЛОКАЛЬНОЕ строгое условие серии (ProvisionedAsync) — если
    /// оно не выполнено, а ноды RUNNING дольше 60 с при неснятом config.state
    /// (воркер не завершает заявку, ретраи) — телеметрия + ранний фейл вместо
    /// молчаливого выживания бюджета (прогон 2026-10-16 AC3: 310 с при живом
    /// лидере). Пример: E2ePhase.WaitAsync(Fx, "…-provisioning",
    ///   E2eFixture.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster,
    ///      () => ProvisionedAsync(cluster)), …).</summary>
    public static Func<Task<bool>> ProvisionStuckFailFast(
        E2eEnvironment fx,
        Func<string, Task<Shared.Etcd.Client.Kv?>> getOrNull,
        string cluster,
        Func<Task<bool>> baseCondition,
        string[]? shards = null)
    {
        shards ??= ["shard1", "shard2"];
        var runningSince = (DateTimeOffset?)null;
        return async () =>
        {
            if (await baseCondition())
                return true;

            var config = await getOrNull($"/clusters/{cluster}/config");
            if (config is null
                || !System.Text.Json.JsonSerializer
                    .Deserialize<Dictionary<string, System.Text.Json.JsonElement>>(config.Value)!
                    .ContainsKey("state"))
            {
                runningSince = null;
                return false;
            }

            var anyRunning = false;
            foreach (var shard in shards)
            foreach (var suffix in new[] { "a", "b" })
            {
                if ((await getOrNull(
                        $"/clusters/{cluster}/shards/{shard}/nodes/{shard}{suffix}/state"))
                    is { Value: "RUNNING" })
                    anyRunning = true;
            }

            if (!anyRunning)
            {
                runningSince = null;
                return false;
            }

            runningSince ??= DateTimeOffset.UtcNow;
            if (DateTimeOffset.UtcNow - runningSince > TimeSpan.FromSeconds(60))
            {
                await fx.CollectDiagnosticsAsync("provision-stuck-fail-fast");
                throw new ApplicationException(
                    $"provisioning is stuck: nodes {cluster} ({string.Join(',', shards)}) RUNNING "
                    + ">60 s, config.state not lifted (the worker does not complete the request — see "
                    + "container-pgw-ew-*.log in the fail-fast telemetry)");
            }

            return false;
        };
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
    /// удерживают stdout-пайп навсегда — ReadToEndAsync не получает EOF (факт стенда:
    /// wal:e2e publish висел 11 мин, app-dll — 10 мин). Отключаем обе формы
    /// реюза: сборка живёт ровно столько, сколько её процесс.</summary>
    internal static readonly IReadOnlyDictionary<string, string> NoMsBuildReuseEnv = new Dictionary<string, string>
    {
        ["DOTNET_CLI_DO_NOT_USE_MSBUILD_SERVER"] = "1",
        ["MSBUILDDISABLENODEREUSE"] = "1",
    };

    /// <summary>Процесс с бюджетом : docker-CLI изредка виснет (daemon-флэйм,
    /// факт стенда: teardown замер на docker logs навсегда) — по истечении дерево
    /// процесса убивается, наружу ApplicationException (вызывающий телеметрии
    /// глотает — «лучшими усилиями», канон e2e-launch). logFile — полный вывод
    /// процесса (успех и таймаут — единый журнал build-вызовов, §4.5);
    /// при kill по бюджету хвост вывода (~40 строк) попадает в исключение.</summary>
    private static async Task<string> RunProcessCoreAsync(
        string file, string[] args, CancellationToken ct, TimeSpan? timeout,
        IReadOnlyDictionary<string, string>? env = null,
        string? logFile = null, bool mergeStderrOnSuccess = false)
    {
        var psi = new ProcessStartInfo(file)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        foreach (var arg in args)
            psi.ArgumentList.Add(arg);
        if (env is not null)
            foreach (var (key, value) in env)
                psi.EnvironmentVariables[key] = value;
        using var process = new Process { StartInfo = psi };
        process.Start();
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (timeout is { } budget)
            timeoutCts.CancelAfter(budget);
        // Оба потока — ПАРАЛЛЕЛЬНО и БЕЗ токена бюджета (§4.5): при kill
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
            return mergeStderrOnSuccess ? MergeOutput(output, error) : output.Trim();
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
                $"{file} {string.Join(' ', args)} did not finish within {timeout!.Value.TotalSeconds:0} s — killed"
                + (tail.Length == 0 ? "" : $"; output tail:\n{tail}"));
        }
    }

    /// <summary>Процесс с бюджетом : docker-CLI изредка виснет — по
    /// истечении дерево процесса убивается, наружу ApplicationException.
    /// Возврат при exit=0 — только stdout (контракт вызовов телеметрии);
    /// объединённый съём — RunProcessMergedAsync.</summary>
    internal static Task<string> RunProcessAsync(
        string file, string[] args, CancellationToken ct, TimeSpan? timeout,
        IReadOnlyDictionary<string, string>? env = null,
        string? logFile = null)
        => RunProcessCoreAsync(file, args, ct, timeout, env, logFile);

    /// <summary>Объединение потоков (канон docs/e2e-launch.md §1):
    /// stdout-блок, затем stderr-блок; между двумя непустыми блоками ровно
    /// один перевод строки, края тримятся. docker-CLI разводит потоки
    /// контейнера по СВОИМ потокам (docker logs: stdout контейнера → stdout
    /// CLI, stderr → stderr CLI) — stdout-only теряет диагностику агентов
    /// (WalReceiver пишет open_failed/stream_broken в stderr). Интерливинг
    /// по времени не восстанавливается — порядок внутри потока сохранён.</summary>
    private static string MergeOutput(string output, string error)
    {
        var o = output.Trim();
        var e = error.Trim();
        return e.Length == 0 ? o : o.Length == 0 ? e : o + "\n" + e;
    }

    /// <summary>Процесс с объединённым возвратом (stdout+stderr) при exit=0;
    /// исключения и таймаут-семантика — идентичны RunProcessAsync. Отдельная
    /// точка входа для телеметрии — контракт RunProcessAsync не меняется.</summary>
    internal static Task<string> RunProcessMergedAsync(
        string file, string[] args, CancellationToken ct, TimeSpan? timeout,
        IReadOnlyDictionary<string, string>? env = null,
        string? logFile = null)
        => RunProcessCoreAsync(file, args, ct, timeout, env, logFile,
            mergeStderrOnSuccess: true);

    /// <summary>docker logs — всегда объединённо (stdout+stderr контейнера):
    /// ["logs", .. logsArgs] — подкоманду logs подставляет сам хелпер.
    /// Бюджет по умолчанию 60 с — как у logs-вызовов телеметрии;
    /// переопределяется параметром timeout.</summary>
    internal static Task<string> RunDockerLogsAsync(
        IReadOnlyList<string> logsArgs, CancellationToken ct = default,
        TimeSpan? timeout = null)
        => RunProcessMergedAsync("docker", ["logs", .. logsArgs], ct,
            timeout ?? TimeSpan.FromSeconds(60));

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
    /// навсегда (факт стенда: slnx-прогон висел 10+ мин). Бюджет 10 мин: полная
    /// пересборка — минуты, тишина дольше бюджета = зависание (fail-fast).
    /// </summary>
    internal static async Task PublishProjectAsync(
        string projectPath, string outDir, string tag, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        StaticPhase($"e2e-publish {tag}: dotnet publish (host, incrementally)…");
        await RunProcessAsync("dotnet",
        [
            "publish", projectPath, "-c", "Release", "-o", outDir, "--nologo",
        ], ct, timeout: TimeSpan.FromMinutes(10), env: NoMsBuildReuseEnv,
            logFile: $"/tmp/pgw-e2e-static-process-{tag}-publish.log");
        StaticPhase($"e2e-publish {tag}: ready in {sw.Elapsed.TotalSeconds:F0} s");
    }

    /// <summary>
    /// Правило пересборки Release (spec «фаза Г»): автосборка при каждом
    /// прогоне E2E — устаревший зелёный бинарь маскировал регрессию.
    /// PGW_TEST_E2E_NOBUILD=1 — обход для бисекта/отладки конкретного бинаря:
    /// publish-шаг пропускается, docker-образы строятся из существующих каталогов
    /// артефактов (пустые — fail-fast с подсказкой).
    /// </summary>
    internal static void EnsurePublishedDir(string dir, string hint)
    {
        if (!Directory.Exists(dir) || Directory.EnumerateFiles(dir, "*.dll").Any() is false)
            throw new ApplicationException(
                $"no publish artifacts in {dir} — build them (dotnet publish) and retry"
                + $" (or unset PGW_TEST_E2E_NOBUILD for an auto-build). {hint}");
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
            ?? throw new ApplicationException("the repository root (docker/node/Dockerfile) was not found");
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
        => await E2eFixture.RunDockerLogsAsync(["--tail", lines.ToString(), ContainerName], ct);

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
        // Журнал воркера — в телеметрию ДО удаления: тест держит воркер в
        // await using и удаляет раньше CollectDiagnostics teardown'а факта —
        // после rm логи контейнера недоступны (прогон 2026-10-16:
        // container-pgw-ew-*.log отсутствовал в артефактах при живых прочих).
        try
        {
            var logs = await E2eFixture.RunDockerLogsAsync(
                ["--timestamps", ContainerName], CancellationToken.None);
            await System.IO.File.WriteAllTextAsync(
                System.IO.Path.Combine(_env.ArtifactsDir, $"container-{ContainerName}.log"),
                logs);
        }
        catch
        {
            // телеметрия лучшими усилиями — не мешает teardown
        }
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
