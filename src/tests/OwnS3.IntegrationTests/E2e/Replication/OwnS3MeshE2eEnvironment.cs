using System.Diagnostics;
using System.Net.Sockets;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using FluentAssertions;
using Xunit;

namespace OwnS3.IntegrationTests.E2e.Replication;

/// <summary>
/// Mesh-контур docker-E2E ownS3 (t35, глава 07; план Task 16): сеть
/// os3m-en-{runId}, N (2–5) контейнеров os3m-n{i}-{runId} (образ owns3:e2e),
/// тома os3m-v{i}-{runId}, динамические хост-порты (зонд FreePort),
/// OWNS3_HOST_ID=os3m-{i}-{runTag}. Ключи: root + replication-ключ (единый
/// peer-комплект контура) + writer. targets через env: полный mesh (каждый
/// знает всех, endpoint = внутреннее имя в сети) либо пусто (fullMeshTargets:
/// false — состав задаёт сценарий через admin-грань). Телеметрия — каноны
/// docs/e2e-launch.md: [PHASE]/phases.log, docker logs/inspect ДО удалений,
/// MarkFailed → остановить без удаления (README-cleanup.txt), teardown при
/// любом исходе + ассерт чистоты по runId. Retry старта ×3. Wait /healthz ≤30 с.
/// </summary>
public sealed class OwnS3MeshE2eEnvironment : IAsyncDisposable
{
    private const string ServiceImage = "owns3:e2e";

    // Литералы e2e-секретов mesh-контура (одноразовый контур; роли — клиентские).
    public const string RootUser = "os3m-root";
    public const string RootPassword = "os3m-root-secret";
    public const string WriterAccessKey = "os3m-writer";
    public const string WriterSecretKey = "os3m-writer-secret";

    // Peer-ключи ПЕР-ПАРА узлов (глава 07 §2/§6): K_pair(i,j) уникален для пары,
    // ОБЕ стороны его регистрируют (policy=replication) и им подписывают запросы
    // друг к другу → резолв sourceName по accessKey в targets работает в обе
    // стороны, а дедуп (sourceName, seqNo) различает всех отправителей (ревью
    // t35, finding 5: общий на весь mesh ключ ломал резолв источников).
    public const string PeerKeyPrefix = "os3m-peer";

    /// <summary>Peer-ключ пары узлов (a, b) — симметричен: pair(a,b)=pair(b,a).</summary>
    public static string PeerPairAccessKeyOf(int a, int b)
    {
        var lo = Math.Min(a, b);
        var hi = Math.Max(a, b);
        return $"{PeerKeyPrefix}-{lo}-{hi}";
    }

    public static string PeerPairSecretKeyOf(int a, int b)
    {
        var lo = Math.Min(a, b);
        var hi = Math.Max(a, b);
        return $"{PeerKeyPrefix}-{lo}-{hi}-secret";
    }

    private readonly INetwork _net;
    private readonly List<IContainer> _containers = [];
    private readonly List<string> _volumeNames = [];
    private readonly List<int> _hostPorts = [];
    private readonly string _runId;
    private readonly string _artifactsDir;
    private bool _failed;

    private OwnS3MeshE2eEnvironment(string slug, string runId, INetwork net,
        List<IContainer> containers, List<string> volumeNames, List<int> hostPorts,
        string artifactsDir)
    {
        Slug = slug;
        _runId = runId;
        _net = net;
        _containers = containers;
        _volumeNames = volumeNames;
        _hostPorts = hostPorts;
        _artifactsDir = artifactsDir;
        Nodes = containers.Count;
    }

    public string Slug { get; }
    public int Nodes { get; }
    public string ArtifactsDir => _artifactsDir;
    public string NetName => $"os3m-en-{_runId}";
    public string RunId => _runId;
    public string RunTag => _runId[..8];

    public void MarkFailed() => _failed = true;

    /// <summary>Docker-имя узла i (os3m-n{i}-{runId}).</summary>
    public string NodeName(int i) => $"os3m-n{i}-{_runId}";

    /// <summary>Грань узла i с хоста (SDK/HttpClient): динамический порт.</summary>
    public string NodeHostUrl(int i) => $"http://localhost:{_hostPorts[i]}";

    /// <summary>Грань узла i изнутри docker-сети (endpoint в targets соседей).</summary>
    public string NodeInternalUrl(int i) => $"http://{NodeName(i)}:9000";

    /// <summary>Подъём mesh-контура: сборка образа (общий сборщик) → сеть → N узлов
    /// (том + динамический порт + wait /healthz ≤30 с). fullMeshTargets — env-seed
    /// полного mesh (каждый знает всех); false — соседей нет (состав через
    /// PUT /admin/replication/targets сценарием). Retry старта ×3 (полное удаление попытки).</summary>
    public static async Task<OwnS3MeshE2eEnvironment> StartAsync(string slug, int nodes,
        bool fullMeshTargets = true, Func<int, ContainerBuilder, ContainerBuilder>? configureNode = null,
        CancellationToken ct = default)
    {
        if (Environment.GetEnvironmentVariable(DockerTrait.EnvVar) != "1")
            throw new InvalidOperationException(
                $"E2E требует {DockerTrait.EnvVar}=1 (иначе Skip в тесте DockerTrait'ом)");
        if (nodes is < 2 or > 5)
            throw new ArgumentOutOfRangeException(nameof(nodes), "mesh 2–5 серверов (глава 07)");

        var runId = Guid.NewGuid().ToString("N");
        var artifactsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-artifacts-{runId}");
        Directory.CreateDirectory(artifactsDir);
        await OwnS3E2eImage.BuildOnceAsync(artifactsDir, ct);

        for (var attempt = 1; ; attempt++)
        {
            var net = new NetworkBuilder().WithName($"os3m-en-{runId}").Build();
            var containers = new List<IContainer>();
            var volumeNames = new List<string>();
            var hostPorts = new List<int>();
            try
            {
                var sw = Stopwatch.StartNew();
                await net.CreateAsync(ct);
                for (var i = 0; i < nodes; i++)
                {
                    var volumeName = $"os3m-v{i}-{runId}";
                    var containerName = NodeNameStatic(runId, i);
                    var hostPort = FreePort();
                    var builder = new ContainerBuilder(ServiceImage)
                        .WithName(containerName)
                        .WithNetwork(net)
                        .WithPortBinding(hostPort, 9000)
                        .WithVolumeMount(volumeName, "/data")
                        .WithEnvironment("OWNS3_ROOT_USER", RootUser)
                        .WithEnvironment("OWNS3_ROOT_PASSWORD", RootPassword)
                        .WithEnvironment("OWNS3_ACCESS_KEYS__0__ACCESSKEY", WriterAccessKey)
                        .WithEnvironment("OWNS3_ACCESS_KEYS__0__SECRETKEY", WriterSecretKey)
                        .WithEnvironment("OWNS3_ACCESS_KEYS__0__POLICY", "read-write");
                    // Peer-ключи ПЕР-ПАРА: узел i регистрирует pair(i,j) для каждого
                    // соседа j (проверка подписей входящих) и кладёт ТОТ ЖЕ ключ в
                    // targets записи j (исходящая подпись к j) — глава 07 §2/§6.
                    // Слоты БЕЗ пропусков (env-трансляция OwnS3 обрывает на первом
                    // пропущенном индексе — канон 05 §2), pair(i,i) — пропускается.
                    var keySlot = 1;
                    for (var j = 0; j < nodes; j++)
                    {
                        if (j == i)
                            continue;
                        builder = builder
                            .WithEnvironment($"OWNS3_ACCESS_KEYS__{keySlot}__ACCESSKEY", PeerPairAccessKeyOf(i, j))
                            .WithEnvironment($"OWNS3_ACCESS_KEYS__{keySlot}__SECRETKEY", PeerPairSecretKeyOf(i, j))
                            .WithEnvironment($"OWNS3_ACCESS_KEYS__{keySlot}__POLICY", "replication");
                        keySlot++;
                    }
                    builder = builder
                        .WithEnvironment("OWNS3_HOST_ID", $"os3m-{i}-{runId[..8]}");
                    if (fullMeshTargets)
                    {
                        // Полный mesh: каждый знает всех (env-seed первого старта — глава 07 §2);
                        // accessKey записи соседа j — pair(i,j) (обе стороны его зарегистрировали)
                        var index = 0;
                        for (var j = 0; j < nodes; j++)
                        {
                            if (j == i)
                                continue;
                            builder = builder
                                .WithEnvironment($"OWNS3_REPLICATION_TARGETS__{index}__NAME", NodeNameStatic(runId, j))
                                .WithEnvironment($"OWNS3_REPLICATION_TARGETS__{index}__ENDPOINT",
                                    $"http://{NodeNameStatic(runId, j)}:9000")
                                .WithEnvironment($"OWNS3_REPLICATION_TARGETS__{index}__ACCESSKEY", PeerPairAccessKeyOf(i, j))
                                .WithEnvironment($"OWNS3_REPLICATION_TARGETS__{index}__SECRETKEY", PeerPairSecretKeyOf(i, j));
                            index++;
                        }
                    }
                    // ContainerBuilder иммутабелен: перехватываем возвращённый builder
                    builder = configureNode?.Invoke(i, builder) ?? builder;
                    var container = builder
                        .WithWaitStrategy(Wait.ForUnixContainer()
                            .UntilHttpRequestIsSucceeded(
                                request => request.ForPort(9000).ForPath("/healthz"),
                                wait => wait.WithTimeout(TimeSpan.FromSeconds(30))))
                        .Build();
                    await container.StartAsync(ct);
                    containers.Add(container);
                    volumeNames.Add(volumeName);
                    hostPorts.Add(hostPort);
                }
                var env = new OwnS3MeshE2eEnvironment(slug, runId, net, containers, volumeNames,
                    hostPorts, artifactsDir);
                Console.WriteLine($"[PHASE] start: ok, {sw.Elapsed.TotalSeconds:F1}s ({nodes} узлов)");
                await File.AppendAllTextAsync(Path.Combine(artifactsDir, "phases.log"),
                    $"{DateTime.UtcNow:O} phase start: ok, {sw.Elapsed.TotalSeconds:F1}s ({nodes} узлов)\n", ct);
                await File.WriteAllTextAsync(Path.Combine(artifactsDir, "README-cleanup.txt"),
                    "# Зачистка окружения прогона (own-only, по runId):\n"
                    + string.Join('\n', volumeNames.Select(v => $"docker volume rm -f {v}")) + "\n"
                    + $"docker network rm {env.NetName}\n", ct);
                return env;
            }
            catch (Exception ex) when (attempt < 3
                && (ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase)
                    || ex.Message.Contains("bind", StringComparison.OrdinalIgnoreCase)
                    || ex.Message.Contains("port", StringComparison.OrdinalIgnoreCase)))
            {
                Console.WriteLine($"[PHASE] start: попытка {attempt} не удалась ({ex.Message.Split('\n')[0]}) — ретрай с чистого листа");
                await RemoveAttemptAsync(net, containers, volumeNames);
                continue;
            }
            catch
            {
                await RemoveAttemptAsync(net, containers, volumeNames);
                throw;
            }
        }
    }

    /// <summary>Сценарий под политикой телеметрии (docs/e2e-launch.md §3):
    /// исключение тела → телеметрия "failed" ДО остановки → MarkFailed → rethrow;
    /// teardown — DisposeAsync при любом исходе.</summary>
    public static async Task RunScenarioAsync(string slug, int nodes,
        Func<OwnS3MeshE2eEnvironment, Task> body, bool fullMeshTargets = true,
        Func<int, ContainerBuilder, ContainerBuilder>? configureNode = null, CancellationToken ct = default)
    {
        var fx = await StartAsync(slug, nodes, fullMeshTargets, configureNode, ct);
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

    /// <summary>Сетевая изоляция узла (A2): docker network disconnect.</summary>
    public async Task DisconnectNodeAsync(int i, CancellationToken ct)
    {
        await RunDockerAsync(["network", "disconnect", NetName, NodeName(i)], ct);
    }

    /// <summary>Возврат узла в сеть (срастание).</summary>
    public async Task ReconnectNodeAsync(int i, CancellationToken ct)
    {
        await RunDockerAsync(["network", "connect", NetName, NodeName(i)], ct);
    }

    /// <summary>Раскол ПОПОЛАМ (A4/A7, честные половины): узлы {0,1} остаются в
    /// основной сети, {2,3} переводятся во вторую сеть os3m-split-{runId} —
    /// половины теряют связь друг с другом (TIMEOUT-природа), внутри половин
    /// связь полная, host-порты обеих половин живы (клиенты с хоста пишут в обе).</summary>
    private INetwork? _splitNet;

    /// <summary>Имя сети раскола (создаётся SplitIntoHalvesAsync).</summary>
    public string SplitNetworkName => $"os3m-split-{_runId}";

    public async Task SplitIntoHalvesAsync(CancellationToken ct)
    {
        _splitNet = new NetworkBuilder().WithName($"os3m-split-{_runId}").Build();
        await _splitNet.CreateAsync(ct);
        for (var i = 2; i < Nodes; i++)
        {
            await RunDockerAsync(["network", "disconnect", NetName, NodeName(i)], ct);
            await RunDockerAsync(["network", "connect", _splitNet.Name, NodeName(i)], ct);
        }
    }

    /// <summary>Подключить узел к сети раскола (изолят сохраняет host-доступность).</summary>
    public async Task AttachNodeToSplitNetworkAsync(int i, CancellationToken ct)
    {
        if (_splitNet is null)
        {
            _splitNet = new NetworkBuilder().WithName($"os3m-split-{_runId}").Build();
            await _splitNet.CreateAsync(ct);
        }
        await RunDockerAsync(["network", "connect", _splitNet.Name, NodeName(i)], ct);
    }

    /// <summary>Отключить узел от сети раскола.</summary>
    public async Task DetachNodeFromSplitNetworkAsync(int i, CancellationToken ct)
    {
        if (_splitNet is null)
            return;
        await RunDockerAsync(["network", "disconnect", _splitNet.Name, NodeName(i)], ct);
    }

    /// <summary>Отключить ВСЕ узлы от сети раскола (finaл heal).</summary>
    public async Task DetachAllFromSplitNetworkAsync(CancellationToken ct)
    {
        if (_splitNet is null)
            return;
        for (var i = 0; i < Nodes; i++)
        {
            try
            {
                await RunDockerAsync(["network", "disconnect", _splitNet.Name, NodeName(i)], ct);
            }
            catch (ApplicationException)
            {
                // узел не в split-сети — норма
            }
        }
    }

    /// <summary>Срастание половин: возврат {2,3} в основную сеть, вторая удаляется.</summary>
    public async Task HealHalvesAsync(CancellationToken ct)
    {
        for (var i = 2; i < Nodes; i++)
        {
            await RunDockerAsync(["network", "disconnect", _splitNet!.Name, NodeName(i)], ct);
            await RunDockerAsync(["network", "connect", NetName, NodeName(i)], ct);
        }
        try { await _splitNet!.DeleteAsync(ct); } catch { /* уже нет */ }
        _splitNet = null;
    }

    /// <summary>docker stop (graceSec — `-t`, SIGKILL после бюджета; A1/A11).</summary>
    public async Task StopNodeAsync(int i, int graceSec = 10, CancellationToken ct = default)
    {
        await RunDockerAsync(["stop", "-t", graceSec.ToString(), NodeName(i)], ct);
    }

    /// <summary>docker start после остановки (A11).</summary>
    public async Task StartNodeAsync(int i, CancellationToken ct)
    {
        await RunDockerAsync(["start", NodeName(i)], ct);
    }

    /// <summary>docker kill — SIGKILL без drain (грязный рестарт A11(в)).</summary>
    public async Task KillNodeAsync(int i, CancellationToken ct)
    {
        await RunDockerAsync(["kill", NodeName(i)], ct);
    }

    public Task<string> RunDockerAsync(IReadOnlyList<string> args, CancellationToken ct)
        => OwnS3E2eEnvironmentShim.RunProcessAsync("docker", [.. args], ct, TimeSpan.FromMinutes(2));

    /// <summary>Сквозная метрика серии (глава 07 §10, [METRIC] по канону
    /// docs/e2e-launch.md): строка [METRIC] name=value в Console + дописывание
    /// в metrics.log ArtifactsDir (по образцу phases.log).</summary>
    public async Task LogMetricAsync(string name, string value)
    {
        var line = $"{name}={value}";
        Console.WriteLine($"[METRIC] {line}");
        await File.AppendAllTextAsync(Path.Combine(_artifactsDir, "metrics.log"),
            $"{DateTime.UtcNow:O} {line}\n");
    }

        /// <summary>Телеметрия ДО любых удалений: logs+inspect ВСЕХ узлов.</summary>
    public async Task CollectDiagnosticsAsync(string mark)
    {
        foreach (var container in _containers)
        {
            var name = container.Name.TrimStart('/');
            await ShellToFileAsync($"logs --tail 2000 {name}",
                Path.Combine(_artifactsDir, $"container-{name}-{mark}.log"));
            await ShellToFileAsync($"inspect {name}",
                Path.Combine(_artifactsDir, $"container-{name}-{mark}.json"));
        }
    }

    /// <summary>Полный teardown при любом исходе: телеметрия → dispose контейнеров →
    /// volume rm с ретраями ≤30 с → сеть → ассерт чистоты по runId (own-only).</summary>
    public async ValueTask DisposeAsync()
    {
        var sw = Stopwatch.StartNew();
        try { await CollectDiagnosticsAsync("teardown"); }
        catch { /* телеметрия не маскирует исход сценария */ }

        foreach (var container in _containers)
        {
            try { await container.StopAsync(); } catch { /* уже остановлен */ }
            if (!_failed)
            {
                try { await container.DisposeAsync(); } catch { /* уже удалён */ }
            }
        }
        if (_failed)
        {
            // docs/e2e-launch.md §3: узлы ОСТАНОВЛЕНЫ, не удалены (тома/сети остаются;
            // чистка — README-cleanup.txt вручную, own-only по runId)
            Console.WriteLine($"[PHASE] teardown: failed-прогон — узлы остановлены (не удалены), {sw.Elapsed.TotalSeconds:F1}s");
            return;
        }

        foreach (var volumeName in _volumeNames)
        {
            for (var i = 0; ; i++)
            {
                var left = await RunDockerOrNullAsync(
                    ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name={volumeName}"],
                    CancellationToken.None);
                if (string.IsNullOrWhiteSpace(left) || i >= 15)
                    break;
                await TryDockerAsync(["volume", "rm", "-f", volumeName]);
                await Task.Delay(TimeSpan.FromSeconds(2));
            }
        }
        if (_splitNet is not null)
        {
            await TryDockerAsync(["network", "rm", _splitNet.Name]);
        }
        try { await _net.DeleteAsync(); } catch { /* уже нет */ }

        // Ассерт чистоты: ни контейнера, ни тома, ни сети с runId прогона (own-only)
        var containers = (await RunDockerOrNullAsync(
                ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name={_runId}"],
                CancellationToken.None))
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.Contains(_runId, StringComparison.Ordinal)).ToList();
        containers.Should().BeEmpty($"teardown убрал все контейнеры прогона {_runId}");
        foreach (var volumeName in _volumeNames)
            (await RunDockerOrNullAsync(
                    ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name={volumeName}"],
                    CancellationToken.None))
                .Trim().Should().BeEmpty($"после teardown не осталось тома {volumeName}");
        (await RunDockerOrNullAsync(
                ["network", "ls", "--format", "{{.Name}}", "--filter", $"name={NetName}"],
                CancellationToken.None))
            .Trim().Should().BeEmpty($"после teardown не осталось сети {NetName}");
        if (_splitNet is not null)
            (await RunDockerOrNullAsync(
                    ["network", "ls", "--format", "{{.Name}}", "--filter", $"name={_splitNet.Name}"],
                    CancellationToken.None))
                .Trim().Should().BeEmpty($"после teardown не осталось сети {_splitNet.Name}");

        Console.WriteLine($"[PHASE] teardown: ok, {sw.Elapsed.TotalSeconds:F1}s");
        await File.AppendAllTextAsync(Path.Combine(_artifactsDir, "phases.log"),
            $"{DateTime.UtcNow:O} phase teardown: ok, {sw.Elapsed.TotalSeconds:F1}s\n");
    }

    private static string NodeNameStatic(string runId, int i) => $"os3m-n{i}-{runId}";

    private static async Task RemoveAttemptAsync(INetwork net, List<IContainer> containers,
        List<string> volumeNames)
    {
        foreach (var container in containers)
        {
            try { await container.StopAsync(); } catch { /* не стартовал */ }
            try { await container.DisposeAsync(); } catch { /* уже удалён */ }
        }
        foreach (var volumeName in volumeNames)
            await TryDockerAsync(["volume", "rm", "-f", volumeName]);
        try { await net.DeleteAsync(); } catch { /* уже нет */ }
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

    private static async Task TryDockerAsync(IReadOnlyList<string> args)
    {
        try { await OwnS3E2eEnvironmentShim.RunProcessAsync("docker", [.. args], CancellationToken.None, TimeSpan.FromMinutes(2)); }
        catch { /* чистка — лучшее усилие */ }
    }

    private static int FreePort()
    {
        var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}

/// <summary>Шим процесс-хелперов (RunProcessAsync OwnS3E2eEnvironment — private).</summary>
public static class OwnS3E2eEnvironmentShim
{
    public static async Task<string> RunProcessAsync(
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
            if (process.ExitCode != 0)
                throw new ApplicationException($"{file} {string.Join(' ', args)} → {process.ExitCode}: {error.Trim()}");
            return output.Trim();
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            try { process.Kill(entireProcessTree: true); }
            catch { /* процесс мог уже выйти */ }
            throw new ApplicationException(
                $"{file} {string.Join(' ', args)} не завершился за {timeout.TotalSeconds:0} c — убит");
        }
    }
}
