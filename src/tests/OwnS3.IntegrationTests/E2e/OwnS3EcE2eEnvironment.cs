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
/// Изолированное docker-E2E окружение ownS3 в EC-режиме (t40, docs/e2e-isolation.md
/// + docs/e2e-launch.md; модель OwnS3E2eEnvironment): своя сеть os3-en-{runId},
/// свой контейнер os3-es-{runId} (образ owns3:e2e — общий сборщик OwnS3E2eImage),
/// СВОИ 4 тома os3-ev{1..4}-{runId} под /data1../data4 (env OWNS3_DATA_DIR —
/// список путей, OWNS3_EC_SET_SIZE=4, OWNS3_EC_PARITY=1 — топология 3+1).
/// Специфика EC: RestartWithVolumesAsync(1,3) — стоп+rm контейнера и подъём
/// с подмножеством монтирований (отсоединённые тома живут в docker — данные
/// сохраняются); ReplaceVolumeEmptyAsync(idx) — подмена тома пустым (диск-
/// замена, канон 06 §9). Телеметрия — единая с эталоном: [PHASE]/phases.log/
/// CollectDiagnosticsAsync ДО удалений/MarkFailed (stop без удаления)/README-
/// cleanup.txt/retry ×3. Гейты: PGW_TEST_DOCKER=1 (DockerTrait),
/// PGW_TEST_E2E_NOBUILD=1 — бисект.
/// </summary>
public sealed class OwnS3EcE2eEnvironment : IAsyncDisposable
{
    private const int DriveCount = 4;

    private readonly INetwork _net;
    private readonly IVolume[] _volumes;
    private IContainer _service;
    private readonly string _runId;
    private readonly int _hostPort;
    private bool _failed;

    private OwnS3EcE2eEnvironment(string slug, string runId, INetwork net, IVolume[] volumes,
        IContainer service, string artifactsDir, int hostPort)
    {
        Slug = slug;
        _runId = runId;
        _net = net;
        _volumes = volumes;
        _service = service;
        ArtifactsDir = artifactsDir;
        _hostPort = hostPort;
        ContainerName = $"os3-es-{runId}";
        VolumeNames = Enumerable.Range(1, DriveCount).Select(i => $"os3-ev{i}-{runId}").ToArray();
    }

    public string Slug { get; }

    /// <summary>Артефакты телеметрии прогона (логи/inspect/phases.log).</summary>
    public string ArtifactsDir { get; }

    /// <summary>Docker-имя сети окружения (os3-en-{runId}).</summary>
    public string NetName => $"os3-en-{_runId}";

    /// <summary>Docker-имя контейнера сервиса (os3-es-{runId}).</summary>
    public string ContainerName { get; }

    /// <summary>Docker-имена 4 томов (os3-ev{1..4}-{runId}).</summary>
    public IReadOnlyList<string> VolumeNames { get; }

    /// <summary>S3-грань с хоста (SDK/HttpClient): динамический порт.</summary>
    public string S3BaseUrl => $"http://localhost:{_hostPort}";

    /// <summary>Тег прогона (8 hex) — компонент имён бакетов сценариев.</summary>
    public string RunTag => _runId[..8];

    /// <summary>Полный guid прогона (ассерт чистоты, README-cleanup).</summary>
    public string RunId => _runId;

    public void MarkFailed() => _failed = true;

    /// <summary>Подъём: общий сборщик образа → сеть → 4 тома → контейнер
    /// (wait /healthz ≤ 30 с); retry ×3 против распознанных гонок — подъём
    /// заново ЦЕЛИКОМ (полное удаление попытки).</summary>
    public static async Task<OwnS3EcE2eEnvironment> StartAsync(
        string slug, CancellationToken ct = default)
    {
        if (Environment.GetEnvironmentVariable(DockerTrait.EnvVar) != "1")
            throw new InvalidOperationException(
                $"E2E требует {DockerTrait.EnvVar}=1 (иначе Skip в тесте DockerTrait'ом)");

        var runId = Guid.NewGuid().ToString("N");
        var artifactsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-artifacts-{runId}");
        Directory.CreateDirectory(artifactsDir);

        // Статический ассет процесса: owns3:e2e (общий с single-сервером).
        await OwnS3E2eImage.BuildOnceAsync(artifactsDir, ct);

        for (var attempt = 1; ; attempt++)
        {
            var net = new NetworkBuilder().WithName($"os3-en-{runId}").Build();
            var volumes = Enumerable.Range(1, DriveCount)
                .Select(i => new VolumeBuilder().WithName($"os3-ev{i}-{runId}").Build())
                .ToArray();
            var hostPort = FreePort();
            var service = BuildContainer(net, hostPort, runId, Enumerable.Range(1, DriveCount).ToArray());

            async Task RemoveAttemptAsync()
            {
                await SafeStopAsync(service, remove: true);
                foreach (var volumeName in Enumerable.Range(1, DriveCount).Select(i => $"os3-ev{i}-{runId}"))
                    await TryDockerAsync(["volume", "rm", "-f", volumeName]);
                try { await net.DeleteAsync(ct); } catch { /* уже нет */ }
            }

            try
            {
                var sw = Stopwatch.StartNew();
                await net.CreateAsync(ct);
                foreach (var volume in volumes)
                    await volume.CreateAsync(ct);
                await service.StartAsync(ct);
                var env = new OwnS3EcE2eEnvironment(slug, runId, net, volumes, service, artifactsDir, hostPort);
                await env.LogPhaseAsync("start", sw);
                await File.WriteAllTextAsync(Path.Combine(artifactsDir, "README-cleanup.txt"),
                    "# Зачистка окружения прогона (own-only, по runId):\n"
                    + $"docker rm -f {env.ContainerName}\n"
                    + string.Join("", env.VolumeNames.Select(v => $"docker volume rm -f {v}\n"))
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

    /// <summary>Рестарт сервиса с ПОДМНОЖЕСТВОМ томов (деградация/возврат):
    /// стоп+rm контейнера; отсоединённые тома живут в docker — данные
    /// сохраняются; новый контейнер монтирует только тома с номерами online
    /// (1..4). Отсутствующий путь позиции — диск offline (канон 06 §2 в правке
    /// Task 0), возврат тома возвращает диск в сет.</summary>
    public async Task RestartWithVolumesAsync(params int[] online)
    {
        var sw = Stopwatch.StartNew();
        await SafeStopAsync(_service, remove: true);
        _service = BuildContainer(_net, _hostPort, _runId, online);
        await _service.StartAsync(CancellationToken.None);
        await LogPhaseAsync($"restart-with-volumes-{string.Join('-', online)}", sw);
    }

    /// <summary>Подмена тома пустым (замена диска, канон 06 §9): volume rm +
    /// новый пустой том с тем же именем — данные позиции стираются.</summary>
    public async Task ReplaceVolumeEmptyAsync(int index)
    {
        var sw = Stopwatch.StartNew();
        var name = $"os3-ev{index}-{_runId}";
        await RunDockerAsync(["volume", "rm", "-f", name], CancellationToken.None);
        var volume = new VolumeBuilder().WithName(name).Build();
        await volume.CreateAsync(CancellationToken.None);
        _volumes[index - 1] = volume;
        await LogPhaseAsync($"replace-volume-{index}", sw);
    }

    /// <summary>Сценарий под политикой телеметрии (docs/e2e-launch.md §3) —
    /// модель эталона: исключение тела → телеметрия "failed" ДО остановки →
    /// MarkFailed → rethrow; teardown — DisposeAsync при любом исходе.</summary>
    public static async Task RunScenarioAsync(
        string slug, Func<OwnS3EcE2eEnvironment, Task> body, CancellationToken ct = default)
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
    /// выводом процесса (бюджет 2 мин).</summary>
    public Task<string> RunDockerAsync(IReadOnlyList<string> args, CancellationToken ct)
        => RunProcessAsync("docker", [.. args], ct, TimeSpan.FromMinutes(2));

    /// <summary>Телеметрия ДО любых удалений (docs/e2e-launch.md §1).</summary>
    public async Task CollectDiagnosticsAsync(string mark)
    {
        await ShellToFileAsync($"logs --tail 2000 {ContainerName}",
            Path.Combine(ArtifactsDir, $"container-{ContainerName}-{mark}.log"));
        await ShellToFileAsync($"inspect {ContainerName}",
            Path.Combine(ArtifactsDir, $"container-{ContainerName}-{mark}.json"));
    }

    /// <summary>Полный teardown при любом исходе: телеметрия → dispose
    /// контейнера → ВСЕ 4 тома rm с ретраями (~30 с, полл 2 с) → сеть →
    /// ассерт чистоты по runId (own-only).</summary>
    public async ValueTask DisposeAsync()
    {
        var sw = Stopwatch.StartNew();
        try { await CollectDiagnosticsAsync("teardown"); }
        catch { /* телеметрия не маскирует исход сценария */ }

        if (_failed)
        {
            // docs/e2e-launch.md §3: контейнер ОСТАНОВИТЬ, не удалить (тома/
            // сеть остаются для разбора; чистка — README-cleanup.txt вручную).
            await SafeStopAsync(_service, remove: false);
            await LogPhaseAsync("teardown", sw);
            return;
        }

        await SafeStopAsync(_service, remove: true);

        // тома: ретраи до ~30 с каждое (демон освобождает асинхронно).
        foreach (var volumeName in VolumeNames)
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

        try { await _net.DeleteAsync(); } catch { /* уже нет */ }

        // Ассерт чистоты: ни контейнера, ни томов, ни сети с runId прогона.
        var containers = await RunDockerOrNullAsync(
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name={_runId}"],
            CancellationToken.None);
        containers.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.Contains(_runId, StringComparison.Ordinal))
            .ToList()
            .Should().BeEmpty($"teardown убрал все контейнеры прогона {_runId}");
        (await RunDockerOrNullAsync(
                ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name=os3-ev"],
                CancellationToken.None))
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.Contains(_runId, StringComparison.Ordinal))
            .ToList()
            .Should().BeEmpty($"после teardown не осталось томов прогона {_runId}");
        (await RunDockerOrNullAsync(
                ["network", "ls", "--format", "{{.Name}}", "--filter", $"name={NetName}"],
                CancellationToken.None))
            .Trim().Should().BeEmpty($"после teardown не осталось сети {NetName}");

        await LogPhaseAsync("teardown", sw);
    }

    // ── внутренняя механика ──

    // Контейнер сервиса: EC-конфиг 3+1 — OWNS3_DATA_DIR всегда ПОЛНЫЙ список
    // позиций (отсутствующий путь смонтированного тома = диск offline);
    // монтируются только live-тома номера из driveNumbers (1..4). Секреты —
    // литералы e2e-эталона (клиенты общие). Wait — /healthz ≤ 30 с
    // (деградация healthz не роняет — спека §4.3).
    private static IContainer BuildContainer(INetwork net, int hostPort, string runId, int[] driveNumbers)
    {
        var builder = new ContainerBuilder(OwnS3E2eImage.Tag)
            .WithName($"os3-es-{runId}")
            .WithNetwork(net)
            .WithPortBinding(hostPort, 9000)
            .WithEnvironment("OWNS3_ROOT_USER", OwnS3E2eEnvironment.RootUser)
            .WithEnvironment("OWNS3_ROOT_PASSWORD", OwnS3E2eEnvironment.RootPassword)
            .WithEnvironment("OWNS3_ACCESS_KEYS__0__ACCESSKEY", OwnS3E2eEnvironment.WriterAccessKey)
            .WithEnvironment("OWNS3_ACCESS_KEYS__0__SECRETKEY", OwnS3E2eEnvironment.WriterSecretKey)
            .WithEnvironment("OWNS3_ACCESS_KEYS__0__POLICY", "read-write")
            .WithEnvironment("OWNS3_ACCESS_KEYS__1__ACCESSKEY", OwnS3E2eEnvironment.ReaderAccessKey)
            .WithEnvironment("OWNS3_ACCESS_KEYS__1__SECRETKEY", OwnS3E2eEnvironment.ReaderSecretKey)
            .WithEnvironment("OWNS3_ACCESS_KEYS__1__POLICY", "read-only")
            .WithEnvironment("OWNS3_ACCESS_KEYS__2__ACCESSKEY", OwnS3E2eEnvironment.AdminAccessKey)
            .WithEnvironment("OWNS3_ACCESS_KEYS__2__SECRETKEY", OwnS3E2eEnvironment.AdminSecretKey)
            .WithEnvironment("OWNS3_ACCESS_KEYS__2__POLICY", "admin")
            .WithEnvironment("OWNS3_HOST_ID", $"os3-e2e-ec-{runId[..8]}")
            .WithEnvironment("OWNS3_DATA_DIR", "/data1,/data2,/data3,/data4")
            .WithEnvironment("OWNS3_EC_SET_SIZE", "4")
            .WithEnvironment("OWNS3_EC_PARITY", "1")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(
                    request => request.ForPort(9000).ForPath("/healthz"),
                    wait => wait.WithTimeout(TimeSpan.FromSeconds(30))));
        foreach (var n in driveNumbers)
            builder = builder.WithVolumeMount($"os3-ev{n}-{runId}", $"/data{n}");
        return builder.Build();
    }

    // Финал фазы (docs/e2e-launch.md §2): [PHASE]-метка с таймингом в Console +
    // UTC-запись в phases.log; фаза > 60 с — немедленный сбор docker-диагностики.
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

    private static async Task TryDockerAsync(IReadOnlyList<string> args)
    {
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
