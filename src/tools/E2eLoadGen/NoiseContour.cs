using System.Diagnostics;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace PgWorker.IntegrationTests.E2eLoadGen;

/// <summary>Один шумовой контур (t24, spec §5.1): сеть pgw-noise-{guid} +
/// контейнеры профиля. Канон docs/e2e-isolation.md: guid во всех именах,
/// телеметрия до удалений, own-only teardown при любом исходе, ассерт
/// чистоты своего контура.</summary>
public sealed class NoiseContour : IAsyncDisposable
{
    private const string AlpineImage = "alpine:3.20";
    private const string PythonImage = "python:3.12-alpine";

    // Общий скрипт DNS-зонда (t24 spec §5.2) — живёт в тестовом проекте.
    private const string ProbeScriptRelativePath =
        "src/tests/PgWorker.IntegrationTests/E2e/dns_probe.py";

    private readonly string _prefix;
    private readonly INetwork _net;
    private readonly List<IContainer> _containers = [];
    private readonly DirectoryInfo _ioDir;
    private readonly string _outDir;

    private NoiseContour(string prefix, INetwork net, DirectoryInfo ioDir, string outDir)
    {
        _prefix = prefix;
        _net = net;
        _ioDir = ioDir;
        _outDir = outDir;
    }

    public static async Task<NoiseContour> CreateAsync(
        int index, string profile, string outDir, CancellationToken ct)
    {
        var prefix = $"pgw-noise-{Guid.NewGuid():N}";
        var net = new NetworkBuilder().WithName(prefix).Build();
        var ioDir = Directory.CreateDirectory(Path.Combine(outDir, $"contour{index}", "io"));
        var contour = new NoiseContour(prefix, net, ioDir, outDir);
        try
        {
            await net.CreateAsync(ct);
            contour.AddContainers(index, profile);
            foreach (var container in contour._containers)
                await container.StartAsync(ct);
            return contour;
        }
        catch
        {
            // частично поднятый контур не оставляем (лучшими усилиями)
            try
            {
                await contour.DisposeAsync();
            }
            catch
            {
                // guid-имена: чужие прогоны не заденем; добьёт ручная зачистка
            }

            throw;
        }
    }

    private void AddContainers(int index, string profile)
    {
        var root = FindRepoRoot();
        var script = File.ReadAllBytes(Path.Combine(root, ProbeScriptRelativePath));
        // Три категории целей spec §5.2: алиас сети + спец-резолвер хоста +
        // внешнее имя (форвард наружу — отдельный тракт от embedded DNS;
        // quay.io в локальном зеркале, трафика не тянет).
        var stormTargets = "noise-target,host.docker.internal,quay.io";

        IContainer Target() => new ContainerBuilder(PythonImage)
            .WithName($"{_prefix}-t{index}")
            .WithNetwork(_net)
            .WithNetworkAliases("noise-target")
            .WithCommand("sh", "-c", "while true; do echo alive; sleep 1; done")
            .Build();

        IContainer Storm() => new ContainerBuilder(PythonImage)
            .WithName($"{_prefix}-dns{index}")
            .WithNetwork(_net)
            .WithEnvironment("DNS_PROBE_TARGETS", stormTargets)
            .WithEnvironment("DNS_PROBE_MODE", "storm")
            .WithResourceMapping(script, "/dns_probe.py")
            .WithCommand("python3", "-u", "/dns_probe.py")
            .Build();

        IContainer Cpu(int j) => new ContainerBuilder(AlpineImage)
            .WithName($"{_prefix}-cpu{j}")
            .WithNetwork(_net)
            .WithCommand("sha256sum", "/dev/zero")
            .Build();

        IContainer Io() => new ContainerBuilder(AlpineImage)
            .WithName($"{_prefix}-io")
            .WithNetwork(_net)
            .WithBindMount(_ioDir.FullName, "/data")
            .WithCommand("sh", "-c",
                "while true; do dd if=/dev/zero of=/data/noise bs=1M count=256 conv=fsync 2>/dev/null; rm -f /data/noise; done")
            .Build();

        // Профили spec §5.1: dns — шторм резолвов; cpu — чистая CPU-нагрузка;
        // io — дисковая демона; full — комбинация (паттерн реального контура
        // по совокупной нагрузке на демона: сеть+контейнеры+DNS+CPU+IO).
        switch (profile)
        {
            case LoadGenOptions.ProfileDns:
                _containers.Add(Target());
                _containers.Add(Storm());
                break;
            case LoadGenOptions.ProfileCpu:
                _containers.Add(Cpu(1));
                _containers.Add(Cpu(2));
                break;
            case LoadGenOptions.ProfileIo:
                _containers.Add(Io());
                break;
            case LoadGenOptions.ProfileFull:
                _containers.Add(Target());
                _containers.Add(Storm());
                _containers.Add(Cpu(1));
                _containers.Add(Cpu(2));
                _containers.Add(Io());
                break;
            default:
                throw new ApplicationException($"неизвестный профиль {profile}");
        }
    }

    public async ValueTask DisposeAsync()
    {
        var problems = new List<string>();
        try
        {
            Directory.CreateDirectory(_outDir);
        }
        catch
        {
            // телеметрия — «лучшими усилиями»
        }

        // Телеметрия прежде удалений: docker logs своих контейнеров (шторм —
        // агрегаты storm-dns; target — пульс) — данные для отчёта по фазам.
        foreach (var container in _containers)
        {
            try
            {
                var id = container.Id;
                if (!string.IsNullOrEmpty(id))
                {
                    var name = (await RunAsync("docker", ["inspect", "-f", "{{.Name}}", id])).Trim().TrimStart('/');
                    var logs = await RunAsync("docker", ["logs", "--timestamps", id]);
                    await File.WriteAllTextAsync(Path.Combine(_outDir, $"container-{name}.log"), logs);
                }
            }
            catch (Exception e)
            {
                problems.Add($"логи {container.Name}: {e.Message}");
            }
        }

        foreach (var container in _containers)
        {
            try
            {
                await container.DisposeAsync();
            }
            catch (Exception e)
            {
                problems.Add($"dispose {container.Name}: {e.Message}");
            }
        }

        try
        {
            await _net.DeleteAsync();
        }
        catch (Exception e)
        {
            problems.Add($"сеть {_prefix}: {e.Message}");
        }

        try
        {
            _ioDir.Delete(recursive: true);
        }
        catch (IOException)
        {
            // bind-каталог мог быть занят демоном; guid-имя — заденем только своё
        }

        // АССЕРТ ЧИСТОТЫ контура: ни контейнеров, ни сети со своим префиксом.
        var leftContainers = (await RunAsync("docker", ["ps", "-a", "--format", "{{.Names}}"]))
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Count(n => n.StartsWith(_prefix, StringComparison.Ordinal));
        if (leftContainers > 0)
            problems.Add($"остались контейнеры контура: {leftContainers}");
        var leftNet = (await RunAsync("docker", ["network", "ls", "--format", "{{.Name}}", "--filter", $"name={_prefix}"])).Trim();
        if (leftNet.Length > 0)
            problems.Add($"осталась сеть {_prefix}");
        if (problems.Count > 0)
            throw new ApplicationException($"{_prefix}: teardown шумового контура неполный:\n- " + string.Join("\n- ", problems));
    }

    // Корень репозитория: тот же маркер, что у E2eFixture.FindRoot.
    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "docker", "node", "Dockerfile")))
            dir = dir.Parent;
        return dir?.FullName ?? throw new ApplicationException("корень репозитория не найден");
    }

    // docker-CLI с бюджетом 2 мин (зависший процесс обязан умирать, а не висеть).
    private static async Task<string> RunAsync(string file, string[] args)
    {
        var psi = new ProcessStartInfo(file, args)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        using var process = Process.Start(psi) ?? throw new ApplicationException($"не удалось запустить {file}");
        using var budget = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var outTask = process.StandardOutput.ReadToEndAsync(budget.Token);
        var errTask = process.StandardError.ReadToEndAsync(budget.Token);
        try
        {
            await process.WaitForExitAsync(budget.Token);
            var output = await outTask;
            var error = await errTask;
            if (process.ExitCode != 0)
                throw new ApplicationException($"{file} {string.Join(' ', args)} → {process.ExitCode}: {error.Trim()}");
            return output.Trim();
        }
        catch (OperationCanceledException)
        {
            process.Kill(entireProcessTree: true);
            throw new ApplicationException($"{file} {string.Join(' ', args)} не завершился за 2 мин — убит");
        }
    }
}
