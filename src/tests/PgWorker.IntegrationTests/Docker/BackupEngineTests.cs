using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// Docker-грань джоба бэкапа (t02): логи контейнера + runtime-инспект
// (exit-код) + HostConfig-поля tmpfs/extra_hosts (arch/19 §2/§6).
public class BackupEngineTests
{
    [Fact]
    public async Task Logs_And_RuntimeInspect_OfExitedContainer()
    {
        // Arrange — контейнер без портов: печатает маркер и падает с кодом 7.
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var container = new ContainerBuilder("alpine:3.20")
            .WithCommand("sh", "-c", "echo pgw-marker; exit 7")
            .Build();
        await container.StartAsync(ct);

        // Act — ждём exited, тянем логи и инспект напрямую движком сокета.
        var engine = new DockerEngineFactory().Create("unix:///var/run/docker.sock");
        var bareName = container.Name.TrimStart('/'); // Testcontainers отдаёт имя с ведущим "/", движок — без
        string? name = null;
        for (var i = 0; i < 30; i++)
        {
            var list = await engine.ListContainersAsync(bareName, all: true, ct);
            if (list.IsSuccess && list.Value.FirstOrDefault(c => c.Names.Contains(bareName)) is { State: "exited" } found)
            {
                name = found.Names[0];
                break;
            }
            await Task.Delay(500, ct);
        }
        name.Should().NotBeNull("контейнер обязан выйти за 15 c");

        var logs = await engine.GetContainerLogsAsync(name!, tail: 100, ct);
        var inspect = await engine.InspectContainerAsync(name!, ct);

        // Assert — stdout размультиплексирован; инспект несёт факт/код выхода.
        logs.IsSuccess.Should().BeTrue(logs.Error?.ToString());
        logs.Value.Should().Contain("pgw-marker");
        inspect.Value.Running.Should().BeFalse();
        inspect.Value.ExitCode.Should().Be(7);
    }

    [Fact]
    public async Task Create_WithTmpfsAndExtraHosts_Applied()
    {
        // Arrange / Act — контейнер с tmpfs-квотой и host-gateway-записью.
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        const string name = "pgw-backup-engine-test";
        var spec = new ContainerSpec(
            Image: "alpine:3.20",
            Env: new Dictionary<string, string>(),
            VolumeName: "",
            VolumeDest: "",
            Ports: [],
            Hostname: name,
            CpuCores: null,
            MemoryBytes: null,
            LabelKey: "pgworker",
            Label: "enginetest",
            ResetEntrypoint: true,
            Cmd: ["sh", "-c", "mount | grep backup-staging; getent hosts host.docker.internal; exit 0"],
            Tmpfs: new Dictionary<string, string> { ["/backup-staging"] = "size=10485760" },
            ExtraHosts: ["host.docker.internal:host-gateway"],
            RestartPolicy: "no");

        var engine = new DockerEngineFactory().Create("unix:///var/run/docker.sock");
        var created = await engine.CreateContainerAsync(spec, name, ct);
        var started = await engine.StartContainerAsync(name, ct);
        string? exited = null;
        for (var i = 0; i < 30; i++)
        {
            var list = await engine.ListContainersAsync(name, all: true, ct);
            if (list.IsSuccess && list.Value.Any(c => c.Names.Contains(name) && c.State == "exited"))
            {
                exited = name;
                break;
            }
            await Task.Delay(500, ct);
        }
        var logs = exited is null ? null : await engine.GetContainerLogsAsync(name, tail: 50, ct);
        var removed = await engine.RemoveContainerAsync(name, force: true, ct);

        // Assert — tmpfs смонтирован, extra_hosts зарезолвился, удаление идемпотентно.
        created.IsSuccess.Should().BeTrue(created.Error?.ToString());
        started.IsSuccess.Should().BeTrue(started.Error?.ToString());
        exited.Should().NotBeNull("контейнер обязан выйти");
        logs!.Value.Should().Contain("tmpfs on /backup-staging");
        logs.Value.Should().Contain("host.docker.internal");
        removed.IsSuccess.Should().BeTrue();
    }
}
