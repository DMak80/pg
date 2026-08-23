using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// Драйвер на живом docker (задача 16, spec §11.1 docker-серия): create/start/stop/rm,
// идемпотентность EnsureNode/RemoveNode/StopNode, BusyPorts. Гейт: PGW_TEST_DOCKER=1.
public class DockerDriverTests
{
    private const string AlpineImage = "alpine:3.20";

    private const string Host = "local";

    private static readonly DockerEngineFactory Factory = new();

    // Уникальный суффикс прогона: параллельные запуски не конфликтуют по именам.
    private static readonly string Suffix = Guid.NewGuid().ToString("N")[..6];

    private static IDockerEngine NewEngine() => Factory.Create("unix:///var/run/docker.sock", hostAlias: Host);

    private static PlainClusterDriver NewDriver()
        => new([new HostEndpoint(Host, "unix:///var/run/docker.sock")], Factory, enableDoorman: false, AlpineImage);

    private static ShardTopology Topology(string cluster, NodeAddress addr) => new(
        cluster, "s1", $"{cluster}-s1",
        new Dictionary<string, NodeAddress> { ["n1"] = addr });

    private static readonly InstallSecrets Secrets = new("su", "sb", "app", "ba", "mv");

    private static readonly EtcdEndpoints Etcd = new(["http://localhost:2379"]);

    private static string ContainerName(string cluster) => $"pgw-{cluster}-s1-n1";

    // Pull образа один раз на прогон (только при включённой серии): create не тянет образ сам.
    private static readonly Lazy<Task> ImageReady = new(async () =>
    {
        await using var engine = (DockerEngine)NewEngine();
        await engine.PullImageAsync(AlpineImage, CancellationToken.None); // ошибка — исключение в Lazy
    }, LazyThreadSafetyMode.ExecutionAndPublication);

    private static async Task CleanupAsync(string cluster)
    {
        try
        {
            await using var engine = NewEngine();
            await engine.RemoveContainerAsync(ContainerName(cluster), force: true, CancellationToken.None);
            await engine.RemoveVolumeAsync($"{ContainerName(cluster)}-data", CancellationToken.None);
        }
        catch
        {
            // best-effort уборка перед тестом
        }
    }

    [Fact]
    public async Task CreateAndStart_AlpineSleepWithPublishPort_Listed()
    {
        DockerTrait.SkipIfUnavailable();
        await ImageReady.Value;

        // Arrange — живой контейнер движком напрямую (Cmd — только у alpine-теста)
        var cluster = $"c1{Suffix}";
        await CleanupAsync(cluster);
        const int port = 25101;
        await using var engine = NewEngine();
        var spec = new ContainerSpec(
            AlpineImage, new Dictionary<string, string>(), $"{ContainerName(cluster)}-data", "/data",
            [new PortMap(8080, port)], "n1", null, null, null, ["sleep", "60"]);

        // Act
        var created = await engine.CreateContainerAsync(spec, ContainerName(cluster), CancellationToken.None);
        var started = await engine.StartContainerAsync(ContainerName(cluster), CancellationToken.None);
        var list = await engine.ListContainersAsync(ContainerName(cluster), all: true, CancellationToken.None);

        // Assert: контейнер создан, запущен и виден в списке
        created.IsSuccess.Should().BeTrue(created.Error?.ToString());
        started.IsSuccess.Should().BeTrue(started.Error?.ToString());
        var container = list.Value.Should().ContainSingle().Subject;
        container.Names.Should().Contain(ContainerName(cluster));
        container.State.Should().Be("running");

        await CleanupAsync(cluster);
    }

    [Fact]
    public async Task EnsureNode_SecondCall_NoErrorSingleContainer()
    {
        DockerTrait.SkipIfUnavailable();
        await ImageReady.Value;

        // Arrange
        var cluster = $"c2{Suffix}";
        await CleanupAsync(cluster);
        var driver = NewDriver();
        var addr = new NodeAddress(Host, new NodePorts(25102, 25103, 25104));

        // Act — повторный Ensure с тем же именем
        var first = await driver.EnsureNodeAsync(Topology(cluster, addr), "n1", addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);
        var second = await driver.EnsureNodeAsync(Topology(cluster, addr), "n1", addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);
        await using var engine = NewEngine();
        var list = await engine.ListContainersAsync(ContainerName(cluster), all: true, CancellationToken.None);

        // Assert: оба вызова успешны, контейнер ровно один (идемпотентность по имени)
        first.IsSuccess.Should().BeTrue(first.Error?.ToString());
        second.IsSuccess.Should().BeTrue(second.Error?.ToString());
        list.Value.Should().HaveCount(1);

        await CleanupAsync(cluster);
    }

    [Fact]
    public async Task RemoveNode_ContainerAndVolumeGone_RepeatSucceeds()
    {
        DockerTrait.SkipIfUnavailable();
        await ImageReady.Value;

        // Arrange
        var cluster = $"c3{Suffix}";
        await CleanupAsync(cluster);
        var driver = NewDriver();
        var addr = new NodeAddress(Host, new NodePorts(25105, 25106, 25107));
        await driver.EnsureNodeAsync(Topology(cluster, addr), "n1", addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);

        // Act — удаление и его повтор (все объекты уже исчезли)
        var removed = await driver.RemoveNodeAsync(cluster, "s1", "n1", CancellationToken.None);
        var repeat = await driver.RemoveNodeAsync(cluster, "s1", "n1", CancellationToken.None);
        await using var engine = NewEngine();
        var list = await engine.ListContainersAsync(ContainerName(cluster), all: true, CancellationToken.None);

        // Assert: контейнера нет, повтор без ошибки (404 = успех)
        removed.IsSuccess.Should().BeTrue(removed.Error?.ToString());
        repeat.IsSuccess.Should().BeTrue(repeat.Error?.ToString());
        list.Value.Should().BeEmpty();

        await CleanupAsync(cluster);
    }

    [Fact]
    public async Task StopNode_ContainerExited_RepeatSucceeds()
    {
        DockerTrait.SkipIfUnavailable();
        await ImageReady.Value;

        // Arrange — живой контейнер (sleep) с корректным именем ноды; volume bind
        var cluster = $"c4{Suffix}";
        await CleanupAsync(cluster);
        const int port = 25108;
        await using (var engine = NewEngine())
        {
            var spec = new ContainerSpec(
                AlpineImage, new Dictionary<string, string>(), $"{ContainerName(cluster)}-data", "/data",
                [new PortMap(8080, port)], "n1", null, null, null, ["sleep", "60"]);
            (await engine.CreateContainerAsync(spec, ContainerName(cluster), CancellationToken.None))
                .IsSuccess.Should().BeTrue();
            (await engine.StartContainerAsync(ContainerName(cluster), CancellationToken.None))
                .IsSuccess.Should().BeTrue();
        }

        // Act — остановка (карантин E3: без rm) и её повтор
        var driver = NewDriver();
        var stopped = await driver.StopNodeAsync(cluster, "s1", "n1", CancellationToken.None);
        var repeat = await driver.StopNodeAsync(cluster, "s1", "n1", CancellationToken.None);
        await using var check = NewEngine();
        var list = await check.ListContainersAsync(ContainerName(cluster), all: true, CancellationToken.None);

        // Assert: контейнер существует в exited (данные/volume не тронуты), повтор — успех
        stopped.IsSuccess.Should().BeTrue(stopped.Error?.ToString());
        repeat.IsSuccess.Should().BeTrue(repeat.Error?.ToString());
        var container = list.Value.Should().ContainSingle().Subject;
        container.State.Should().Be("exited");

        await CleanupAsync(cluster);
    }

    [Fact]
    public async Task BusyPorts_ReflectsPublishedPort()
    {
        DockerTrait.SkipIfUnavailable();
        await ImageReady.Value;

        // Arrange — живой контейнер с publish-портом
        var cluster = $"c5{Suffix}";
        await CleanupAsync(cluster);
        const int port = 25109;
        await using var engine = NewEngine();
        var spec = new ContainerSpec(
            AlpineImage, new Dictionary<string, string>(), "", "",
            [new PortMap(8080, port)], "n1", null, null, null, ["sleep", "60"]);
        await engine.CreateContainerAsync(spec, ContainerName(cluster), CancellationToken.None);
        await engine.StartContainerAsync(ContainerName(cluster), CancellationToken.None);

        // Act
        var busy = await engine.BusyPortsAsync(CancellationToken.None);

        // Assert: пара (хост, порт) отражена
        busy.IsSuccess.Should().BeTrue(busy.Error?.ToString());
        busy.Value.Should().Contain((Host, port));

        await CleanupAsync(cluster);
    }
}
