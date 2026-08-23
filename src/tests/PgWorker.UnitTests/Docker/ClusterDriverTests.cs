using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using PgWorker.Core;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// Драйверы кластера plain/swarm (задача 15): идемпотентный EnsureNode, stop-only
// карантин E3, порядок RemoveNode, constraint+publish у swarm-сервиса.
public class ClusterDriverTests
{
    // Мок движка: записывает вызовы и отвечает заготовками.
    private sealed class FakeEngine : IDockerEngine
    {
        public List<(string Call, object? Arg)> Calls = [];

        public List<DockerContainer> Containers = [];
        public List<DockerSwarmNode> Nodes = [];
        public IReadOnlySet<(string, int)> Busy = new HashSet<(string, int)>();
        public ContainerSpec? CreatedSpec;
        public ServiceSpec? CreatedService;
        public string CreatedName = "";

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public Task<Result> PingAsync(CancellationToken ct)
        {
            Calls.Add(("ping", null));
            return Task.FromResult(Result.Success());
        }

        public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(string namePrefix, bool all, CancellationToken ct)
        {
            Calls.Add(("list-containers", namePrefix));
            var matched = Containers
                .Where(c => c.Names.Any(n => n.StartsWith(namePrefix, StringComparison.Ordinal)))
                .ToList();
            return Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success(matched));
        }

        public Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct)
        {
            Calls.Add(("create", spec));
            CreatedSpec = spec;
            CreatedName = name;
            return Task.FromResult(Result.Success());
        }

        public Task<Result> StartContainerAsync(string idOrName, CancellationToken ct)
        {
            Calls.Add(("start", idOrName));
            return Task.FromResult(Result.Success());
        }

        public Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct)
        {
            Calls.Add(("stop", idOrName));
            return Task.FromResult(Result.Success());
        }

        public Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct)
        {
            Calls.Add(("rm-container", idOrName));
            return Task.FromResult(Result.Success());
        }

        public Task<Result> RemoveVolumeAsync(string name, CancellationToken ct)
        {
            Calls.Add(("rm-volume", name));
            return Task.FromResult(Result.Success());
        }

        public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct)
        {
            Calls.Add(("list-nodes", null));
            return Task.FromResult(Result<IReadOnlyList<DockerSwarmNode>>.Success(Nodes));
        }

        public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct)
        {
            Calls.Add(("create-service", spec));
            CreatedService = spec;
            return Task.FromResult(Result.Success());
        }

        public Task<Result> RemoveServiceAsync(string name, CancellationToken ct)
        {
            Calls.Add(("rm-service", name));
            return Task.FromResult(Result.Success());
        }

        public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerTask>>.Success([]));

        public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct)
        {
            Calls.Add(("busy-ports", null));
            return Task.FromResult(Result<IReadOnlySet<(string, int)>>.Success(Busy));
        }
    }

    // Фабрика-заглушка: раздаёт преднастроенные движки (перехват Create).
    private sealed class FakeFactory(IDockerEngine engine) : DockerEngineFactory
    {
        public override IDockerEngine Create(string endpoint, string? hostAlias = null) => engine;
    }

    private static ShardTopology Topology(NodeAddress addr) => new(
        "shop", "shard1", "shop-shard1",
        new Dictionary<string, NodeAddress> { ["shard1a"] = addr });

    private static readonly NodeAddress Addr = new("h1", new NodePorts(15432, 18008, 16432));

    private static readonly InstallSecrets Secrets = new("su", "sb", "app", "ba", "mv");

    private static readonly EtcdEndpoints Etcd = new(["http://etcd:2379"]);

    private static PlainClusterDriver NewPlainDriver(FakeEngine engine)
        => new([new HostEndpoint("h1", "fake://h1")], new FakeFactory(engine), enableDoorman: true);

    [Fact]
    public async Task EnsureNode_ExistingContainer_DoesNotRecreate()
    {
        // Arrange — контейнер ноды уже есть (повторный тик/пересоздание после сбоя)
        var engine = new FakeEngine
        {
            Containers = [new DockerContainer("id1", ["pgw-shop-shard1-shard1a"], "running", "img")],
        };
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.EnsureNodeAsync(Topology(Addr), "shard1a", Addr, Secrets, Etcd, CancellationToken.None);

        // Assert: ни create, ни start — только сверка списком
        result.IsSuccess.Should().BeTrue();
        engine.Calls.Should().NotContain(c => c.Call == "create");
        engine.Calls.Should().NotContain(c => c.Call == "start");
    }

    [Fact]
    public async Task EnsureNode_NewContainer_EnvFromBuildersAndVolumeAndPorts()
    {
        // Arrange
        var engine = new FakeEngine();
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.EnsureNodeAsync(Topology(Addr), "shard1a", Addr, Secrets, Etcd, CancellationToken.None);

        // Assert: имя pgw-<C>-<X>-<n>; env из SpiloEnvBuilder + PGW_NODE_HOST;
        // volume -data; publish тройка портов; затем start
        result.IsSuccess.Should().BeTrue();
        engine.CreatedName.Should().Be("pgw-shop-shard1-shard1a");
        engine.CreatedSpec.Should().NotBeNull();
        var spec = engine.CreatedSpec!;
        spec.VolumeName.Should().Be("pgw-shop-shard1-shard1a-data");
        spec.VolumeDest.Should().Be("/home/postgres/pgroot");
        spec.Hostname.Should().Be("shard1a");
        spec.Env["SCOPE"].Should().Be("shop-shard1");
        spec.Env["ETCD_HOSTS"].Should().Be("http://etcd:2379");
        spec.Env["PGW_NODE_HOST"].Should().Be("h1");
        spec.Env["DOORMAN_CONFIG"].Should().Contain("pool_mode = \"transaction\"");
        spec.Env["HAPROXY_CONFIG"].Should().Contain("server shard1a h1:15432 check port 18008");
        spec.Ports.Should().BeEquivalentTo(
        [
            new PortMap(5432, 15432),
            new PortMap(8008, 18008),
            new PortMap(6432, 16432),
        ]);
        engine.Calls.Select(c => c.Call).Should().Equal("list-containers", "create", "start");
    }

    [Fact]
    public async Task EnsureNode_DoormanDisabled_NoDoormanPortAndConfig()
    {
        // Arrange — флаг EnableDoorman=false (R1: узел без пулера, компромисс стенда)
        var engine = new FakeEngine();
        var driver = new PlainClusterDriver(
            [new HostEndpoint("h1", "fake://h1")], new FakeFactory(engine), enableDoorman: false);

        // Act
        var result = await driver.EnsureNodeAsync(Topology(Addr), "shard1a", Addr, Secrets, Etcd, CancellationToken.None);

        // Assert: порт 6432 не публикуется, DOORMAN_CONFIG не генерируется
        result.IsSuccess.Should().BeTrue();
        engine.CreatedSpec.Should().NotBeNull();
        var spec = engine.CreatedSpec!;
        spec.Ports.Should().NotContain(p => p.ContainerPort == 6432);
        spec.Env.Should().NotContainKey("DOORMAN_CONFIG");
        spec.Env.Should().NotContainKey("PGW_DOORMAN_PORT");
    }

    [Fact]
    public async Task RemoveNode_StopsRemovesContainerAndVolume()
    {
        // Arrange
        var engine = new FakeEngine();
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.RemoveNodeAsync("shop", "shard1", "shard1a", CancellationToken.None);

        // Assert: stop → rm(force) → volume rm; никаких других вызовов
        result.IsSuccess.Should().BeTrue();
        engine.Calls.Select(c => c.Call).Should().Equal("stop", "rm-container", "rm-volume");
        engine.Calls[1].Arg.Should().Be("pgw-shop-shard1-shard1a");
        engine.Calls[2].Arg.Should().Be("pgw-shop-shard1-shard1a-data");
    }

    [Fact]
    public async Task StopNode_StopsOnly_NoRemoveNoVolume()
    {
        // Arrange — карантин вернувшегося шарда (E3): данные на месте
        var engine = new FakeEngine();
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.StopNodeAsync("shop", "shard1", "shard1a", CancellationToken.None);

        // Assert: только stop
        result.IsSuccess.Should().BeTrue();
        engine.Calls.Select(c => c.Call).Should().Equal("stop");
        engine.Calls[0].Arg.Should().Be("pgw-shop-shard1-shard1a");
    }

    [Fact]
    public async Task GetHosts_UsedSlotsByContainerCount()
    {
        // Arrange — на хосте два pgw-контейнера
        var engine = new FakeEngine
        {
            Containers =
            [
                new DockerContainer("a", ["pgw-shop-shard1-shard1a"], "running", "i"),
                new DockerContainer("b", ["pgw-shop-shard1-shard1b"], "exited", "i"),
                new DockerContainer("c", ["foreign"], "running", "i"),
            ],
        };
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.GetHostsAsync(CancellationToken.None);

        // Assert: UsedSlots = число контейнеров префикса (foreign не считается)
        result.IsSuccess.Should().BeTrue();
        var host = result.Value.Should().ContainSingle().Subject;
        host.Name.Should().Be("h1");
        host.UsedSlots.Should().Be(2);
    }

    [Fact]
    public async Task Swarm_EnsureNode_ServiceConstraintAndPublishPorts()
    {
        // Arrange — swarm-нода h1 существует; драйвер строит сервис с constraint
        var engine = new FakeEngine
        {
            Nodes = [new DockerSwarmNode("node-id-1", "h1", "ready", 0)],
        };
        var driver = new SwarmClusterDriver("fake://manager", new FakeFactory(engine), enableDoorman: true);

        // Act
        var result = await driver.EnsureNodeAsync(Topology(Addr), "shard1a", Addr, Secrets, Etcd, CancellationToken.None);

        // Assert: ServiceSpec с constraint по id найденной ноды; шаблон — как у plain
        result.IsSuccess.Should().BeTrue();
        engine.CreatedService.Should().NotBeNull();
        var spec = engine.CreatedService!;
        spec.Name.Should().Be("pgw-shop-shard1-shard1a");
        spec.NodeConstraint.Should().Be("node-id-1");
        spec.Template.Ports.Should().Contain(p => p.ContainerPort == 5432 && p.HostPort == 15432);
        spec.Template.VolumeName.Should().Be("pgw-shop-shard1-shard1a-data");
    }

    [Fact]
    public async Task Swarm_GetHosts_FromReadyNodes()
    {
        // Arrange
        var engine = new FakeEngine
        {
            Nodes =
            [
                new DockerSwarmNode("n1", "h1", "ready", 3),
                new DockerSwarmNode("n2", "h2", "down", 1),
            ],
        };
        var driver = new SwarmClusterDriver("fake://manager", new FakeFactory(engine), enableDoorman: true);

        // Act
        var result = await driver.GetHostsAsync(CancellationToken.None);

        // Assert: только ready-ноды, UsedSlots = running tasks
        result.IsSuccess.Should().BeTrue();
        var host = result.Value.Should().ContainSingle().Subject;
        host.Name.Should().Be("h1");
        host.UsedSlots.Should().Be(3);
    }

    [Fact]
    public async Task ListNodeObjects_ReturnsPrefixedNames()
    {
        // Arrange
        var engine = new FakeEngine
        {
            Containers =
            [
                new DockerContainer("a", ["pgw-shop-shard1-shard1a"], "running", "i"),
                new DockerContainer("b", ["pgw-shop-shard2-shard2a"], "running", "i"),
                new DockerContainer("c", ["pgw-other-shard1-shard1a"], "running", "i"),
            ],
        };
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.ListNodeObjectsAsync("shop", CancellationToken.None);

        // Assert: имена pgw-<C>-*, детерминированный порядок
        result.Value.Should().Equal("pgw-shop-shard1-shard1a", "pgw-shop-shard2-shard2a");
    }
}
