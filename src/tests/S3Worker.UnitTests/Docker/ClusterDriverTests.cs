using FluentAssertions;
using S3Worker.Docker.Drivers;
using Xunit;

namespace S3Worker.UnitTests.Docker;

// Docker-слой домена S3 (arch/23 §2/§7, spec §7): ensure/remove ноды
// s3w-<C>-node<k> с volume данных, инспекции S7/E9, env-сверка ротации,
// сироты X2, placement-атрибуты хостов (arch/23 §4).
public class ClusterDriverTests
{
    // Движок-дублёр: запоминает spec создания, хранит контейнеры/тома/факты
    // инспекта (порт StubEngine ClusterDriverEnvTests valkey).
    private sealed class StubEngine : IDockerEngine
    {
        public ContainerSpec? CreatedSpec { get; private set; }

        public string? CreatedName { get; private set; }

        public ServiceSpec? CreatedService { get; private set; }

        public bool ContainerExists { get; set; }

        public bool ContainerRunning { get; set; } = true;

        public int PublishedPort { get; set; } = 18001;

        public IReadOnlyDictionary<string, string>? Env { get; set; }

        public Shared.Docker.NodeLimits? Limits { get; set; }

        public IReadOnlyList<Shared.Docker.DockerContainer> Containers { get; set; } = [];

        public IReadOnlyList<Shared.Docker.DockerSwarmNode> Nodes { get; set; } =
            [new Shared.Docker.DockerSwarmNode("id-a", "h1", "ready", 0)];

        public IReadOnlyList<string> Services { get; set; } = [];

        public HashSet<string> Volumes { get; } = new(StringComparer.Ordinal);

        public readonly List<string> DeletedContainers = [];
        public readonly List<string> DeletedVolumes = [];

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        // Листинг по префиксу (сироты X2 — arch/24 §2); label-атрибут запоминается.
        public string? LastVolumeLabelKey { get; private set; }
        public string? LastVolumeLabel { get; private set; }

        public Task<Result<IReadOnlyList<string>>> ListVolumesAsync(string namePrefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(
                Volumes.Where(v => v.StartsWith(namePrefix, StringComparison.Ordinal))
                    .OrderBy(v => v, StringComparer.Ordinal).ToList()));

        public Task<Result> EnsureVolumeAsync(string name, string labelKey, string label, CancellationToken ct)
        {
            LastVolumeLabelKey = labelKey;
            LastVolumeLabel = label;
            return EnsureVolumeAsync(name, ct);
        }

        public Task<Result> EnsureVolumeAsync(string name, CancellationToken ct)
        {
            Volumes.Add(name);
            return Task.FromResult(Result.Success());
        }

        public Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<bool>.Success(Volumes.Contains(name)));

        public Task<Result> DeleteVolumeAsync(string name, CancellationToken ct)
        {
            DeletedVolumes.Add(name);
            Volumes.Remove(name);
            return Task.FromResult(Result.Success());
        }

        public Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct)
        {
            CreatedSpec = spec;
            CreatedName = name;
            ContainerExists = true;
            return Task.FromResult(Result.Success());
        }

        public Task<Result> StartContainerAsync(string idOrName, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct)
        {
            DeletedContainers.Add(idOrName);
            ContainerExists = false;
            return Task.FromResult(Result.Success());
        }

        public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(string namePrefix, bool all, CancellationToken ct)
        {
            // Явный список (GetHosts/сироты) — фильтр по префиксу; иначе флаг
            // ContainerExists моделирует «контейнер с полным именем есть»
            // (идемпотентность ensure: префикс = полное имя s3w-<C>-node<k>).
            if (Containers.Count > 0)
                return Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success(
                    Containers.Where(c => c.Names.Any(n => n.StartsWith(namePrefix, StringComparison.Ordinal))).ToList()));
            return Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success(
                ContainerExists
                    ? [new DockerContainer("stub", [namePrefix], "running", "owns3:dev")]
                    : []));
        }

        public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerSwarmNode>>.Success(Nodes));

        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectContainerEnvAsync(string idOrName, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(
                ContainerExists ? Env : null));

        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectServiceEnvAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(Env));

        public Task<Result<Shared.Docker.NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<Shared.Docker.NodeLimits?>.Success(
                ContainerExists ? Limits : null));

        public Task<Result<Shared.Docker.NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<Shared.Docker.NodeLimits?>.Success(Limits));

        // E9-инспект: объект есть → порт из PortBindings + Running; нет → null.
        public Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, int containerPort, CancellationToken ct)
            => Task.FromResult(Result<DockerNodeEndpoint?>.Success(
                ContainerExists ? new DockerNodeEndpoint(PublishedPort, ContainerRunning) : null));

        public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct)
        {
            CreatedService = spec;
            return Task.FromResult(Result.Success());
        }

        public Task<Result> RemoveServiceAsync(string name, CancellationToken ct)
        {
            DeletedContainers.Add(name);
            ContainerExists = false;
            return Task.FromResult(Result.Success());
        }

        // Остальные члены union — вне сценариев этого файла.
        private static NotImplementedException NotUsed() => new("движок-дублёр: маршрут вне сценария теста");

        public Task<Result> PingAsync(CancellationToken ct) => throw NotUsed();
        public Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct) => throw NotUsed();
        public Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct) => throw NotUsed();
        public Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct) => throw NotUsed();
        public Task<Result> EnsureNetworkAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> DeleteNetworkAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> NetworkConnectAsync(string network, string container, CancellationToken ct) => throw NotUsed();
        public Task<Result> RemoveVolumeAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(
                Services.Where(s => s.StartsWith(namePrefix, StringComparison.Ordinal)).ToList()));
        public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<string>?>> InspectContainerCmdAsync(string idOrName, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<string>?>> InspectServiceCmdAsync(string name, CancellationToken ct) => throw NotUsed();
    }

    private sealed class StubFactory(StubEngine engine) : DockerEngineFactory
    {
        public override IDockerEngine Create(string endpoint, string? hostAlias = null) => engine;
    }

    // Фабрика с роутингом движков по endpoint'у (каждому хосту — свой движок).
    private sealed class RoutingFactory(StubEngine[] engines) : DockerEngineFactory
    {
        private int _next;

        public override IDockerEngine Create(string endpoint, string? hostAlias = null)
            => engines[Math.Min(Interlocked.Increment(ref _next) - 1, engines.Length - 1)];
    }

    private static IReadOnlyDictionary<string, string> SampleEnv { get; } =
        new Dictionary<string, string>
        {
            ["OWNS3_ROOT_USER"] = "root",
            ["OWNS3_ROOT_PASSWORD"] = "secret",
            ["OWNS3_DATA_DIR"] = "/data",
        };

    private static S3NodeSpec Spec(string cluster = "c1", string node = "node1", int port = 18001)
        => new(cluster, node, "h1", port, "owns3:dev", SampleEnv, 1m, 1024L);

    // AAA: Plain — ensure ноды: имя s3w-<C>-node<k>, порт 9000→host, env, том,
    // unless-stopped, label s3worker, лимиты.
    [Fact]
    public async Task Plain_EnsureNode_CreatesContainerWithVolumeAndLimits()
    {
        // Arrange — Plain-драйвер с движком-дублёром (контейнер отсутствует).
        var engine = new StubEngine();
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

        // Act
        var result = await driver.EnsureNodeAsync(Spec(), contourNetwork: null, CancellationToken.None);

        // Assert — спека контейнера один в один arch/23 §2 (имена/порт/том/политики).
        result.IsSuccess.Should().BeTrue();
        engine.CreatedName.Should().Be("s3w-c1-node1");
        engine.CreatedSpec!.Ports.Should().Equal([new PortMap(9000, 18001)]);
        engine.CreatedSpec.Env.Should().BeEquivalentTo(SampleEnv);
        engine.CreatedSpec.VolumeName.Should().Be("s3w-c1-node1-data");
        engine.CreatedSpec.VolumeDest.Should().Be("/data");
        engine.CreatedSpec.RestartPolicy.Should().Be("unless-stopped");
        engine.CreatedSpec.LabelKey.Should().Be("s3worker");
        engine.CreatedSpec.Label.Should().Be("c1");
        engine.CreatedSpec.CpuCores.Should().Be(1.0); // decimal-спека → double? ContainerSpec
        engine.CreatedSpec.MemoryBytes.Should().Be(1024L);
        engine.CreatedSpec.Network.Should().BeNull();
        engine.Volumes.Should().Contain("s3w-c1-node1-data");
        engine.LastVolumeLabelKey.Should().Be("s3worker");
        engine.LastVolumeLabel.Should().Be("c1");
    }

    // AAA: Plain — contour-сеть стенда/E2E доезжает до NetworkMode контейнера.
    [Fact]
    public async Task Plain_EnsureNode_ContourNetwork_SetsNetwork()
    {
        // Arrange
        var engine = new StubEngine();
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

        // Act
        var result = await driver.EnsureNodeAsync(Spec(), contourNetwork: "contour", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        engine.CreatedSpec!.Network.Should().Be("contour");
    }

    // AAA: Plain — идемпотентность: существующий контейнер не пересоздаётся.
    [Fact]
    public async Task Plain_EnsureNode_ExistingContainer_NotRecreated()
    {
        // Arrange — контейнер s3w-c1-node1 уже есть (спека та же).
        var engine = new StubEngine { ContainerExists = true };
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

        // Act
        var result = await driver.EnsureNodeAsync(Spec(), contourNetwork: null, CancellationToken.None);

        // Assert — create не вызван (CreatedSpec остался null).
        result.IsSuccess.Should().BeTrue();
        engine.CreatedSpec.Should().BeNull();
    }

    // AAA: Plain — remove с томом: RemoveContainer + DeleteVolume.
    [Fact]
    public async Task Plain_RemoveNode_RemovesContainerAndVolume()
    {
        // Arrange — контейнер и том существуют.
        var engine = new StubEngine { ContainerExists = true };
        await engine.EnsureVolumeAsync("s3w-c1-node1-data", CancellationToken.None);
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

        // Act
        var result = await driver.RemoveNodeAsync("c1", "node1", removeVolume: true, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        engine.DeletedContainers.Should().Contain("s3w-c1-node1");
        engine.DeletedVolumes.Should().Contain("s3w-c1-node1-data");
    }

    // AAA: Plain — remove БЕЗ тома: контейнер убран, том не тронут.
    [Fact]
    public async Task Plain_RemoveNode_KeepVolume_LeavesVolume()
    {
        // Arrange
        var engine = new StubEngine { ContainerExists = true };
        await engine.EnsureVolumeAsync("s3w-c1-node1-data", CancellationToken.None);
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

        // Act
        var result = await driver.RemoveNodeAsync("c1", "node1", removeVolume: false, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        engine.DeletedContainers.Should().Contain("s3w-c1-node1");
        engine.DeletedVolumes.Should().BeEmpty();
    }

    // AAA: Plain — S7-инспект: контейнера нет → null (положительное свидетельство);
    // есть → Host+published-порт+Running.
    [Fact]
    public async Task Plain_InspectNode_NullWhenAbsent_PortWhenPresent()
    {
        // Arrange
        var engine = new StubEngine { ContainerExists = false };
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

        // Act + Assert — объекта нет → null.
        var absent = await driver.InspectNodeAsync("c1", "node1", CancellationToken.None);
        absent.IsSuccess.Should().BeTrue();
        absent.Value.Should().BeNull();

        // Act + Assert — объект есть → факт размещения.
        engine.ContainerExists = true;
        engine.PublishedPort = 18007;
        var present = await driver.InspectNodeAsync("c1", "node1", CancellationToken.None);
        present.IsSuccess.Should().BeTrue();
        present.Value.Should().BeEquivalentTo(new S3NodeInspection("h1", 18007, Running: true));
    }

    // AAA: Plain — env живой ноды (сверка ротации): перебор хостов, первый найденный.
    [Fact]
    public async Task Plain_NodeEnv_ReturnsContainerEnvOrNull()
    {
        // Arrange
        var engine = new StubEngine { ContainerExists = true, Env = SampleEnv };
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

        // Act
        var env = await driver.NodeEnvAsync("c1", "node1", CancellationToken.None);

        // Assert — env фактического контейнера; объекта нет → null.
        env.IsSuccess.Should().BeTrue();
        env.Value.Should().BeEquivalentTo(SampleEnv);
        engine.ContainerExists = false;
        var none = await driver.NodeEnvAsync("c1", "node1", CancellationToken.None);
        none.IsSuccess.Should().BeTrue();
        none.Value.Should().BeNull();
    }

    // AAA: Plain — сироты X2: контейнеры s3w-<C>-* + тома node1..node9.
    [Fact]
    public async Task Plain_ListNodeObjects_ContainersAndVolumes()
    {
        // Arrange — контейнер node1 + тома node1/node2 существуют.
        var engine = new StubEngine
        {
            Containers = [new DockerContainer("id1", ["s3w-c1-node1"], "running", "owns3:dev")],
        };
        await engine.EnsureVolumeAsync("s3w-c1-node1-data", CancellationToken.None);
        await engine.EnsureVolumeAsync("s3w-c1-node2-data", CancellationToken.None);
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

        // Act
        var result = await driver.ListNodeObjectsAsync("c1", CancellationToken.None);

        // Assert — и контейнер, и оба тома в перечне.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEquivalentTo(["s3w-c1-node1", "s3w-c1-node1-data", "s3w-c1-node2-data"]);
    }

    // AAA: Plain — сироты X2 multi-host: сирота-том на другом хосте (утраченный
    // контейнер) попадает в перечень — существование тома проверяется на ВСЕХ
    // движках, не только на первом.
    [Fact]
    public async Task Plain_ListNodeObjects_OrphanVolumeOnOtherHost()
    {
        // Arrange — два хоста; том node2 существует только на h2.
        var h1 = new StubEngine();
        var h2 = new StubEngine();
        await h2.EnsureVolumeAsync("s3w-c1-node2-data", CancellationToken.None);
        var driver = new PlainClusterDriver(
            [new HostEndpoint("h1", "fake://h1"), new HostEndpoint("h2", "fake://h2")],
            new RoutingFactory([h1, h2]));

        // Act
        var result = await driver.ListNodeObjectsAsync("c1", CancellationToken.None);

        // Assert — сирота-том h2 в перечне (снос X2 его подберёт).
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEquivalentTo(["s3w-c1-node2-data"]);
    }

    // AAA: Swarm — сироты: сервисы по префиксу + сирота-том на другой swarm-ноде
    // (утраченный контейнер) — объединение инспекций всех движков (как в сносе).
    [Fact]
    public async Task Swarm_ListNodeObjects_OrphanVolumeOnOtherNode()
    {
        // Arrange — менеджер + две swarm-ноды; том node3 только на второй ноде.
        var manager = new StubEngine();
        var n1 = new StubEngine();
        var n2 = new StubEngine();
        await n2.EnsureVolumeAsync("s3w-c1-node3-data", CancellationToken.None);
        var driver = new SwarmClusterDriver(
            "fake://manager",
            new RoutingFactory([manager, n1, n2]),
            [new HostEndpoint("n1", "fake://n1"), new HostEndpoint("n2", "fake://n2")]);

        // Act
        var result = await driver.ListNodeObjectsAsync("c1", CancellationToken.None);

        // Assert — сирота-том второй ноды в перечне.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEquivalentTo(["s3w-c1-node3-data"]);
    }

    // AAA: Plain — placement-атрибуты (arch/23 §4): Name из Hosts, UsedSlots по
    // числу контейнеров s3w-*, Labels пуст (plain-селекторы — только docker_host).
    [Fact]
    public async Task Plain_GetHosts_NamesFromTable_SlotsByContainers()
    {
        // Arrange — на h1 два контейнера s3w-*.
        var engine = new StubEngine
        {
            Containers =
            [
                new DockerContainer("id1", ["s3w-c1-node1"], "running", "owns3:dev"),
                new DockerContainer("id2", ["s3w-c2-node1"], "running", "owns3:dev"),
            ],
        };
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

        // Act
        var result = await driver.GetHostsAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var host = result.Value.Should().ContainSingle().Subject;
        host.Name.Should().Be("h1");
        host.UsedSlots.Should().Be(2);
        host.Labels.Should().BeEmpty();
    }

    // AAA: Plain — docker-факт лимитов (автоконверге C): контейнера нет → null.
    [Fact]
    public async Task Plain_NodeResources_LimitsOrNull()
    {
        // Arrange
        var engine = new StubEngine { ContainerExists = true, Limits = new Shared.Docker.NodeLimits(1_000_000_000, 1024) };
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

        // Act
        var limits = await driver.NodeResourcesAsync("c1", "node1", CancellationToken.None);

        // Assert — факт есть; объекта нет → null.
        limits.IsSuccess.Should().BeTrue();
        limits.Value.Should().BeEquivalentTo(new Shared.Docker.NodeLimits(1_000_000_000, 1024));
        engine.ContainerExists = false;
        var none = await driver.NodeResourcesAsync("c1", "node1", CancellationToken.None);
        none.IsSuccess.Should().BeTrue();
        none.Value.Should().BeNull();
    }

    // AAA: Swarm — ensure: сервис vwk-имени, env/том/лимиты в шаблоне таска.
    [Fact]
    public async Task Swarm_EnsureNode_ServiceWithTemplate()
    {
        // Arrange — swarm-менеджер: нода h1 (id-a).
        var engine = new StubEngine();
        var driver = new SwarmClusterDriver("fake://manager", new StubFactory(engine));

        // Act
        var result = await driver.EnsureNodeAsync(Spec(), contourNetwork: null, CancellationToken.None);

        // Assert — имя сервиса, constraint по Hostname==h1, шаблон с томом/env.
        result.IsSuccess.Should().BeTrue();
        engine.CreatedService!.Name.Should().Be("s3w-c1-node1");
        engine.CreatedService.NodeConstraint.Should().Be("id-a");
        engine.CreatedService.Template.Env.Should().BeEquivalentTo(SampleEnv);
        engine.CreatedService.Template.VolumeName.Should().Be("s3w-c1-node1-data");
        engine.CreatedService.Template.VolumeDest.Should().Be("/data");
        engine.CreatedService.Template.Ports.Should().Equal([new PortMap(9000, 18001)]);
        engine.CreatedService.Template.LabelKey.Should().Be("s3worker");
    }

    // AAA: Swarm — placement-атрибуты из ListNodes: ready-ноды с Labels.
    [Fact]
    public async Task Swarm_GetHosts_ReadyNodesWithLabels()
    {
        // Arrange — h1 ready с метками, h2 down (в placement не участвует).
        var engine = new StubEngine
        {
            Nodes =
            [
                new DockerSwarmNode("id-a", "h1", "ready", 2,
                    new Dictionary<string, string> { ["geo"] = "dc1" }),
                new DockerSwarmNode("id-b", "h2", "down", 0,
                    new Dictionary<string, string> { ["geo"] = "dc2" }),
            ],
        };
        var driver = new SwarmClusterDriver("fake://manager", new StubFactory(engine));

        // Act
        var result = await driver.GetHostsAsync(CancellationToken.None);

        // Assert — только ready; метки размеченной отражены.
        result.IsSuccess.Should().BeTrue();
        var host = result.Value.Should().ContainSingle().Subject;
        host.Name.Should().Be("h1");
        host.UsedSlots.Should().Be(2);
        host.Labels.Should().Equal(new Dictionary<string, string> { ["geo"] = "dc1" });
    }

    // AAA: Swarm — env сервиса (ротация) и сироты (сервисы по префиксу).
    [Fact]
    public async Task Swarm_NodeEnv_FromService()
    {
        // Arrange
        var engine = new StubEngine { Env = SampleEnv };
        var driver = new SwarmClusterDriver("fake://manager", new StubFactory(engine));

        // Act
        var env = await driver.NodeEnvAsync("c1", "node1", CancellationToken.None);

        // Assert
        env.IsSuccess.Should().BeTrue();
        env.Value.Should().BeEquivalentTo(SampleEnv);
    }
}
