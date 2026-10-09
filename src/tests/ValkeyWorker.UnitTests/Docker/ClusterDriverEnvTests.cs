using FluentAssertions;
using Shared.Core;
using ValkeyWorker.Docker.Drivers;
using Xunit;

namespace ValkeyWorker.UnitTests.Docker;

// env-TLS (arch/21 §2): Env спецификации ноды доезжает до docker-объекта
// (Plain — ContainerSpec контейнера; Swarm — TaskTemplate.ContainerSpec
// сервиса), NodeEnvAsync отдаёт env фактического объекта (null = объекта
// нет), CleanupLegacyVolumeAsync удаляет легаси-том старой volume-модели.
public class ClusterDriverEnvTests
{
    // Движок-дублёр: запоминает spec создания (env/cmd) и отдаёт env по имени.
    private sealed class StubEngine : IDockerEngine
    {
        public ContainerSpec? CreatedSpec { get; private set; }

        public string? CreatedName { get; private set; }

        public ServiceSpec? CreatedService { get; private set; }

        public IReadOnlyDictionary<string, string>? Env { get; set; }

        public IReadOnlyList<Shared.Docker.DockerContainer> Containers { get; set; } = [];

        public bool VolumeInUse { get; set; }

        public readonly List<string> DeletedVolumes = [];

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct)
        {
            CreatedSpec = spec;
            CreatedName = name;
            return Task.FromResult(Result.Success());
        }

        public Task<Result> StartContainerAsync(string idOrName, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(string namePrefix, bool all, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success(Containers));

        public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerSwarmNode>>.Success(
                [new DockerSwarmNode("node-id-a", "h1", "ready", 0)]));

        public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct)
        {
            CreatedService = spec;
            return Task.FromResult(Result.Success());
        }

        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectContainerEnvAsync(string idOrName, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(Env));

        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectServiceEnvAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(Env));

        public Task<Result> DeleteVolumeAsync(string name, CancellationToken ct)
        {
            DeletedVolumes.Add(name);
            return Task.FromResult(VolumeInUse
                ? Result.Failed(new ApplicationException("volume in use"))
                : Result.Success());
        }

        // Остальные члены union — вне сценария теста.
        private static NotImplementedException NotUsed() => new("движок-дублёр: маршрут вне сценария теста");

        public Task<Result> PingAsync(CancellationToken ct) => throw NotUsed();
        public Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct) => throw NotUsed();
        public Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct) => throw NotUsed();
        public Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct) => throw NotUsed();
        public Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct) => throw NotUsed();
        public Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct) => throw NotUsed();
        public Task<Result> EnsureNetworkAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> DeleteNetworkAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> NetworkConnectAsync(string network, string container, CancellationToken ct) => throw NotUsed();
        public Task<Result> RemoveVolumeAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> EnsureVolumeAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> RemoveServiceAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<string>?>> InspectContainerCmdAsync(string idOrName, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<string>?>> InspectServiceCmdAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, int containerPort, CancellationToken ct) => throw NotUsed();
        public Task<Result<Shared.Docker.NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<Shared.Docker.NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct) => throw NotUsed();
    }

    private sealed class StubFactory(StubEngine engine) : DockerEngineFactory
    {
        public override IDockerEngine Create(string endpoint, string? hostAlias = null)
            => engine;
    }

    private static IReadOnlyDictionary<string, string> SampleEnv { get; } =
        new Dictionary<string, string>
        {
            ["VALKEY_TLS_CERT"] = "-----BEGIN CERTIFICATE-----",
            ["VALKEY_TLS_KEY"] = "-----BEGIN PRIVATE KEY-----",
            ["VALKEY_TLS_CA"] = "-----BEGIN CERTIFICATE-----",
        };

    private static ValkeyNodeSpec Spec(IReadOnlyDictionary<string, string>? env)
        => new("c1", "node1", "h1", 17001, "valkey/valkey:9.1.2",
            ["sh", "-c", "umask 077; exec valkey-server"], 1m, 1024L, Env: env);

    [Fact]
    public async Task Plain_EnsureNode_EnvДоезжаетДоContainerSpec()
    {
        // Arrange — Plain-драйвер с движком-дублёром (контейнер отсутствует).
        var engine = new StubEngine();
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

        // Act
        var result = await driver.EnsureNodeAsync(Spec(SampleEnv), CancellationToken.None);

        // Assert — env из spec в ContainerSpec (без Binds: томов в модели нет).
        result.IsSuccess.Should().BeTrue();
        engine.CreatedName.Should().Be("vwk-c1-node1");
        engine.CreatedSpec!.Env.Should().BeEquivalentTo(SampleEnv);
        engine.CreatedSpec.Binds.Should().BeNull();
        engine.CreatedSpec.Cmd.Should().BeEquivalentTo(["sh", "-c", "umask 077; exec valkey-server"]);
    }

    [Fact]
    public async Task Plain_NodeEnv_ОтдаётEnvЖивогоКонтейнераИлиNull()
    {
        // Arrange — движок знает env контейнера vwk-c1-node1.
        var engine = new StubEngine { Env = SampleEnv };
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

        // Act
        var env = await driver.NodeEnvAsync("c1", "node1", CancellationToken.None);

        // Assert — env фактического объекта; объекта нет → null.
        env.IsSuccess.Should().BeTrue();
        env.Value.Should().BeEquivalentTo(SampleEnv);
        engine.Env = null;
        var none = await driver.NodeEnvAsync("c1", "node1", CancellationToken.None);
        none.IsSuccess.Should().BeTrue();
        none.Value.Should().BeNull();
    }

    [Fact]
    public async Task Plain_CleanupLegacyVolume_УдаляетТомНаВсехEngines()
    {
        // Arrange — два хоста (по движку на каждый).
        var e1 = new StubEngine();
        var e2 = new StubEngine();
        var engines = new[] { e1, e2 };
        var driver = new PlainClusterDriver(
            [new HostEndpoint("h1", "fake://h1"), new HostEndpoint("h2", "fake://h2")],
            new RoutingFactory(engines));

        // Act
        var result = await driver.CleanupLegacyVolumeAsync("c1", CancellationToken.None);

        // Assert — DELETE vwk-c1-tls прошёл на каждом engine; 404 = успех.
        result.IsSuccess.Should().BeTrue();
        e1.DeletedVolumes.Should().Contain("vwk-c1-tls");
        e2.DeletedVolumes.Should().Contain("vwk-c1-tls");
    }

    [Fact]
    public async Task Plain_CleanupLegacyVolume_409_Failed()
    {
        // Arrange — том занят (409 volume-in-use).
        var engine = new StubEngine { VolumeInUse = true };
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new StubFactory(engine));

        // Act
        var result = await driver.CleanupLegacyVolumeAsync("c1", CancellationToken.None);

        // Assert — Failed (безусловный ретрай следующим тиком надзора).
        result.IsSuccess.Should().BeFalse();
    }

    // Фабрика с роутингом движков по endpoint'у (каждому хосту — свой движок).
    private sealed class RoutingFactory(StubEngine[] engines) : DockerEngineFactory
    {
        private int _next;

        public override IDockerEngine Create(string endpoint, string? hostAlias = null)
            => engines[System.Math.Min(System.Threading.Interlocked.Increment(ref _next) - 1, engines.Length - 1)];
    }

    // AAA: Swarm — Env доезжает до ServiceSpec.TaskTemplate (ContainerSpec).
    [Fact]
    public async Task Swarm_EnsureNode_EnvДоезжаетДоServiceTemplate()
    {
        // Arrange — Swarm-драйвер с движком-дублёром (ListNodes → h1).
        var engine = new StubEngine();
        var driver = new SwarmClusterDriver("fake://manager", new StubFactory(engine));

        // Act
        var result = await driver.EnsureNodeAsync(Spec(SampleEnv), CancellationToken.None);

        // Assert — env в Template сервиса; имя vwk-c1-node1; без Binds.
        result.IsSuccess.Should().BeTrue();
        engine.CreatedService!.Name.Should().Be("vwk-c1-node1");
        engine.CreatedService.Template.Env.Should().BeEquivalentTo(SampleEnv);
        engine.CreatedService.Template.Binds.Should().BeNull();
    }

    // AAA: Swarm — NodeEnvAsync роутится в InspectServiceEnvAsync.
    [Fact]
    public async Task Swarm_NodeEnv_ИзСервиса()
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
