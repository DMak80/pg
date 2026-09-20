using ValkeyWorker.Core.Model;
using ValkeyWorker.Docker.Drivers;

namespace ValkeyWorker.UnitTests.Docker;

// SwarmClusterDriver TLS-volume (t06-ревью): named volume нод-локален — архив
// сертов обязан уходить в engine НОДЫ размещения (таблица Hosts по имени ноды),
// не в manager. Ноды нет в таблице (однонодовый контур) — manager.
public class SwarmClusterDriverTlsTests
{
    [Fact]
    public void PutTlsArchive_RoutesToPlacementNodeEngine()
    {
        // Arrange — таблица Hosts: node-a endpoint A, node-b endpoint B
        var factory = new RoutingFactory();
        var driver = new SwarmClusterDriver("tcp://manager", factory,
        [
            new HostEndpoint("node-a", "tcp://node-a:2375"),
            new HostEndpoint("node-b", "tcp://node-b:2375"),
        ]);

        // Act
        driver.PutTlsArchiveAsync("c1", "node-a", [], "img", TestContext.Current.CancellationToken);

        // Assert — запись ушла в engine ноды node-a, не в manager
        factory.Engines["tcp://node-a:2375"].PutCalls.Should().Be(1);
        factory.Engines["tcp://manager"].PutCalls.Should().Be(0);
        factory.Engines["tcp://node-b:2375"].PutCalls.Should().Be(0);
    }

    [Fact]
    public void GetTlsArchive_RoutesToPlacementNodeEngine()
    {
        // Arrange
        var factory = new RoutingFactory();
        var driver = new SwarmClusterDriver("tcp://manager", factory,
        [
            new HostEndpoint("node-a", "tcp://node-a:2375"),
        ]);

        // Act
        driver.GetTlsArchiveAsync("c1", "node-a", "img", TestContext.Current.CancellationToken);

        // Assert — чтение с ноды размещения
        factory.Engines["tcp://node-a:2375"].GetCalls.Should().Be(1);
        factory.Engines["tcp://manager"].GetCalls.Should().Be(0);
    }

    [Fact]
    public void TlsArchive_UnknownHost_FallsBackToManager()
    {
        // Arrange — однонодовый контур: ноды в таблице нет
        var factory = new RoutingFactory();
        var driver = new SwarmClusterDriver("tcp://manager", factory, []);

        // Act
        driver.PutTlsArchiveAsync("c1", "docker-desktop", [], "img", TestContext.Current.CancellationToken);
        driver.GetTlsArchiveAsync("c1", "docker-desktop", "img", TestContext.Current.CancellationToken);

        // Assert — поведение не меняется: manager engine
        factory.Engines["tcp://manager"].PutCalls.Should().Be(1);
        factory.Engines["tcp://manager"].GetCalls.Should().Be(1);
    }

    [Fact]
    public async Task RemoveTlsVolume_DeletesOnAllNodesAndManager()
    {
        // Arrange — таблица Hosts: две ноды + manager; объём нод-локален,
        // демонтаж X1 обязан снять его на КАЖДОМ engine (t06-ревью)
        var factory = new RoutingFactory();
        var driver = new SwarmClusterDriver("tcp://manager", factory,
        [
            new HostEndpoint("node-a", "tcp://node-a:2375"),
            new HostEndpoint("node-b", "tcp://node-b:2375"),
        ]);

        // Act
        var removed = await driver.RemoveTlsVolumeAsync("c1", TestContext.Current.CancellationToken);

        // Assert — DELETE дошёл до всех нод таблицы и manager (404 = успех)
        removed.IsSuccess.Should().BeTrue();
        factory.Engines["tcp://node-a:2375"].DeleteCalls.Should().Be(1);
        factory.Engines["tcp://node-b:2375"].DeleteCalls.Should().Be(1);
        factory.Engines["tcp://manager"].DeleteCalls.Should().Be(1);
    }

    [Fact]
    public async Task EnsureTlsVolume_EnsuresOnPlacementNodeEngine()
    {
        // Arrange
        var factory = new RoutingFactory();
        var driver = new SwarmClusterDriver("tcp://manager", factory,
        [
            new HostEndpoint("node-a", "tcp://node-a:2375"),
        ]);

        // Act
        await driver.EnsureTlsVolumeAsync("c1", "node-a", TestContext.Current.CancellationToken);

        // Assert — ensure на ноде размещения (та же, куда Put пишет серты)
        factory.Engines["tcp://node-a:2375"].EnsureCalls.Should().Be(1);
        factory.Engines["tcp://manager"].EnsureCalls.Should().Be(0);
    }

    // Фабрика, отдающая по endpoint'у отдельный записывающий движок.
    private sealed class RoutingFactory : DockerEngineFactory
    {
        public Dictionary<string, RecordingEngine> Engines { get; } = new();

        public override IDockerEngine Create(string endpoint, string? hostAlias = null)
        {
            if (!Engines.TryGetValue(endpoint, out var engine))
            {
                engine = new RecordingEngine();
                Engines[endpoint] = engine;
            }

            return engine;
        }
    }

    // Движок-дублёр: считает volume-archive вызовы, остальное — не для теста.
    private sealed class RecordingEngine : IDockerEngine
    {
        public int PutCalls { get; private set; }

        public int GetCalls { get; private set; }

        public int DeleteCalls { get; private set; }

        public int EnsureCalls { get; private set; }

        public Task<Result> PutVolumeArchiveAsync(string name, byte[] tar, string image, CancellationToken ct)
        {
            PutCalls++;
            return Task.FromResult(Result.Success());
        }

        public Task<Result<byte[]?>> GetVolumeArchiveAsync(string name, string image, CancellationToken ct)
        {
            GetCalls++;
            return Task.FromResult(Result<byte[]?>.Success(null));
        }

        public Task<Result> DeleteVolumeAsync(string name, CancellationToken ct)
        {
            DeleteCalls++;
            return Task.FromResult(Result.Success());
        }

        public Task<Result> EnsureVolumeAsync(string name, CancellationToken ct)
        {
            EnsureCalls++;
            return Task.FromResult(Result.Success());
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private static NotImplementedException NotUsed()
            => new("движок-дублёр: маршрут вне сценария теста");

        public Task<Result> PingAsync(CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(string namePrefix, bool all, CancellationToken ct) => throw NotUsed();
        public Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> StartContainerAsync(string idOrName, CancellationToken ct) => throw NotUsed();
        public Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct) => throw NotUsed();
        public Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct) => throw NotUsed();
        public Task<Result> EnsureNetworkAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> DeleteNetworkAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct) => throw NotUsed();
        public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct) => throw NotUsed();
        public Task<Result> RemoveServiceAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct) => throw NotUsed();
        public Task<Result<Shared.Docker.NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<Shared.Docker.NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<string>?>> InspectContainerCmdAsync(string idOrName, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<string>?>> InspectServiceCmdAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, int containerPort, CancellationToken ct) => throw NotUsed();

        // ── union-члены t07 вне сценария теста ──
        public Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct) => throw NotUsed();
        public Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct) => throw NotUsed();
        public Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct) => throw NotUsed();
        public Task<Result> RemoveVolumeAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectContainerEnvAsync(string idOrName, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectServiceEnvAsync(string name, CancellationToken ct) => throw NotUsed();
    }
}
