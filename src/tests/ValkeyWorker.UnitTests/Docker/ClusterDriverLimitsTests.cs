using FluentAssertions;
using ValkeyWorker.Core.Model;
// В ассертах — доменная запись; docker-факт — с квалификатором Shared.Docker.
using NodeLimits = ValkeyWorker.Core.Model.NodeLimits;
using ValkeyWorker.Docker.Drivers;
using Xunit;

namespace ValkeyWorker.UnitTests.Docker;

// Конверсия лимитов в драйвере (t07 §4.6.2, шаг 6.6 плана): движок возвращает
// docker-факт Shared.Docker.NodeLimits(long NanoCpus, long MemoryBytes),
// доменная запись NodeLimits(decimal? CpuCores, long? MemoryBytes). 0 = без
// лимита → null (семантика значений прежнего vwk-движка — сверено с ассертами
// ResourcesAutorecreateTests/SupervisionTests).
public class ClusterDriverLimitsTests
{
    // Движок-дублёр: отдаёт заготовленный docker-факт лимитов.
    private sealed class StubEngine(Shared.Docker.NodeLimits? limits) : IDockerEngine
    {
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public Task<Result<Shared.Docker.NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<Shared.Docker.NodeLimits?>.Success(limits));

        public Task<Result<Shared.Docker.NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<Shared.Docker.NodeLimits?>.Success(limits));

        // Остальные члены union — вне сценария теста.
        private static NotImplementedException NotUsed() => new("движок-дублёр: маршрут вне сценария теста");

        public Task<Result> PingAsync(CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(string namePrefix, bool all, CancellationToken ct) => throw NotUsed();
        public Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct) => throw NotUsed();
        public Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct) => throw NotUsed();
        public Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> StartContainerAsync(string idOrName, CancellationToken ct) => throw NotUsed();
        public Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct) => throw NotUsed();
        public Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct) => throw NotUsed();
        public Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct) => throw NotUsed();
        public Task<Result> EnsureNetworkAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> DeleteNetworkAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> RemoveVolumeAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> EnsureVolumeAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> DeleteVolumeAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result> PutVolumeArchiveAsync(string name, byte[] tar, string image, CancellationToken ct) => throw NotUsed();
        public Task<Result<byte[]?>> GetVolumeArchiveAsync(string name, string image, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct) => throw NotUsed();
        public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct) => throw NotUsed();
        public Task<Result> RemoveServiceAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectContainerEnvAsync(string idOrName, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectServiceEnvAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<string>?>> InspectContainerCmdAsync(string idOrName, CancellationToken ct) => throw NotUsed();
        public Task<Result<IReadOnlyList<string>?>> InspectServiceCmdAsync(string name, CancellationToken ct) => throw NotUsed();
        public Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, int containerPort, CancellationToken ct) => throw NotUsed();
    }

    // Фабрика с единственным движком для любого endpoint'а.
    private sealed class StubFactory(Shared.Docker.NodeLimits? limits) : DockerEngineFactory
    {
        public override IDockerEngine Create(string endpoint, string? hostAlias = null)
            => new StubEngine(limits);
    }

    [Theory]
    [InlineData(1_500_000_000, 2_000_000_000)]
    public async Task Plain_NodeResources_ConvertsNanoCpusToCores(long nanoCpus, long memoryBytes)
    {
        // Arrange — docker-факт: 1.5 ядра / 2 GiB
        var driver = new PlainClusterDriver(
            [new HostEndpoint("h1", "fake://h1")],
            new StubFactory(new Shared.Docker.NodeLimits(nanoCpus, memoryBytes)));

        // Act
        var result = await driver.NodeResourcesAsync("c1", "node1", CancellationToken.None);

        // Assert: доменная запись — decimal? ядра + байты
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEquivalentTo(new NodeLimits(1.5m, memoryBytes));
    }

    [Fact]
    public async Task Plain_NodeResources_ZeroMeansUnlimited_Nulls()
    {
        // Arrange — 0/0 = docker-факт «без лимита»
        var driver = new PlainClusterDriver(
            [new HostEndpoint("h1", "fake://h1")],
            new StubFactory(new Shared.Docker.NodeLimits(0, 0)));

        // Act
        var result = await driver.NodeResourcesAsync("c1", "node1", CancellationToken.None);

        // Assert: null/null — прежняя семантика vwk-движка
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEquivalentTo(new NodeLimits(null, null));
    }

    [Fact]
    public async Task Plain_NodeResources_NoObject_Null()
    {
        // Arrange — объекта нет (движок отдал null — факта нет)
        var driver = new PlainClusterDriver(
            [new HostEndpoint("h1", "fake://h1")],
            new StubFactory(null));

        // Act
        var result = await driver.NodeResourcesAsync("c1", "node1", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeNull();
    }

    [Fact]
    public async Task Swarm_NodeResources_ConvertsNanoCpusToCores()
    {
        // Arrange — swarm: инспекция сервиса, конверсия та же
        var driver = new SwarmClusterDriver(
            "fake://manager",
            new StubFactory(new Shared.Docker.NodeLimits(500_000_000, 1_000_000_000)));

        // Act
        var result = await driver.NodeResourcesAsync("c1", "node1", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEquivalentTo(new NodeLimits(0.5m, 1_000_000_000));
    }
}
