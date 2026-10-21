using FluentAssertions;
using KafkaWorker.Docker.Drivers;
using Shared.Core;
using Shared.Docker;
using Xunit;

namespace KafkaWorker.UnitTests.Docker;

// PlainClusterDriver.EnsureNodeAsync (arch/16 §2.3, K3): идемпотентность —
// ЖИВОЙ контейнер не пересоздаётся; найденный созданным-но-незапущенным
// (держатель клэйма умер между create и start — takeover-путь) доводится
// до running, иначе кластер навсегда недосчитывается брокера.
public class PlainClusterDriverTests
{
    // Компактный мок движка: list-ответы заготовками, вызовы записываются.
    private sealed class FakeEngine : IDockerEngine
    {
        public readonly List<string> Calls = [];
        public List<DockerContainer> Containers = [];

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public Task<Result> PingAsync(CancellationToken ct) => Task.FromResult(Result.Success());

        // Names — без ведущего "/", как чистит реальный движок (DockerEngine).
        public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(string namePrefix, bool all, CancellationToken ct)
        {
            Calls.Add($"list:{namePrefix}");
            return Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success(
                Containers.Where(c => c.Names.Any(n => n.StartsWith(namePrefix, StringComparison.Ordinal))).ToList()));
        }

        public Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct)
            => Task.FromResult(Result<DockerContainerInspect>.Success(
                new DockerContainerInspect(id, id, [], [], [])));

        public Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct)
            => Task.FromResult(Result<string>.Success(string.Empty));

        public Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct)
        {
            Calls.Add($"create:{name}");
            Containers.Add(new DockerContainer(name, [name], "created", spec.Image));
            return Task.FromResult(Result.Success());
        }

        public Task<Result> StartContainerAsync(string idOrName, CancellationToken ct)
        {
            Calls.Add($"start:{idOrName}");
            var c = Containers.FirstOrDefault(x => x.Names.Contains(idOrName));
            if (c is not null)
                Containers.Remove(c);
            Containers.Add(c! with { State = "running" });
            return Task.FromResult(Result.Success());
        }

        public Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> KillContainerAsync(string idOrName, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct)
            => Task.FromResult(Result<string>.Success(string.Empty));

        public Task<Result> EnsureNetworkAsync(string name, CancellationToken ct)
        {
            Calls.Add($"ensure-network:{name}");
            return Task.FromResult(Result.Success());
        }

        public Task<Result> DeleteNetworkAsync(string name, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> NetworkConnectAsync(string network, string container, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> RemoveVolumeAsync(string name, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<bool>.Success(false));

        public Task<Result> EnsureVolumeAsync(string name, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<IReadOnlyList<string>>> ListVolumesAsync(string namePrefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));

        public Task<Result> EnsureVolumeAsync(string name, string labelKey, string label, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> DeleteVolumeAsync(string name, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerSwarmNode>>.Success([]));

        public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> RemoveServiceAsync(string name, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));

        public Task<Result<IReadOnlyList<DockerService>>> ListServiceRecordsAsync(string namePrefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerService>>.Success([]));

        public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerTask>>.Success([]));

        public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlySet<(string, int)>>.Success(
                new HashSet<(string, int)>()));

        public Task<Result<NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<NodeLimits?>.Success(null));

        public Task<Result<NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<NodeLimits?>.Success(null));

        public Task<Result<IReadOnlyDictionary<string, string>?>>
            InspectContainerEnvAsync(string idOrName, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(null));

        public Task<Result<IReadOnlyDictionary<string, string>?>>
            InspectServiceEnvAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(null));

        public Task<Result<IReadOnlyList<string>?>>
            InspectContainerCmdAsync(string idOrName, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>?>.Success(null));

        public Task<Result<IReadOnlyList<string>?>>
            InspectServiceCmdAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>?>.Success(null));

        public Task<Result<DockerNodeEndpoint?>>
            InspectNodeEndpointAsync(string name, int containerPort, CancellationToken ct)
            => Task.FromResult(Result<DockerNodeEndpoint?>.Success(null));
    }

    private sealed class FakeFactory(IDockerEngine engine) : DockerEngineFactory
    {
        public override IDockerEngine Create(string endpoint, string? hostAlias = null) => engine;
    }

    private static KafkaNodeSpec Spec(string nodeName = "broker1") => new(
        "shop", nodeName, "h1", 19094, "kafka:dev",
        new Dictionary<string, string>(), CpuCores: null, MemoryBytes: null);

    private static PlainClusterDriver NewDriver(FakeEngine engine)
        => new([new HostEndpoint("h1", "fake://h1")], new FakeFactory(engine));

    [Fact]
    public async Task EnsureNode_ExistingCreatedContainer_StartsWithoutRecreate()
    {
        // Arrange — контейнер ноды существует в статусе created (умерший
        // держатель клэйма успел create, но не start)
        var engine = new FakeEngine
        {
            Containers = [new DockerContainer("c1", ["kfw-shop-broker1"], "created", "kafka:dev")],
        };
        var driver = NewDriver(engine);

        // Act — ensure выжившего инстанса.
        var ensured = await driver.EnsureNodeAsync(Spec(), CancellationToken.None);

        // Assert — контейнер ДОВЕДЁН до running (start), пересоздания нет.
        ensured.IsSuccess.Should().BeTrue();
        engine.Calls.Should().ContainSingle(c => c.StartsWith("start:"),
            "created container must be started — takeover finishes node bring-up");
        engine.Calls.Should().NotContain(c => c.StartsWith("create:"),
            "existing container is not recreated (K3)");
        engine.Containers.Should().ContainSingle().Which.State.Should().Be("running");
    }

    [Fact]
    public async Task EnsureNode_ExistingRunningContainer_Nop()
    {
        // Arrange — живая нода.
        var engine = new FakeEngine
        {
            Containers = [new DockerContainer("c1", ["kfw-shop-broker1"], "running", "kafka:dev")],
        };
        var driver = NewDriver(engine);

        // Act.
        var ensured = await driver.EnsureNodeAsync(Spec(), CancellationToken.None);

        // Assert — ни create, ни start: идемпотентность живого (K3).
        ensured.IsSuccess.Should().BeTrue();
        engine.Calls.Should().NotContain(c => c.StartsWith("create:")).And.NotContain(c => c.StartsWith("start:"));
    }

    [Fact]
    public async Task EnsureNode_NoContainer_CreatesAndStarts()
    {
        // Arrange — ноды нет.
        var engine = new FakeEngine();
        var driver = NewDriver(engine);

        // Act.
        var ensured = await driver.EnsureNodeAsync(Spec(), CancellationToken.None);

        // Assert — полный путь: create + start.
        ensured.IsSuccess.Should().BeTrue();
        engine.Calls.Should().ContainInOrder("ensure-network:kfw-net-shop", "create:kfw-shop-broker1", "start:kfw-shop-broker1");
    }
}
