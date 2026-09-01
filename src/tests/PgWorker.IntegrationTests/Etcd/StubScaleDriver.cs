using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;

namespace PgWorker.IntegrationTests.Etcd;

// Мок docker-драйвера для контрактных тестов scale (t06 §8): записывает
// удаления/создания, отдаёт фиксированные хосты/объекты — etcd-сторона реальна.
public sealed class StubScaleDriver : IClusterDriver
{
    public readonly List<string> EnsuredNodes = [];
    public readonly List<string> RemovedNodes = [];
    public List<string> NodeObjects = [];
    public IReadOnlySet<(string Host, int Port)> BusyPorts = new HashSet<(string, int)>();

    public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<HostInfo>>.Success(
            (IReadOnlyList<HostInfo>)[new HostInfo("h1", 0), new HostInfo("h2", 0)]));

    public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct)
        => Task.FromResult(Result<IReadOnlySet<(string, int)>>.Success(BusyPorts));

    public Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
        InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, CancellationToken ct)
    {
        EnsuredNodes.Add($"{topology.Shard}/{nodeName}");
        NodeObjects.Add($"pgw-{topology.Cluster}-{topology.Shard}-{nodeName}");
        return Task.FromResult(Result.Success());
    }

    public Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct)
    {
        RemovedNodes.Add($"{shard}/{nodeName}");
        NodeObjects.RemoveAll(name => name == $"pgw-{cluster}-{shard}-{nodeName}");
        return Task.FromResult(Result.Success());
    }

    public Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result<string>> ExecNodeAsync(
        string cluster, string shard, string node, IReadOnlyList<string> cmd, CancellationToken ct)
        => Task.FromResult(Result<string>.Success(string.Empty));

    // Инспекция усыновления (adopt-repair T3): фиксированная карта находок.
    public IReadOnlyDictionary<string, DiscoveredNode> InspectResult { get; set; }
        = new Dictionary<string, DiscoveredNode>();

    public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
        string cluster, IReadOnlyCollection<string> nodeNames, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyDictionary<string, DiscoveredNode>>.Success(
            (IReadOnlyDictionary<string, DiscoveredNode>)InspectResult
                .Where(p => nodeNames.Contains(p.Key))
                .ToDictionary(p => p.Key, p => p.Value)));

    public Task<Result<string>> ExecContainerAsync(string containerName, IReadOnlyList<string> cmd, CancellationToken ct)
        => Task.FromResult(Result<string>.Success(string.Empty));

    public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<string>>.Success(
            (IReadOnlyList<string>)NodeObjects));
}
