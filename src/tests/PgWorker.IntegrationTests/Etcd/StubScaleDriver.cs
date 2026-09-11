using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;

namespace PgWorker.IntegrationTests.Etcd;

// Мок docker-драйвера для контрактных тестов scale (t06 §8): записывает
// удаления/создания, отдаёт фиксированные хосты/объекты — etcd-сторона реальна.
public sealed class StubScaleDriver : IClusterDriver
{
    // Plain-семантика: инспект отражает факт running-процесса (arch/14 §5 C).
    public bool SupportsRunningInspection { get; init; } = true;

    // t02: стабы джобов бэкапов — контрактным тестам scale движки не нужны.
    public IDockerEngine? EngineFor(string host) => null;

    public Task<Result> RemoveBackupJobsAsync(string cluster, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result> RemoveRestoreJobsAsync(string cluster, string shard, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public readonly List<string> EnsuredNodes = [];
    public readonly List<string> RemovedNodes = [];
    public List<string> NodeObjects = [];
    public IReadOnlySet<(string Host, int Port)> BusyPorts = new HashSet<(string, int)>();

    // t03: контейнеры WAL-агентов бэкапов — фиксация ensure/remove + мутабельная
    // карта живых объектов (супервиз-тесты WalStreamProcess подменяют State).
    public readonly List<string> EnsuredBackupAgents = [];
    public readonly List<string> RemovedBackupAgents = [];
    public List<DockerContainer> BackupAgentObjects = [];

    // Спеки агентов, отданные драйверу (контракт WalStreamProcess: Network=null —
    // сеть нод проставляет реальный драйвер; ревью Ф7 №1).
    public List<ContainerSpec> EnsuredAgentSpecs = [];

    public Task<Result> EnsureBackupAgentAsync(
        string cluster, string shard, ContainerSpec spec, string host, CancellationToken ct)
    {
        var name = BackupAgentNames.Container(cluster, shard);
        EnsuredBackupAgents.Add(name);
        EnsuredAgentSpecs.Add(spec);
        if (BackupAgentObjects.All(c => !c.Names.Contains("/" + name)))
            BackupAgentObjects.Add(new DockerContainer($"id-{name}", ["/" + name], "running", spec.Image));
        return Task.FromResult(Result.Success());
    }

    public Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)
    {
        foreach (var name in BackupAgentNamesOf(cluster, shard))
        {
            RemovedBackupAgents.Add(name);
            BackupAgentObjects.RemoveAll(c => c.Names.Any(n => n.TrimStart('/') == name));
        }

        return Task.FromResult(Result.Success());
    }

    public Task<Result<IReadOnlyList<DockerContainer>>> ListBackupAgentsAsync(
        string cluster, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success(
            (IReadOnlyList<DockerContainer>)BackupAgentObjects
                .Where(c => c.Names.Any(n => n.TrimStart('/').StartsWith(
                    BackupAgentNames.Prefix(cluster), StringComparison.Ordinal)))
                .ToList()));

    // Живые агенты кластера (shard=null → все): имена без ведущего "/".
    private List<string> BackupAgentNamesOf(string cluster, string? shard)
    {
        var prefix = BackupAgentNames.Prefix(cluster);
        return BackupAgentObjects
            .SelectMany(c => c.Names)
            .Select(n => n.TrimStart('/'))
            .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
            .Where(n => shard is null || n == BackupAgentNames.Container(cluster, shard))
            .Distinct()
            .ToList();
    }

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

    // Д3 вне контрактных сценариев scale (t06 §8): утрата не доказана — не лечим.
    public Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct)
        => Task.FromResult(Result<DataPresence>.Success(DataPresence.Unknown));

    public Task<Result<string>> ExecContainerAsync(string containerName, IReadOnlyList<string> cmd, CancellationToken ct)
        => Task.FromResult(Result<string>.Success(string.Empty));

    public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<string>>.Success(
            (IReadOnlyList<string>)NodeObjects));
}
