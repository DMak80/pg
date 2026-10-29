using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;
using PgWorker.Docker.Drivers;

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

    // t25: legacy wal-контейнеры pgw-backup-wal-* — карта живых объектов для
    // санитарного сноса + журнал удалённых имён; ensure/list/logs упразднены
    // (приёмник живёт в контейнере ноды, отдельных агентов больше нет).
    public readonly List<string> RemovedBackupAgents = [];
    public List<DockerContainer> BackupAgentObjects = [];

    // t25: число вызовов санитарного сноса (тик WalStreamProcess — один на тик
    // кластера; повторный тик по чистому листингу — no-op).
    public int RemoveBackupAgentsCalls;

    public Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)
    {
        RemoveBackupAgentsCalls++;
        foreach (var name in LegacyAgentNamesOf(cluster, shard))
        {
            RemovedBackupAgents.Add(name);
            BackupAgentObjects.RemoveAll(c => c.Names.Any(n => n.TrimStart('/') == name));
        }

        return Task.FromResult(Result.Success());
    }

    // Живые legacy агенты кластера (shard=null → все): имена без ведущего "/";
    // префикс pgw-backup-wal-<C>- — строкой (t25: агентов-контейнеров больше нет);
    // матчинг шарда как в драйвере (t27): старый формат <X> ИЛИ per-node <X>-<нода>.
    private List<string> LegacyAgentNamesOf(string cluster, string? shard)
    {
        var prefix = $"pgw-backup-wal-{cluster}-";
        return BackupAgentObjects
            .SelectMany(c => c.Names)
            .Select(n => n.TrimStart('/'))
            .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
            .Where(n => shard is null || AgentShardOf(n[prefix.Length..]) == shard)
            .Distinct()
            .ToList();

        static string AgentShardOf(string tail)
            => tail.Split('-')[0];
    }

    public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<HostInfo>>.Success(
            (IReadOnlyList<HostInfo>)[new HostInfo("h1", 0), new HostInfo("h2", 0)]));

    public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct)
        => Task.FromResult(Result<IReadOnlySet<(string, int)>>.Success(BusyPorts));

    // t22: restPassword каждого EnsureNode-вызова (окно ротации не расширяется).
    public readonly Dictionary<string, string> EnsuredRestPasswords = new();

    public Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
        InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, PgTuneResult? tuning,
        bool syncStrict, string restPassword, CancellationToken ct)
    {
        EnsuredNodes.Add($"{topology.Shard}/{nodeName}");
        NodeObjects.Add($"pgw-{topology.Cluster}-{topology.Shard}-{nodeName}");
        EnsuredRestPasswords[$"{topology.Shard}/{nodeName}"] = restPassword;
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

    // Инспекция усыновления (adopt-repair T3): фиксированная карта находок.
    public IReadOnlyDictionary<string, DiscoveredNode> InspectResult { get; set; }
        = new Dictionary<string, DiscoveredNode>();

    public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
        string cluster, IReadOnlyCollection<string> nodeNames, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyDictionary<string, DiscoveredNode>>.Success(
            (IReadOnlyDictionary<string, DiscoveredNode>)InspectResult
                .Where(p => nodeNames.Contains(p.Key))
                .ToDictionary(p => p.Key, p => p.Value)));

        public Task<Result<IReadOnlyDictionary<string, string>>> InspectNodeEnvAsync(
        string cluster, string shard, string nodeName, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success(
            (IReadOnlyDictionary<string, string>)new Dictionary<string, string>()));

public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<string>>.Success(
            (IReadOnlyList<string>)NodeObjects));
}
