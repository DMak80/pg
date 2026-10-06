using PgWorker.Core.Model;
using Shared.Core.Planning;

/// <summary>Pg-инстанс обобщённых планировщиков (t09): группы = шарды,
/// адрес = тройка портов pg/patroni/doorman, ключ результата — "shard/node".</summary>
public static class PgPlanning
{
    public static IReadOnlyList<NodeGroup> ToGroups(IReadOnlyList<ShardSpec> shards)
        => shards.Select(s => new NodeGroup(s.Name, s.Nodes.Select(n => n.Name).ToList())).ToList();

    public static IReadOnlyList<int> PortsOf(NodeAddress a) => [a.Ports.Pg, a.Ports.Patroni, a.Ports.Doorman];

    public static string HostOf(NodeAddress a) => a.Host;

    /// <summary>Адрес ноды: последовательная тройка слотов диапазона
    /// (pg / patroni / doorman = base / base+1 / base+2 — arch/14 §2.4 п.2,
    /// t24: схема смещений +3000/+1500 упразднена; диапазон параметризуется
    /// средой — прод per-install, E2E — per-contour окно).</summary>
    public static NodeAddress MakeAddress(string host, int basePort)
        => new(host, new NodePorts(basePort, basePort + 1, basePort + 2));

    public static string KeyOf(NodePlacement p) => $"{p.Group}/{p.Node}";
}
