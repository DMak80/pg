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

    public static NodeAddress MakeAddress(string host, int basePort)
        => new(host, new NodePorts(basePort, basePort + 3000, basePort + 1500));

    public static string KeyOf(NodePlacement p) => $"{p.Group}/{p.Node}";
}
