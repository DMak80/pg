using KafkaWorker.Core.Model;
using Shared.Core.Planning;

/// <summary>Kfw-инстанс обобщённых планировщиков (t09): одна группа на кластер,
/// адрес = один client-порт, ключ результата — имя ноды.</summary>
public static class KfwPlanning
{
    public static IReadOnlyList<NodeGroup> Group(string cluster, IReadOnlyList<string> nodes)
        => [new(cluster, nodes)];

    public static IReadOnlyList<int> PortsOf(NodeAddress a) => [a.ClientPort];

    public static string HostOf(NodeAddress a) => a.Host;

    public static NodeAddress MakeAddress(string host, int port) => new(host, port);

    public static string KeyOf(NodePlacement p) => p.Node;
}
