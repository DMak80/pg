using S3Worker.Core.Model;

/// <summary>S3-инстанс обобщённых планировщиков (порт KfwPlanning): одна группа
/// на кластер, адрес = один S3-порт, ключ результата — имя ноды (arch/23 §2).</summary>
public static class S3Planning
{
    public static IReadOnlyList<NodeGroup> Group(string cluster, IReadOnlyList<string> nodes)
        => [new(cluster, nodes)];

    public static IReadOnlyList<int> PortsOf(NodeAddress a) => [a.S3Port];

    public static string HostOf(NodeAddress a) => a.Host;

    public static NodeAddress MakeAddress(string host, int port) => new(host, port);

    public static string KeyOf(NodePlacement p) => p.Node;
}
