using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Парсер стендовой топологии /cluster/nodes/<node> → <ip> (lease TTL у нод стенда, arch/02 §2.3).
// Реестр однороден: любые ключи под префиксом — узлы; посторонних форм нет.
public static class StandNodesParser
{
    public static IReadOnlyList<StandNode> Parse(IReadOnlyList<Kv> kvs)
    {
        var nodes = new List<StandNode>();
        foreach (var kv in kvs)
        {
            // "/cluster/nodes/<node>" → ["", "cluster", "nodes", <node>]
            var segments = kv.Key.Split('/');
            if (segments.Length != 4 || segments[1] != "cluster" || segments[2] != "nodes" || segments[3].Length == 0)
                continue;

            var address = kv.Value.Trim();
            nodes.Add(new StandNode(segments[3], address.Length == 0 ? null : address));
        }

        return nodes;
    }
}
