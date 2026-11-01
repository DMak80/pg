using Shared.Etcd.Client;

namespace EtcdWorker.Etcd;

/// <summary>Разбор листов /etcd/nodes/&lt;k&gt;/{state,placed_on} (arch/25 §5):
/// Kv[] префикса → узлы; толерантно (битые номера/чужие листы — игнор,
/// значения — raw-строки с обрезкой кавычек JSON-строк).</summary>
public static class EtcdNodesParser
{
    public sealed record ManagedNodeState(int K, string? State, string? PlacedOn);

    public static IReadOnlyList<ManagedNodeState> Parse(IReadOnlyList<Kv> kvs)
    {
        var nodes = new Dictionary<int, ManagedNodeState>();
        foreach (var kv in kvs)
        {
            // Схема: /etcd/nodes/<k>/<leaf> (5 сегментов).
            var segments = kv.Key.Split('/');
            if (segments.Length != 5 || segments[1] != "etcd" || segments[2] != "nodes")
                continue;
            if (!int.TryParse(segments[3], out var k) || k <= 0)
                continue;
            var value = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim().Trim('"');
            var leaf = segments[4];
            // Узел рождается только листами state/placed_on — unknown-лист
            // (например, /etcd/nodes/1/usage) не ошибка и узла НЕ создаёт.
            var current = nodes.TryGetValue(k, out var existing)
                ? existing
                : new ManagedNodeState(k, null, null);
            switch (leaf)
            {
                case "state":
                    nodes[k] = current with { State = value };
                    break;
                case "placed_on":
                    nodes[k] = current with { PlacedOn = value };
                    break;
            }
        }

        return nodes.Values.OrderBy(n => n.K).ToList();
    }
}
