namespace EtcdWorker.Etcd;

/// <summary>Ключи домена /etcd/ (arch/etcdworker/00-etcd-contract.md).</summary>
public static class EtcdDomainKeys
{
    public const string ConfigKey = "/etcd/config";
    public const string NodesPrefix = "/etcd/nodes/";

    public static string StateKey(int k) => $"{NodesPrefix}{k}/state";

    public static string PlacedOnKey(int k) => $"{NodesPrefix}{k}/placed_on";
}

/// <summary>JSON /etcd/config {"nodes":N,"created_unix":T}: serialize +
/// толерантный parse (битый/неполный → null-компоненты; spec §3.3).</summary>
public static class EtcdConfigJson
{
    public sealed record EtcdConfig(int Nodes, long CreatedUnix);

    public static string Serialize(int nodes, long createdUnix)
        => $"{{\"nodes\":{nodes},\"created_unix\":{createdUnix}}}";

    public static EtcdConfig? Parse(string raw)
    {
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(raw);
            var root = doc.RootElement;
            if (root.ValueKind != System.Text.Json.JsonValueKind.Object)
                return null;
            var nodes = root.TryGetProperty("nodes", out var n)
                        && n.ValueKind == System.Text.Json.JsonValueKind.Number
                        && n.TryGetInt32(out var nodesValue)
                ? nodesValue
                : (int?)null;
            var created = root.TryGetProperty("created_unix", out var c)
                          && c.ValueKind == System.Text.Json.JsonValueKind.Number
                          && c.TryGetInt64(out var createdValue)
                ? createdValue
                : (long?)null;
            return nodes is null || created is null ? null : new EtcdConfig(nodes.Value, created.Value);
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }
    }
}
