using System.Globalization;
using System.Text.Json;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;

namespace ValkeyWorker.Etcd.Parsing;

// Парсер контроль-плейна /valkey/clusters/ в доменную модель ValkeyWorker
// (arch/20 §2/§5; порт стиля KafkaSnapshotParser). Чистая функция Kv[] →
// модель: битые значения не бросают исключений, а попадают в parseErrors;
// неизвестные ключи — в unknownKeys (arch/20 §5).
public static class ValkeySnapshotParser
{
    private sealed class NodeAcc(string name)
    {
        public readonly string Name = name;
        public string? State;
        public string? ResourcesRaw;
    }

    private sealed class ClusterAcc(string name)
    {
        public readonly string Name = name;
        public string? ConfigRaw;
        public string? Endpoints;
        public string? AppUser;
        public string? AppPassword;
        public string? AdminUser;
        public string? AdminPassword;
        public readonly Dictionary<string, NodeAcc> Nodes = [];
        public readonly List<string> Errors = [];
        public readonly List<string> UnknownKeys = [];
    }

    // kvs префикса /valkey/clusters/ → снапшот (кластеры + неизвестные + ошибки).
    public static Result<ParsedValkeySnapshot> Parse(IReadOnlyList<Kv> kvs)
    {
        var accs = new Dictionary<string, ClusterAcc>();
        var unknown = new List<string>();

        foreach (var kv in kvs)
        {
            // "/valkey/clusters/<C>/leaf…" → ["", "valkey", "clusters", <C>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 5 || segments[1] != "valkey" || segments[2] != "clusters"
                || segments[3].Length == 0)
            {
                continue; // чужой префикс (в т.ч. /pgworker/, /valkeyworker/) — не наша забота
            }

            var acc = GetOrAdd(accs, segments[3], static name => new ClusterAcc(name));
            switch (segments[4])
            {
                case "config" when segments.Length == 5:
                    acc.ConfigRaw = kv.Value;
                    break;

                case "endpoints" when segments.Length == 5:
                    acc.Endpoints = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                case "app_user" when segments.Length == 5:
                    acc.AppUser = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                case "app_password" when segments.Length == 5:
                    acc.AppPassword = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                case "admin_user" when segments.Length == 5:
                    acc.AdminUser = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                case "admin_password" when segments.Length == 5:
                    acc.AdminPassword = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                case "nodes" when segments.Length == 7
                    && segments[5].Length > 0
                    && segments[6] is "state" or "resources":
                {
                    var node = GetOrAdd(acc.Nodes, segments[5], static name => new NodeAcc(name));
                    if (segments[6] == "state")
                    {
                        node.State = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    }
                    else
                    {
                        node.ResourcesRaw = kv.Value;
                    }

                    break;
                }

                default:
                    // система развивается — неизвестный ключ не ошибка (arch/20 §5):
                    // имя ключа в unknownKeys (длина списка = счётчик).
                    acc.UnknownKeys.Add(kv.Key);
                    unknown.Add(kv.Key);
                    break;
            }
        }

        var clusters = accs.Values
            .OrderBy(c => c.Name, StringComparer.Ordinal)
            .Select(BuildCluster)
            .ToList();

        return Result<ParsedValkeySnapshot>.Success(
            new ParsedValkeySnapshot(clusters, unknown,
                [.. clusters.SelectMany(c => c.ParseErrors)]));
    }

    private static ValkeyClusterSnapshot BuildCluster(ClusterAcc acc)
        => new(
            acc.Name,
            ParseConfig(acc.Name, acc.ConfigRaw, acc.Errors),
            acc.Nodes
                .OrderBy(pair => pair.Key, StringComparer.Ordinal)
                .ToDictionary(
                    pair => pair.Key,
                    pair => BuildNode(pair.Value, acc.Errors)),
            acc.Endpoints,
            acc.AppUser,
            acc.AppPassword,
            acc.AdminUser,
            acc.AdminPassword,
            acc.UnknownKeys,
            acc.Errors);

    private static ValkeyClusterConfig? ParseConfig(string cluster, string? raw, List<string> errors)
    {
        if (raw is null)
            return null; // нет ключа — не ошибка (классификатор трактует как Skip)

        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            return new ValkeyClusterConfig(
                ReadInt(root, "nodes") ?? 0,
                ReadLong(root, "maxmemory_bytes") ?? 0,
                ReadString(root, "maxmemory_policy") ?? "",
                ReadLong(root, "created_unix") ?? 0,
                ReadString(root, "state")); // отсутствие state = Active (arch/20 §2)
        }
        catch (JsonException)
        {
            errors.Add($"/valkey/clusters/{cluster}/config: битый JSON config");
            return null;
        }
    }

    private static ValkeyNodeSnapshot BuildNode(NodeAcc node, List<string> errors)
    {
        ValkeyResources? resources = null;
        if (node.ResourcesRaw is not null)
        {
            resources = ParseResources(node.ResourcesRaw);
            if (resources is null)
                errors.Add($"/valkey/clusters/-/nodes/{node.Name}/resources: битый JSON");
        }

        return new ValkeyNodeSnapshot(node.Name, node.State, resources);
    }

    // Raw-строки как в etcd (arch/20 §5): парсинг в лимиты — ProcessCommon.
    private static ValkeyResources? ParseResources(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return null;

            return new ValkeyResources(
                ReadString(root, "cpu"),
                ReadString(root, "mem"),
                ReadString(root, "disk"));
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static int? ReadInt(JsonElement root, string field)
        => root.TryGetProperty(field, out var v) && v.ValueKind == JsonValueKind.Number
            && v.TryGetInt32(out var i) ? i : null;

    private static long? ReadLong(JsonElement root, string field)
        => root.TryGetProperty(field, out var v) && v.ValueKind == JsonValueKind.Number
            && v.TryGetInt64(out var l) ? l : null;

    private static string? ReadString(JsonElement root, string field)
        => root.TryGetProperty(field, out var v) && v.ValueKind == JsonValueKind.String
            ? v.GetString() : null;

    private static T GetOrAdd<TKey, T>(Dictionary<TKey, T> dict, TKey key, Func<TKey, T> factory)
        where TKey : notnull
    {
        if (!dict.TryGetValue(key, out var value))
        {
            value = factory(key);
            dict[key] = value;
        }

        return value;
    }
}
