using System.Globalization;
using System.Text.Json;
using S3Worker.Core.Model;
using Shared.Etcd.Client;

namespace S3Worker.Etcd.Parsing;

// Парсер контроль-плейна /s3/clusters/ в доменную модель S3Worker (arch/23 §2;
// порт стиля ValkeySnapshotParser). Чистая функция Kv[] → модель: битые значения
// не бросают исключений, а попадают в parseError-поля; неизвестные ключи
// (в т.ч. connectivity/sync — t33) — в unknownKeys (arch/23 §2/§9).
public static class S3SnapshotParser
{
    private sealed class NodeAcc(string name)
    {
        public readonly string Name = name;
        public string? State;
        public string? PlacedOn;
        public string? PlacementRaw;
        public string? ResourcesRaw;
        public string? UsageRaw;
    }

    private sealed class ClusterAcc(string name)
    {
        public readonly string Name = name;
        public string? ConfigRaw;
        public string? Endpoints;
        public readonly Dictionary<string, NodeAcc> Nodes = [];
        public readonly List<string> UnknownKeys = [];
    }

    // kvs префикса /s3/clusters/ → снапшот домена (кластеры + неизвестные).
    public static Result<S3DomainSnapshot> Parse(IReadOnlyList<Kv> kvs)
    {
        var accs = new Dictionary<string, ClusterAcc>();
        var unknown = new List<string>();

        foreach (var kv in kvs)
        {
            // "/s3/clusters/<C>/leaf…" → ["", "s3", "clusters", <C>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 2 || segments[1] != "s3")
            {
                continue; // чужой префикс (в т.ч. /s3worker/, /pgworker/) — не домен /s3/
            }

            // Ключи домена /s3/ вне схемы кластеров (arch/23 §2: unknownKeys —
            // не ошибка): /s3/whatever, /s3/clusters/<C>/unknown-t33, ключи t33.
            if (segments.Length < 4 || segments[2] != "clusters" || segments[3].Length == 0)
            {
                unknown.Add(kv.Key);
                continue;
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

                case "nodes" when segments.Length == 7
                    && segments[5].Length > 0
                    && segments[6] is "placement" or "resources" or "state" or "placed_on" or "usage":
                {
                    var node = GetOrAdd(acc.Nodes, segments[5], static name => new NodeAcc(name));
                    switch (segments[6])
                    {
                        case "state":
                            node.State = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                            break;
                        case "placed_on":
                            node.PlacedOn = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                            break;
                        case "placement":
                            node.PlacementRaw = kv.Value;
                            break;
                        case "resources":
                            node.ResourcesRaw = kv.Value;
                            break;
                        case "usage":
                            node.UsageRaw = kv.Value;
                            break;
                    }

                    break;
                }

                default:
                    // система развивается — неизвестный ключ не ошибка (arch/23 §2):
                    // connectivity/sync (t33) и прочие — имя в unknownKeys (счётчик).
                    acc.UnknownKeys.Add(kv.Key);
                    unknown.Add(kv.Key);
                    break;
            }
        }

        var clusters = accs.Values
            .OrderBy(c => c.Name, StringComparer.Ordinal)
            .Select(BuildCluster)
            .ToList();

        return Result<S3DomainSnapshot>.Success(new S3DomainSnapshot(clusters, unknown));
    }

    private static S3ClusterSnapshot BuildCluster(ClusterAcc acc)
    {
        // Битый config — parseError кластера (config → null: классификатор
        // трактует отсутствие как Skip, тик не падает — arch/23 §9).
        string? clusterError = null;
        var config = ParseConfig(acc.ConfigRaw);
        if (acc.ConfigRaw is not null && config is null)
            clusterError = $"/s3/clusters/{acc.Name}/config: битый JSON config";

        // Ноды по номеру node<k> (Ordinal — однозначность в пределах 1..9).
        var nodes = acc.Nodes
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => BuildNode(acc.Name, pair.Value))
            .ToList();

        return new S3ClusterSnapshot(acc.Name, config, nodes, acc.Endpoints, clusterError);
    }

    private static S3NodeSnapshot BuildNode(string cluster, NodeAcc node)
    {
        var errors = new List<string>();

        var placement = ParseJson<S3NodePlacement>(node.PlacementRaw, (root, err) =>
            new S3NodePlacement(
                ReadString(root, "docker_host"),
                ReadString(root, "swarm_node"),
                ReadLabels(root, "swarm_labels", err)));
        if (node.PlacementRaw is not null && placement is null)
            errors.Add($"placement: битый JSON");

        var resources = ParseJson<S3NodeResources>(node.ResourcesRaw, (root, _) =>
            new S3NodeResources(
                ReadString(root, "cpu"),
                ReadString(root, "mem"),
                ReadString(root, "disk")));
        if (node.ResourcesRaw is not null && resources is null)
            errors.Add($"resources: битый JSON");

        var usage = ParseJson<S3NodeUsage>(node.UsageRaw, (root, _) => new S3NodeUsage(
            ReadLong(root, "used_bytes") ?? 0,
            ReadLong(root, "total_bytes") ?? 0,
            ReadLong(root, "updated_unix") ?? 0));
        if (node.UsageRaw is not null && usage is null)
            errors.Add($"usage: битый JSON");

        return new S3NodeSnapshot(
            node.Name,
            node.State,
            node.PlacedOn,
            placement,
            resources,
            usage,
            errors.Count > 0 ? $"/s3/clusters/{cluster}/nodes/{node.Name}: {string.Join("; ", errors)}" : null);
    }

    private static S3ClusterConfig? ParseConfig(string? raw)
    {
        if (raw is null)
            return null; // нет ключа — не ошибка (классификатор: Skip)

        return ParseJson<S3ClusterConfig>(raw, (root, _) => new S3ClusterConfig(
            ReadString(root, "platform"),
            ReadInt(root, "nodes"),
            ReadString(root, "secret_profile"),
            ReadLong(root, "created_unix"),
            ReadString(root, "state"))); // отсутствие state = Active (arch/23 §2)
    }

    // Разбор JSON-значения с проекцией root → T; JsonException/не-объект → null
    // (parseError без падения тика — arch/23 §9).
    private static T? ParseJson<T>(string? raw, Func<JsonElement, List<string>, T> project) where T : class
    {
        if (raw is null)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return null;
            var err = new List<string>();
            var value = project(root, err);
            return err.Count > 0 ? null : value;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    // swarm_labels: словарь строк (значения не-строки — битый placement).
    private static IReadOnlyDictionary<string, string>? ReadLabels(JsonElement root, string field, List<string> err)
    {
        if (!root.TryGetProperty(field, out var v))
            return null;
        if (v.ValueKind != JsonValueKind.Object)
        {
            err.Add(field);
            return null;
        }

        var labels = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var label in v.EnumerateObject())
        {
            if (label.Value.ValueKind == JsonValueKind.String)
                labels[label.Name] = label.Value.GetString()!;
            else
                err.Add(field);
        }

        return labels;
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
