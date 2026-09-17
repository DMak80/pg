using System.Globalization;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора префикса /valkey/clusters/ (arch/02 §11.1).
public sealed record ValkeyClustersParseResult(
    IReadOnlyList<ValkeyClusterInfo> Clusters,
    IReadOnlyList<KeyParseError> Errors,
    int UnknownKeyCount);

// Результат разбора очереди ротаций /valkeyworker/rotations/ (arch/20 §3).
public sealed record ValkeyRotationsParseResult(
    IReadOnlyList<ValkeyRotationTicket> Tickets,
    IReadOnlyList<KeyParseError> Errors);

// Парсер valkey-домена: чистые функции Kv[] → модель, битые значения не бросают
// исключений — порождают KeyParseError (порт KafkaParser; arch/20 §5).
public static class ValkeyParser
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
        public readonly Dictionary<string, NodeAcc> Nodes = [];
    }

    public static ValkeyClustersParseResult ParseClusters(IReadOnlyList<Kv> kvs)
    {
        var errors = new List<KeyParseError>();
        var unknown = 0;
        var accs = new Dictionary<string, ClusterAcc>();

        foreach (var kv in kvs)
        {
            // "/valkey/clusters/<C>/leaf…" → ["", "valkey", "clusters", <C>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 5 || segments[1] != "valkey" || segments[2] != "clusters"
                || segments[3].Length == 0)
            {
                unknown++;
                continue;
            }

            var acc = GetOrAdd(accs, segments[3], static name => new ClusterAcc(name));
            switch (segments[4])
            {
                case "config" when segments.Length == 5:
                    acc.ConfigRaw = kv.Value;
                    break;

                case "endpoints" when segments.Length == 5:
                    // Пустой/пробельный → отсутствует (arch/20 §5).
                    acc.Endpoints = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                // Креды: панель app_* не читает вовсе, admin_* — только refresher в
                // secrets-стор (§4.4); здесь — expected-skip без unknownKeys (02 §11.1).
                case "app_user" or "app_password" or "admin_user" or "admin_password"
                    when segments.Length == 5:
                    break;

                case "nodes" when segments.Length == 7
                    && segments[5].Length > 0
                    && segments[6] is "state" or "resources":
                {
                    var node = GetOrAdd(acc.Nodes, segments[5], static name => new NodeAcc(name));
                    if (segments[6] == "state")
                        node.State = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    else
                        node.ResourcesRaw = kv.Value;
                    break;
                }

                default:
                    // система развивается — неизвестный ключ не ошибка, только счётчик (arch/20 §5)
                    unknown++;
                    break;
            }
        }

        var clusters = accs.Values
            .OrderBy(c => c.Name, StringComparer.Ordinal)
            .Select(acc => BuildCluster(acc, errors))
            .ToList();

        return new ValkeyClustersParseResult(clusters, errors, unknown);
    }

    // Ротации: {"role","requested_unix","requested_by"}; role — raw-строка (толерантно);
    // битый JSON / нет requested_unix → parseError-запись (ключ не трогаем).
    public static ValkeyRotationsParseResult ParseRotations(IReadOnlyList<Kv> kvs)
    {
        var tickets = new List<ValkeyRotationTicket>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            // "/valkeyworker/rotations/<C>" → ["", "valkeyworker", "rotations", <C>]
            var segments = kv.Key.Split('/');
            if (segments.Length != 4 || segments[3].Length == 0)
            {
                errors.Add(new(kv.Key, "ожидается /valkeyworker/rotations/<cluster>"));
                continue;
            }

            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                var requested = JsonValues.ReadLong(root, "requested_unix");
                if (requested is null)
                {
                    errors.Add(new(kv.Key, "нет поля requested_unix"));
                    continue;
                }

                tickets.Add(new ValkeyRotationTicket(
                    segments[3],
                    JsonValues.ReadString(root, "role") ?? "",
                    requested.Value,
                    JsonValues.ReadString(root, "requested_by")));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"битый JSON: {e.Message}"));
            }
        }

        return new(tickets, errors);
    }

    private static ValkeyClusterInfo BuildCluster(ClusterAcc acc, List<KeyParseError> errors)
    {
        var (nodes, maxmemory, policy, createdUnix, state) = ParseConfig(acc.Name, acc.ConfigRaw, errors);
        var nodeList = acc.Nodes
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => BuildNode(acc.Name, pair.Value, errors))
            .ToList();
        return new ValkeyClusterInfo(
            acc.Name, state, nodes, maxmemory, policy, createdUnix, acc.Endpoints, nodeList);
    }

    // Ключа config нет — кластер-скелет из прочих ключей; не ошибка парсера.
    private static (
        int Nodes, long MaxmemoryBytes, string MaxmemoryPolicy, long? CreatedUnix,
        ValkeyClusterState State)
        ParseConfig(string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return (0, 0, "", null, ValkeyClusterState.Active);

        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            return (
                AsInt(JsonValues.ReadLong(root, "nodes")),
                JsonValues.ReadLong(root, "maxmemory_bytes") ?? 0,
                JsonValues.ReadString(root, "maxmemory_policy") ?? "",
                JsonValues.ReadLong(root, "created_unix"),
                ValkeyClusterStates.Parse(JsonValues.ReadString(root, "state")));
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/valkey/clusters/{cluster}/config", "битый JSON config"));
            return (0, 0, "", null, ValkeyClusterState.Active);
        }
    }

    // resources: cpu — decimal invariant; mem/disk — "<int>Gi" → int; неканонический
    // суффикс/число → поле null (не ошибка — заявка неполна); cpu не число → parseError.
    private static ValkeyNodeInfo BuildNode(string cluster, NodeAcc acc, List<KeyParseError> errors)
    {
        decimal? cpu = null;
        int? memGi = null, diskGi = null;
        if (acc.ResourcesRaw is not null)
        {
            try
            {
                using var doc = JsonDocument.Parse(acc.ResourcesRaw);
                var root = doc.RootElement;
                var cpuRaw = JsonValues.ReadString(root, "cpu");
                if (cpuRaw is not null
                    && decimal.TryParse(cpuRaw, NumberStyles.Number, CultureInfo.InvariantCulture, out var cpuValue))
                    cpu = cpuValue;
                else
                    errors.Add(new KeyParseError(
                        $"/valkey/clusters/{cluster}/nodes/{acc.Name}/resources", "поле cpu не число"));

                memGi = ParseGi(JsonValues.ReadString(root, "mem"));
                diskGi = ParseGi(JsonValues.ReadString(root, "disk"));
            }
            catch (JsonException)
            {
                errors.Add(new KeyParseError(
                    $"/valkey/clusters/{cluster}/nodes/{acc.Name}/resources", "битый JSON resources"));
            }
        }

        return new ValkeyNodeInfo(acc.Name, acc.State, cpu, memGi, diskGi);
    }

    private static int? ParseGi(string? raw)
        => raw is not null && raw.EndsWith("Gi", StringComparison.Ordinal)
           && int.TryParse(raw[..^2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : null;

    private static int AsInt(long? value) => value is null ? 0 : (int)value.Value;

    private static TValue GetOrAdd<TKey, TValue>(Dictionary<TKey, TValue> dictionary, TKey key, Func<TKey, TValue> factory)
        where TKey : notnull
    {
        if (!dictionary.TryGetValue(key, out var value))
        {
            value = factory(key);
            dictionary[key] = value;
        }

        return value;
    }
}
