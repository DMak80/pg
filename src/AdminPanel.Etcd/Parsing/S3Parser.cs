using System.Globalization;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Core.S3;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора префикса /s3/clusters/ (arch/02 §12.1).
public sealed record S3ClustersParseResult(
    IReadOnlyList<S3ClusterInfo> Clusters,
    IReadOnlyList<KeyParseError> Errors,
    int UnknownKeyCount);

// Результат разбора очереди ротаций /s3worker/rotations/ (arch/23 §8).
public sealed record S3RotationsParseResult(
    IReadOnlyList<S3RotationTicket> Tickets,
    IReadOnlyList<KeyParseError> Errors);

// Парсер s3-домена: чистые функции Kv[] → модель, битые значения не бросают
// исключений — порождают KeyParseError (порт ValkeyParser; arch/23 §2).
// PKI-ключи (pki/*, nodes/<I>/pki) пропускаются МОЛЧА — материал только
// воркеру (arch/23 §6), без unknownKeys-счётчика.
public static class S3Parser
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
        public string? LinksRaw;
        public string? DenyListRaw;
        public string? ConnectivityRaw;
        public string? SyncRaw;
        public readonly Dictionary<string, NodeAcc> Nodes = [];
    }

    public static S3ClustersParseResult ParseClusters(IReadOnlyList<Kv> kvs)
    {
        var errors = new List<KeyParseError>();
        var unknown = 0;
        var accs = new Dictionary<string, ClusterAcc>();

        foreach (var kv in kvs)
        {
            // "/s3/clusters/<C>/leaf…" → ["", "s3", "clusters", <C>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 5 || segments[1] != "s3" || segments[2] != "clusters"
                || segments[3].Length == 0)
            {
                unknown++;
                continue;
            }

            var acc = GetOrAdd(accs, segments[3], static name => new ClusterAcc(name));
            switch (segments[4])
            {
                // PKI репликации — только воркеру (arch/23 §6): молча, без unknownKeys.
                case "pki":
                    break;

                case "config" when segments.Length == 5:
                    acc.ConfigRaw = kv.Value;
                    break;

                case "endpoints" when segments.Length == 5:
                    acc.Endpoints = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                case "links" when segments.Length == 5:
                    acc.LinksRaw = kv.Value;
                    break;

                case "denylist" when segments.Length == 5:
                    acc.DenyListRaw = kv.Value;
                    break;

                case "connectivity" when segments.Length == 5:
                    acc.ConnectivityRaw = kv.Value;
                    break;

                case "sync" when segments.Length == 5:
                    acc.SyncRaw = kv.Value;
                    break;

                case "nodes" when segments.Length == 7
                    && segments[5].Length > 0
                    && segments[6] is "placement" or "resources" or "state" or "placed_on"
                        or "usage" or "pki":
                {
                    if (segments[6] == "pki")
                        break; // PKI инстанса — молча (arch/23 §6)

                    var node = GetOrAdd(acc.Nodes, segments[5], static name => new NodeAcc(name));
                    switch (segments[6])
                    {
                        case "placement":
                            node.PlacementRaw = kv.Value;
                            break;
                        case "resources":
                            node.ResourcesRaw = kv.Value;
                            break;
                        case "state":
                            node.State = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                            break;
                        case "placed_on":
                            node.PlacedOn = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                            break;
                        case "usage":
                            node.UsageRaw = kv.Value;
                            break;
                    }

                    break;
                }

                default:
                    // система развивается — неизвестный ключ не ошибка, только счётчик (arch/23 §2)
                    unknown++;
                    break;
            }
        }

        var clusters = accs.Values
            .OrderBy(c => c.Name, StringComparer.Ordinal)
            .Select(acc => BuildCluster(acc, errors))
            .ToList();

        return new S3ClustersParseResult(clusters, errors, unknown);
    }

    // Ротации: {"requested_unix","requested_by"}; битый JSON / нет
    // requested_unix → parseError-запись (ключ не трогаем).
    public static S3RotationsParseResult ParseRotations(IReadOnlyList<Kv> kvs)
    {
        var tickets = new List<S3RotationTicket>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            // "/s3worker/rotations/<C>" → ["", "s3worker", "rotations", <C>]
            var segments = kv.Key.Split('/');
            if (segments.Length != 4 || segments[3].Length == 0)
            {
                errors.Add(new(kv.Key, "expected /s3worker/rotations/<cluster>"));
                continue;
            }

            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                var requested = JsonValues.ReadLong(root, "requested_unix");
                if (requested is null)
                {
                    errors.Add(new(kv.Key, "missing requested_unix field"));
                    continue;
                }

                tickets.Add(new S3RotationTicket(
                    segments[3], requested.Value, JsonValues.ReadString(root, "requested_by")));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"broken JSON: {e.Message}"));
            }
        }

        return new(tickets, errors);
    }

    // Сборка кластера из накопленных raw-значений (толерантно: битое — parseError,
    // поле пустое, кластер-скелет строится).
    private static S3ClusterInfo BuildCluster(ClusterAcc acc, List<KeyParseError> errors)
    {
        var (platform, nodes, profile, created, state) = ParseConfig(acc.Name, acc.ConfigRaw, errors);
        return new S3ClusterInfo(
            acc.Name,
            state,
            platform,
            nodes,
            profile,
            created,
            acc.Endpoints,
            [.. acc.Nodes.OrderBy(p => p.Key, StringComparer.Ordinal)
                .Select(p => BuildNode(acc.Name, p.Value, errors))],
            ParseLinks(acc.Name, acc.LinksRaw, errors),
            ParseDenyList(acc.Name, acc.DenyListRaw, errors),
            ParseConnectivity(acc.Name, acc.ConnectivityRaw, errors),
            ParseSync(acc.Name, acc.SyncRaw, errors));
    }

    // config: {"platform","nodes","secret_profile","created_unix","state"?}.
    private static (string Platform, int Nodes, string SecretProfile, long? CreatedUnix,
        S3ClusterState State) ParseConfig(string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return ("", 0, "", null, S3ClusterState.Active);

        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            return (
                JsonValues.ReadString(root, "platform") ?? "",
                AsInt(JsonValues.ReadLong(root, "nodes")),
                JsonValues.ReadString(root, "secret_profile") ?? "",
                JsonValues.ReadLong(root, "created_unix"),
                S3ClusterStates.Parse(JsonValues.ReadString(root, "state")));
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/s3/clusters/{cluster}/config", "broken config JSON"));
            return ("", 0, "", null, S3ClusterState.Active);
        }
    }

    // placement: {"docker_host"?,"swarm_node"?,"swarm_labels"?{…}} — конъюнкция
    // указанных атрибутов (arch/23 §4); пустой объект → null (не ошибка).
    private static S3NodePlacement? ParsePlacement(string cluster, string node, string? raw,
        List<KeyParseError> errors)
    {
        if (raw is null)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            IReadOnlyDictionary<string, string>? labels = null;
            if (root.TryGetProperty("swarm_labels", out var labelsEl)
                && labelsEl.ValueKind == JsonValueKind.Object)
            {
                var dict = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (var prop in labelsEl.EnumerateObject())
                    dict[prop.Name] = prop.Value.GetString() ?? "";
                labels = dict;
            }

            return new S3NodePlacement(
                JsonValues.ReadString(root, "docker_host"),
                JsonValues.ReadString(root, "swarm_node"),
                labels);
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError(
                $"/s3/clusters/{cluster}/nodes/{node}/placement", "broken placement JSON"));
            return null;
        }
    }

    // resources: cpu — decimal invariant; mem/disk — "<int>Gi" → int (как valkey).
    private static S3NodeResources? ParseResources(string cluster, string node, string? raw,
        List<KeyParseError> errors)
    {
        if (raw is null)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var cpuRaw = JsonValues.ReadString(root, "cpu");
            if (cpuRaw is null
                || !decimal.TryParse(cpuRaw, NumberStyles.Number, CultureInfo.InvariantCulture, out var cpu))
            {
                errors.Add(new KeyParseError(
                    $"/s3/clusters/{cluster}/nodes/{node}/resources", "cpu field is not a number"));
                return null;
            }

            var memGi = ParseGi(JsonValues.ReadString(root, "mem"));
            var diskGi = ParseGi(JsonValues.ReadString(root, "disk"));
            if (memGi is null || diskGi is null)
            {
                errors.Add(new KeyParseError(
                    $"/s3/clusters/{cluster}/nodes/{node}/resources", "mem/disk not in <int>Gi format"));
                return null;
            }

            return new S3NodeResources(cpu, memGi, diskGi);
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError(
                $"/s3/clusters/{cluster}/nodes/{node}/resources", "broken resources JSON"));
            return null;
        }
    }

    // usage: {"used_bytes","total_bytes","updated_unix"} — все три обязательны.
    private static S3NodeUsage? ParseUsage(string cluster, string node, string? raw,
        List<KeyParseError> errors)
    {
        if (raw is null)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var used = JsonValues.ReadLong(root, "used_bytes");
            var total = JsonValues.ReadLong(root, "total_bytes");
            var updated = JsonValues.ReadLong(root, "updated_unix");
            if (used is null || total is null || updated is null)
            {
                errors.Add(new KeyParseError(
                    $"/s3/clusters/{cluster}/nodes/{node}/usage",
                    "missing required fields used_bytes/total_bytes/updated_unix"));
                return null;
            }

            return new S3NodeUsage(used.Value, total.Value, updated.Value);
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError(
                $"/s3/clusters/{cluster}/nodes/{node}/usage", "broken usage JSON"));
            return null;
        }
    }

    private static S3NodeInfo BuildNode(string cluster, NodeAcc acc, List<KeyParseError> errors)
        => new(
            acc.Name,
            acc.State,
            acc.PlacedOn,
            ParsePlacement(cluster, acc.Name, acc.PlacementRaw, errors),
            ParseResources(cluster, acc.Name, acc.ResourcesRaw, errors),
            ParseUsage(cluster, acc.Name, acc.UsageRaw, errors));

    // links: [{"a","b"},…] — произвольный набор пар (arch/23 §2).
    private static IReadOnlyList<S3ReplicationLink> ParseLinks(
        string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return [];
        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
            {
                errors.Add(new KeyParseError($"/s3/clusters/{cluster}/links", "value is not an array"));
                return [];
            }

            return [.. doc.RootElement.EnumerateArray().Select(el => new S3ReplicationLink(
                JsonValues.ReadString(el, "a") ?? "",
                JsonValues.ReadString(el, "b") ?? ""))];
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/s3/clusters/{cluster}/links", "broken links JSON"));
            return [];
        }
    }

    // denylist: ["<I>",…] — отозванные инстансы.
    private static IReadOnlyList<string> ParseDenyList(
        string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return [];
        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
            {
                errors.Add(new KeyParseError($"/s3/clusters/{cluster}/denylist", "value is not an array"));
                return [];
            }

            return [.. doc.RootElement.EnumerateArray()
                .Where(el => el.ValueKind == JsonValueKind.String)
                .Select(el => el.GetString() ?? "")];
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/s3/clusters/{cluster}/denylist", "broken denylist JSON"));
            return [];
        }
    }

    // connectivity (arch/23 §3): state/checked_unix/links-матрица/probe_error?.
    private static S3ConnectivityInfo? ParseConnectivity(
        string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var matrix = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.Ordinal);
            if (root.TryGetProperty("links", out var linksEl)
                && linksEl.ValueKind == JsonValueKind.Object)
            {
                foreach (var row in linksEl.EnumerateObject())
                {
                    var cells = new Dictionary<string, string>(StringComparer.Ordinal);
                    if (row.Value.ValueKind == JsonValueKind.Object)
                        foreach (var cell in row.Value.EnumerateObject())
                            cells[cell.Name] = cell.Value.GetString() ?? "";
                    matrix[row.Name] = cells;
                }
            }

            return new S3ConnectivityInfo(
                JsonValues.ReadString(root, "state"),
                JsonValues.ReadLong(root, "checked_unix"),
                matrix,
                JsonValues.ReadString(root, "probe_error"));
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/s3/clusters/{cluster}/connectivity", "broken connectivity JSON"));
            return null;
        }
    }

    // sync (arch/23 §3): state/checked_unix/pairs/last_error?.
    private static S3SyncInfo? ParseSync(string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var pairs = new List<S3SyncPair>();
            if (root.TryGetProperty("pairs", out var pairsEl)
                && pairsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var el in pairsEl.EnumerateArray())
                    pairs.Add(new S3SyncPair(
                        JsonValues.ReadString(el, "from") ?? "",
                        JsonValues.ReadString(el, "to") ?? "",
                        JsonValues.ReadLong(el, "missing") ?? 0,
                        JsonValues.ReadLong(el, "extra") ?? 0,
                        JsonValues.ReadLong(el, "lag_bytes"),
                        JsonValues.ReadLong(el, "lag_objects")));
            }

            return new S3SyncInfo(
                JsonValues.ReadString(root, "state"),
                JsonValues.ReadLong(root, "checked_unix"),
                pairs,
                JsonValues.ReadString(root, "last_error"));
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/s3/clusters/{cluster}/sync", "broken sync JSON"));
            return null;
        }
    }

    private static int? ParseGi(string? raw)
        => raw is not null && raw.EndsWith("Gi", StringComparison.Ordinal)
           && int.TryParse(raw[..^2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : null;

    // Unchecked-каст (порт ValkeyParser): config.nodes вне int не бросает —
    // контракт парсера «битые значения не бросают исключений» (arch/23 §2).
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
