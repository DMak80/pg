using System.Globalization;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора префикса /clusters/ (spec §6.1).
public sealed record ClustersParseResult(
    IReadOnlyList<ClusterInfo> Clusters,
    IReadOnlyList<KeyParseError> Errors,
    int UnknownKeyCount);

// Парсер контроль-плейна шардинга /clusters/<C>/…: чистая функция Kv[] → модель,
// битые значения не бросают исключений — порождают KeyParseError (arch/02 §7).
public static class ClustersParser
{
    private sealed class ShardAcc
    {
        public string? Dsn;
        public string? ReplicasRaw;
        public string? Master;
        public readonly List<(string Name, string? State)> Nodes = [];
    }

    private sealed class ClusterAcc(string name)
    {
        public readonly string Name = name;
        public string? ConfigRaw;
        public readonly Dictionary<string, ShardAcc> Shards = [];
        public readonly Dictionary<int, string> Routing = [];
        public readonly Dictionary<int, string> StatusRaw = [];
        public readonly List<HealRecord> Heals = [];
    }

    public static ClustersParseResult Parse(IReadOnlyList<Kv> kvs)
    {
        var errors = new List<KeyParseError>();
        var unknown = 0;
        var accs = new Dictionary<string, ClusterAcc>();

        foreach (var kv in kvs)
        {
            // "/clusters/<C>/leaf…" → ["", "clusters", <C>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 4 || segments[1] != "clusters" || segments[2].Length == 0)
            {
                unknown++;
                continue;
            }

            var acc = GetOrAdd(accs, segments[2], static name => new ClusterAcc(name));
            switch (segments[3])
            {
                case "config" when segments.Length == 4:
                    acc.ConfigRaw = kv.Value;
                    break;

                case "shards" when segments.Length == 6
                    && segments[4].Length > 0
                    && segments[5] is "dsn" or "replicas" or "master":
                {
                    var shard = GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc());
                    switch (segments[5])
                    {
                        case "dsn":
                            shard.Dsn = kv.Value;
                            break;
                        case "replicas":
                            shard.ReplicasRaw = kv.Value;
                            break;
                        default:
                            shard.Master = kv.Value;
                            break;
                    }

                    break;
                }

                case "shards" when segments.Length == 8
                    && segments[4].Length > 0
                    && segments[5] == "nodes"
                    && segments[6].Length > 0
                    && segments[7] == "state":
                {
                    var shard = GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc());
                    shard.Nodes.Add((segments[6], string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim()));
                    break;
                }

                case "buckets" when segments.Length == 6 && segments[4] == "routing"
                    && segments[5].StartsWith("bucket_", StringComparison.Ordinal):
                {
                    if (TryBucketId(segments[5], out var id))
                        acc.Routing[id] = kv.Value;
                    else
                        errors.Add(new KeyParseError(kv.Key, "нечисловой id бакета в имени ключа"));
                    break;
                }

                case "buckets" when segments.Length == 6 && segments[4] == "status"
                    && segments[5].StartsWith("bucket_", StringComparison.Ordinal):
                {
                    if (TryBucketId(segments[5], out var id))
                        acc.StatusRaw[id] = kv.Value;
                    else
                        errors.Add(new KeyParseError(kv.Key, "нечисловой id бакета в имени ключа"));
                    break;
                }

                case "heals" when segments.Length == 5 && segments[4].Length > 0:
                {
                    var heal = ParseHeal(kv.Key, kv.Value);
                    if (heal is null)
                        errors.Add(new KeyParseError(kv.Key, "битый JSON heal-записи"));
                    else
                        acc.Heals.Add(heal);
                    break;
                }

                default:
                    // система развивается — неизвестный ключ не ошибка, только счётчик (arch/02 §2.1)
                    unknown++;
                    break;
            }
        }

        var clusters = accs.Values
            .OrderBy(c => c.Name, StringComparer.Ordinal)
            .Select(acc => BuildCluster(acc, errors))
            .ToList();

        return new ClustersParseResult(clusters, errors, unknown);
    }

    private static ClusterInfo BuildCluster(ClusterAcc acc, List<KeyParseError> errors)
    {
        var (dbName, bucketsCount, createdUnix, state) = ParseConfig(acc.Name, acc.ConfigRaw, errors);

        var shards = acc.Shards
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => BuildShard(acc.Name, pair.Key, pair.Value, errors))
            .ToList();

        var buckets = BuildBuckets(bucketsCount, acc, errors);

        return new ClusterInfo(acc.Name, dbName, bucketsCount, createdUnix, state, shards, buckets, acc.Heals);
    }

    private static (string? DbName, int BucketsCount, long? CreatedUnix, ClusterState State) ParseConfig(
        string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return (null, 0, null, ClusterState.Active); // ключа нет — incomplete, не ошибка (arch/02 §7)

        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var buckets = JsonValues.ReadLong(root, "buckets");
            return (
                JsonValues.ReadString(root, "dbname"),
                buckets is null ? 0 : (int)buckets.Value,
                JsonValues.ReadLong(root, "created_unix"), // может отсутствовать у старых init (arch/02 §2.1)
                JsonValues.ReadString(root, "state") switch
                {
                    "NOT_INITIALIZED" => ClusterState.NotInitialized,
                    "DELETING" => ClusterState.Deleting, // arch/02 §9.4
                    _ => ClusterState.Active, // отсутствие state = Active (arch/02 §9)
                });
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/clusters/{cluster}/config", "битый JSON config"));
            return (null, 0, null, ClusterState.Active);
        }
    }

    private static ShardInfo BuildShard(string cluster, string name, ShardAcc shard, List<KeyParseError> errors)
    {
        var prefix = $"/clusters/{cluster}/shards/{name}/";
        int? replicas = null;
        if (shard.ReplicasRaw is not null)
        {
            if (int.TryParse(shard.ReplicasRaw.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
                replicas = parsed;
            else
                errors.Add(new KeyParseError(prefix + "replicas", "значение не целое число"));
        }

        if (shard.Master == string.Empty)
            errors.Add(new KeyParseError(prefix + "master", "пустое значение"));

        var nodes = shard.Nodes
            .OrderBy(n => n.Name, StringComparer.Ordinal)
            .Select(n => new NodeInfo(n.Name, n.State))
            .ToList();

        var dsn = DsnParser.Parse(shard.Dsn ?? "");
        return new ShardInfo(
            name,
            shard.Dsn ?? "",
            dsn.Hosts,
            dsn.Port,
            dsn.DbName,
            dsn.User,
            replicas,
            string.IsNullOrWhiteSpace(shard.Master) ? null : shard.Master.Trim(),
            nodes,
            null); // Runtime — SQL-проба t06
    }

    private static IReadOnlyList<BucketInfo> BuildBuckets(int bucketsCount, ClusterAcc acc, List<KeyParseError> errors)
    {
        // ids: полный диапазон 0..N-1 из config (все N, включая ACTIVE — arch/02 §2.1)
        // ∪ фактические ключи (out-of-range routing вида bucket_99 остаются видимыми для
        // алерта t04 bucket-out-of-range; incomplete-кластер — только фактические, spec §3.7).
        var ids = bucketsCount > 0
            ? Enumerable.Range(0, bucketsCount).Union(acc.Routing.Keys).Union(acc.StatusRaw.Keys)
            : acc.Routing.Keys.Union(acc.StatusRaw.Keys);
        ids = ids.OrderBy(id => id);

        var result = new List<BucketInfo>();
        foreach (var id in ids)
        {
            acc.Routing.TryGetValue(id, out var owner);
            MoveInfo? move = null;
            var state = BucketState.Active;
            if (acc.StatusRaw.TryGetValue(id, out var raw)
                && !TryParseStatus(raw, out state, out move))
            {
                errors.Add(new KeyParseError(
                    $"/clusters/{acc.Name}/buckets/status/bucket_{id}",
                    "битый JSON или неизвестное state"));
            }

            result.Add(new BucketInfo(id, owner, state, move));
        }

        return result;
    }

    private static bool TryParseStatus(string raw, out BucketState state, out MoveInfo? move)
    {
        state = BucketState.Active;
        move = null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            state = JsonValues.ReadString(root, "state") switch
            {
                "SYNCING" => BucketState.Syncing,
                "FROZEN" => BucketState.Frozen,
                "ABORTING" => BucketState.Aborting,
                "NOT_INITIALIZED" => BucketState.NotInitialized,
                _ => BucketState.Active,
            };
            if (state == BucketState.Active)
                return false; // state отсутствует или неизвестен — считаем ключ битым

            if (state == BucketState.NotInitialized)
            {
                // начальное состояние создаваемого кластера: без target/phase — не переезд (arch/02 §9)
                move = new MoveInfo(
                    JsonValues.ReadString(root, "owner"),
                    null,
                    null,
                    JsonValues.ReadLong(root, "updated_unix"),
                    null,
                    null);
                return true;
            }

            move = new MoveInfo(
                JsonValues.ReadString(root, "owner"),
                JsonValues.ReadString(root, "target"),
                JsonValues.ReadLong(root, "started_unix"),
                JsonValues.ReadLong(root, "updated_unix"),
                JsonValues.ReadString(root, "phase"),
                JsonValues.ReadString(root, "last_error"));
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    // Heal-запись: имя бакета — из поля "bucket", при его отсутствии — суффикс ключа (spec §6.1).
    private static HealRecord? ParseHeal(string key, string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            return new HealRecord(
                JsonValues.ReadString(root, "bucket") ?? key[(key.LastIndexOf('/') + 1)..],
                JsonValues.ReadString(root, "was"),
                JsonValues.ReadString(root, "now"),
                JsonValues.ReadString(root, "reason"),
                JsonValues.ReadLong(root, "ts"));
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static bool TryBucketId(string leaf, out int id)
        => int.TryParse(leaf["bucket_".Length..], NumberStyles.Integer, CultureInfo.InvariantCulture, out id);

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
