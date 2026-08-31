using System.Globalization;
using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;

namespace PgWorker.Etcd.Parsing;

// Парсер контроль-плейна /clusters/ + Patroni DCS /service/ в доменную модель PgWorker
// (адаптация AdminPanel ClustersParser/ServiceParser, arch/14 §3). Чистые функции
// Kv[] → модель: битые значения не бросают исключений, а попадают в parseErrors.
public static class ClusterSnapshotParser
{
    // Состояние HA-scope Patroni (P2.2: initialize + leader = кластер поднялся).
    public sealed record HaScopeState(string Scope, bool Initialized, string? LeaderName);

    private sealed class ShardAcc
    {
        public string? Dsn;
        public string? ReplicasRaw;
        public string? Master;
        public string? StateRaw;
        public readonly List<(string Name, string? State)> Nodes = [];
        public readonly Dictionary<string, string?> AppParams = [];
    }

    private sealed class ClusterAcc(string name)
    {
        public readonly string Name = name;
        public string? ConfigRaw;
        public string? AppUser;
        public string? AppPassword;
        public readonly Dictionary<string, ShardAcc> Shards = [];
        public readonly Dictionary<int, string> Routing = [];
        public readonly Dictionary<int, string> StatusRaw = [];
    }

    // kvs префикса /clusters/ → кластеры (config+shards+nodes+routing+status).
    // Толерантность: битый JSON ключа → запись в parseErrors, ключ пропущен;
    // неизвестные ключи игнорируются; state="NOT_INITIALIZED"/"TO_REMOVE", отсутствие → Active.
    public static Result<IReadOnlyList<ClusterSnapshot>> ParseClusters(
        IReadOnlyList<Kv> kvs, out IReadOnlyList<string> parseErrors)
    {
        var errors = new List<string>();
        var accs = new Dictionary<string, ClusterAcc>();

        foreach (var kv in kvs)
        {
            // "/clusters/<C>/leaf…" → ["", "clusters", <C>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 4 || segments[1] != "clusters" || segments[2].Length == 0)
            {
                continue; // чужой префикс (в т.ч. /pgworker/) — не наша забота
            }

            var acc = GetOrAdd(accs, segments[2], static name => new ClusterAcc(name));
            switch (segments[3])
            {
                case "config" when segments.Length == 4:
                    acc.ConfigRaw = kv.Value;
                    break;

                case "shards" when segments.Length == 6
                    && segments[4].Length > 0
                    && segments[5] == "state":
                {
                    // Маркер демонтажа шарда (t06 §4.2): единственное значение "TO_REMOVE";
                    // иное/битое — не ошибка, ToRemove=false (значение одно — parseError не пишем).
                    var shard = GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc());
                    shard.StateRaw = kv.Value;
                    break;
                }

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

                case "shards" when segments.Length == 8
                    && segments[4].Length > 0
                    && segments[5] == "nodes"
                    && segments[6].Length > 0
                    && segments[7] == "app_params":
                {
                    var shard = GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc());
                    // Kv.Value non-nullable: Trim() пустой строки даёт "" — ключ есть
                    // с пустым значением (spec §3.1); Trim() — нормализация пробелов.
                    shard.AppParams[segments[6]] = kv.Value.Trim();
                    break;
                }

                case "buckets" when segments.Length == 6 && segments[4] == "routing"
                    && segments[5].StartsWith("bucket_", StringComparison.Ordinal):
                {
                    if (TryBucketId(segments[5], out var id))
                        acc.Routing[id] = kv.Value;
                    else
                        errors.Add($"{kv.Key}: нечисловой id бакета в имени ключа");
                    break;
                }

                case "buckets" when segments.Length == 6 && segments[4] == "status"
                    && segments[5].StartsWith("bucket_", StringComparison.Ordinal):
                {
                    if (TryBucketId(segments[5], out var id))
                        acc.StatusRaw[id] = kv.Value;
                    else
                        errors.Add($"{kv.Key}: нечисловой id бакета в имени ключа");
                    break;
                }

                case "app_user" when segments.Length == 4:
                    acc.AppUser = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                case "app_password" when segments.Length == 4:
                    acc.AppPassword = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                default:
                    // система развивается — неизвестный ключ не ошибка, просто игнор
                    break;
            }
        }

        var clusters = accs.Values
            .OrderBy(c => c.Name, StringComparer.Ordinal)
            .Select(acc => BuildCluster(acc, errors))
            .ToList();

        parseErrors = errors;
        return Result<IReadOnlyList<ClusterSnapshot>>.Success(clusters);
    }

    // kvs префикса /service/ → состояние scope'ов Patroni (<C>-<X>).
    public static IReadOnlyList<HaScopeState> ParseService(IReadOnlyList<Kv> kvs)
    {
        var accs = new Dictionary<string, (string? Leader, string? Initialize)>();
        foreach (var kv in kvs)
        {
            // "/service/<scope>/…" → ["", "service", <scope>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length != 4 || segments[1] != "service" || segments[2].Length == 0)
            {
                continue;
            }

            var slot = GetOrAdd(accs, segments[2], static _ => (null, (string?)null));
            switch (segments[3])
            {
                case "leader":
                    slot.Leader = kv.Value;
                    accs[segments[2]] = slot;
                    break;
                case "initialize":
                    slot.Initialize = kv.Value;
                    accs[segments[2]] = slot;
                    break;
            }
        }

        return accs
            .OrderBy(p => p.Key, StringComparer.Ordinal)
            .Select(p => new HaScopeState(
                p.Key,
                p.Value.Initialize is { Length: > 0 },
                ParseLeader(p.Value.Leader)))
            .ToList();
    }

    private static ClusterSnapshot BuildCluster(ClusterAcc acc, List<string> errors)
    {
        var config = ParseConfig(acc.Name, acc.ConfigRaw, errors);
        var shards = acc.Shards
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => BuildShard(acc.Name, pair.Key, pair.Value, errors))
            .ToList();
        var routing = BuildRouting(config.Buckets, acc, errors);
        AppCredentials? app = acc.AppUser is { Length: > 0 } u && acc.AppPassword is { Length: > 0 } p
            ? new AppCredentials(u, p)
            : null;
        return new ClusterSnapshot(config, shards, routing, app);
    }

    private static ClusterConfig ParseConfig(string cluster, string? raw, List<string> errors)
    {
        if (raw is null)
            return new ClusterConfig(cluster, 0, string.Empty, null, ClusterState.Active, null, null); // нет ключа — не ошибка

        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var buckets = ReadLong(root, "buckets");
            return new ClusterConfig(
                cluster,
                buckets is null ? 0 : (int)buckets.Value,
                ReadString(root, "dbname") ?? string.Empty,
                ReadLong(root, "created_unix"), // может отсутствовать у старых init
                ReadString(root, "state") switch
                {
                    "NOT_INITIALIZED" => ClusterState.NotInitialized,
                    "TO_REMOVE" => ClusterState.ToRemove,
                    _ => ClusterState.Active, // отсутствие state = Active (02 §2.1)
                },
                ReadString(root, "bucket_admin_user"),
                ReadString(root, "bucket_admin_password"));
        }
        catch (JsonException)
        {
            errors.Add($"/clusters/{cluster}/config: битый JSON config");
            return new ClusterConfig(cluster, 0, string.Empty, null, ClusterState.Active, null, null);
        }
    }

    private static ShardSpec BuildShard(string cluster, string name, ShardAcc shard, List<string> errors)
    {
        var prefix = $"/clusters/{cluster}/shards/{name}/";
        var replicas = 0;
        if (shard.ReplicasRaw is not null)
        {
            if (int.TryParse(shard.ReplicasRaw.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
                replicas = parsed;
            else
                errors.Add(prefix + "replicas: значение не целое число");
        }

        var nodes = shard.Nodes
            .OrderBy(n => n.Name, StringComparer.Ordinal)
            .Select(n => new NodeSpec(
                name, n.Name, ParseNodeState(n.State),
                shard.AppParams.TryGetValue(n.Name, out var appParams) ? appParams : null))
            .ToList();

        return new ShardSpec(
            name,
            replicas,
            string.IsNullOrWhiteSpace(shard.Dsn) ? null : shard.Dsn.Trim(),
            string.IsNullOrWhiteSpace(shard.Master) ? null : shard.Master.Trim(),
            nodes,
            ToRemove: shard.StateRaw?.Trim() == "TO_REMOVE");
    }

    private static IReadOnlyList<BucketRoute> BuildRouting(int bucketsCount, ClusterAcc acc, List<string> errors)
    {
        // ids: полный диапазон 0..N-1 из config (все N, включая ACTIVE — «дыра» owner=null допустима)
        // ∪ фактические ключи (out-of-range bucket_99 остаются видимыми).
        var ids = bucketsCount > 0
            ? Enumerable.Range(0, bucketsCount).Union(acc.Routing.Keys).Union(acc.StatusRaw.Keys)
            : acc.Routing.Keys.Union(acc.StatusRaw.Keys);

        var result = new List<BucketRoute>();
        foreach (var id in ids.OrderBy(id => id))
        {
            acc.Routing.TryGetValue(id, out var owner);
            BucketMoveState? status = null; // нет status-ключа = ACTIVE
            string? moveSource = null;
            string? moveTarget = null;
            string? movePhase = null;
            long? moveUpdatedUnix = null;
            if (acc.StatusRaw.TryGetValue(id, out var raw)
                && !TryParseStatus(raw, out status, out moveSource, out moveTarget,
                    out movePhase, out moveUpdatedUnix))
            {
                errors.Add($"/clusters/{acc.Name}/buckets/status/bucket_{id}: битый JSON или неизвестное state");
                status = null;
            }

            result.Add(new BucketRoute(
                id,
                string.IsNullOrWhiteSpace(owner) ? null : owner.Trim(),
                status,
                MoveTarget: moveTarget,
                MoveSource: moveSource,
                MovePhase: movePhase,
                MoveUpdatedUnix: moveUpdatedUnix));
        }

        return result;
    }

    private static bool TryParseStatus(string raw, out BucketMoveState? state,
        out string? source, out string? target, out string? phase, out long? updatedUnix)
    {
        state = null;
        source = null;
        target = null;
        phase = null;
        updatedUnix = null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            state = ReadString(root, "state") switch
            {
                "SYNCING" => BucketMoveState.Syncing,
                "FROZEN" => BucketMoveState.Frozen,
                "ABORTING" => BucketMoveState.Aborting,
                "NOT_INITIALIZED" => BucketMoveState.NotInitialized,
                _ => null,
            };
            if (state is null)
                return false;

            // owner/target из СТАТУС-ключа (guard G4 t06): после flip статус-owner
            // отличается от routing-owner; у NOT_INITIALIZED — owner без target (02 §9).
            source = ReadString(root, "owner");
            target = state == BucketMoveState.NotInitialized ? null : ReadString(root, "target");
            // phase/updated_unix — возраст и фаза доведения для репарации (§3.5).
            phase = ReadString(root, "phase");
            updatedUnix = ReadLong(root, "updated_unix");
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    // nodes/<n>/state: панель отображает как строку; наш enum с толерантным дефолтом.
    private static NodeState ParseNodeState(string? raw) => raw switch
    {
        "PROVISIONING" => NodeState.Provisioning,
        "RUNNING" => NodeState.Running,
        "REBUILDING" => NodeState.Rebuilding,
        "UNREACHABLE" => NodeState.Unreachable,
        "QUARANTINED" => NodeState.Quarantined,
        "REMOVING" => NodeState.Removing,
        "TO_RECREATE" => NodeState.ToRecreate,
        _ => NodeState.NotInitialized,
    };

    // leader: JSON {"name":…} (Patroni) либо plain-строка-имя (стенд).
    private static string? ParseLeader(string? raw)
    {
        if (raw is null)
            return null; // нет ключа = нет лидера

        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (doc.RootElement.ValueKind == JsonValueKind.Object)
                return ReadString(doc.RootElement, "name")?.Trim();
        }
        catch (JsonException)
        {
            // не JSON — трактуем как строку-имя
        }

        var trimmed = raw.Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private static bool TryBucketId(string leaf, out int id)
        => int.TryParse(leaf["bucket_".Length..], NumberStyles.Integer, CultureInfo.InvariantCulture, out id);

    // Толерантное чтение полей JSON-значений: строки-числа, отсутствующие поля.
    private static string? ReadString(JsonElement root, string name)
        => root.ValueKind == JsonValueKind.Object
            && root.TryGetProperty(name, out var element)
            && element.ValueKind is JsonValueKind.String or JsonValueKind.Number
            ? element.ToString()
            : null;

    private static long? ReadLong(JsonElement root, string name)
    {
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty(name, out var element))
            return null;

        return element.ValueKind switch
        {
            JsonValueKind.Number => element.TryGetInt64(out var value) ? value : null,
            JsonValueKind.String when long.TryParse(element.GetString(), out var value) => value,
            _ => null,
        };
    }

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
