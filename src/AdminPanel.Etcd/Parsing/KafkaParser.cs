using System.Globalization;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора префикса /kafka/clusters/ (arch/02 §10.1).
public sealed record KafkaClustersParseResult(
    IReadOnlyList<KafkaClusterInfo> Clusters,
    IReadOnlyList<KeyParseError> Errors,
    int UnknownKeyCount);

// Результат разбора очереди ротаций /kafkaworker/rotations/ (arch/15 §4).
public sealed record KafkaRotationsParseResult(
    IReadOnlyList<KafkaRotationTicket> Tickets,
    IReadOnlyList<KeyParseError> Errors);

// Парсер kafka-домена: чистые функции Kv[] → модель, битые значения не бросают
// исключений — порождают KeyParseError (порт стиля ClustersParser; arch/15 §6).
public static class KafkaParser
{
    private sealed class BrokerAcc(string name)
    {
        public readonly string Name = name;
        public string? State;
        public string? Role;
        public string? ResourcesRaw;
    }

    private sealed class ClusterAcc(string name)
    {
        public readonly string Name = name;
        public string? ConfigRaw;
        public string? Endpoints;
        public readonly Dictionary<string, BrokerAcc> Brokers = [];
        public readonly List<(string Name, string Raw)> Topics = [];
    }

    public static KafkaClustersParseResult ParseClusters(IReadOnlyList<Kv> kvs)
    {
        var errors = new List<KeyParseError>();
        var unknown = 0;
        var accs = new Dictionary<string, ClusterAcc>();

        foreach (var kv in kvs)
        {
            // "/kafka/clusters/<C>/leaf…" → ["", "kafka", "clusters", <C>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 5 || segments[1] != "kafka" || segments[2] != "clusters"
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
                    acc.Endpoints = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                // Креды SASL: панель читает их для проб через refresher (B6), в модель
                // кластера не выносит (arch/02 §10.1) — expected-skip без счётчика.
                case "app_user" or "app_password" when segments.Length == 5:
                    break;

                case "brokers" when segments.Length == 7
                    && segments[5].Length > 0
                    && segments[6] is "state" or "role" or "resources":
                {
                    var broker = GetOrAdd(acc.Brokers, segments[5], static name => new BrokerAcc(name));
                    switch (segments[6])
                    {
                        case "state":
                            broker.State = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                            break;
                        case "role":
                            broker.Role = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                            break;
                        default:
                            broker.ResourcesRaw = kv.Value;
                            break;
                    }

                    break;
                }

                case "topics" when segments.Length == 6 && segments[5].Length > 0:
                    acc.Topics.Add((segments[5], kv.Value));
                    break;

                default:
                    // система развивается — неизвестный ключ не ошибка, только счётчик (arch/15 §6)
                    unknown++;
                    break;
            }
        }

        var clusters = accs.Values
            .OrderBy(c => c.Name, StringComparer.Ordinal)
            .Select(acc => BuildCluster(acc, errors))
            .ToList();

        return new KafkaClustersParseResult(clusters, errors, unknown);
    }

    public static KafkaRotationsParseResult ParseRotations(IReadOnlyList<Kv> kvs)
    {
        var tickets = new List<KafkaRotationTicket>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            // "/kafkaworker/rotations/<C>" → ["", "kafkaworker", "rotations", <C>]
            var segments = kv.Key.Split('/');
            if (segments.Length != 4 || segments[3].Length == 0)
            {
                errors.Add(new(kv.Key, "ожидается /kafkaworker/rotations/<cluster>"));
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

                tickets.Add(new KafkaRotationTicket(
                    segments[3], requested.Value, JsonValues.ReadString(root, "requested_by")));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"битый JSON: {e.Message}"));
            }
        }

        return new(tickets, errors);
    }

    private static KafkaClusterInfo BuildCluster(ClusterAcc acc, List<KeyParseError> errors)
    {
        var (brokers, rf, minIsr, partitions, retention, createdUnix, state) =
            ParseConfig(acc.Name, acc.ConfigRaw, errors);

        var brokerInfos = acc.Brokers
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => BuildBroker(acc.Name, pair.Value, errors))
            .ToList();

        var topics = acc.Topics
            .OrderBy(pair => pair.Name, StringComparer.Ordinal)
            .Select(pair => BuildTopic(acc.Name, pair.Name, pair.Raw, errors))
            .OfType<KafkaTopicInfo>()
            .ToList();

        return new KafkaClusterInfo(
            acc.Name, state, brokers, rf, minIsr, partitions, retention, createdUnix,
            acc.Endpoints, brokerInfos, topics);
    }

    private static (
        int Brokers, int ReplicationFactor, int MinInSyncReplicas, int DefaultPartitions,
        long DefaultRetentionMs, long? CreatedUnix, KafkaClusterState State)
        ParseConfig(string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            // Ключа нет — кластер-скелет из прочих ключей; не ошибка парсера (pg-семантика incomplete).
            return (0, 0, 0, 0, 0, null, KafkaClusterState.Active);

        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            return (
                AsInt(JsonValues.ReadLong(root, "brokers")),
                AsInt(JsonValues.ReadLong(root, "replication_factor")),
                AsInt(JsonValues.ReadLong(root, "min_insync_replicas")),
                AsInt(JsonValues.ReadLong(root, "default_partitions")),
                // default_retention_ms — не nullable в модели: отсутствующее поле = 0 (битые config
                // отсекаются выше; фактические записи несут полный набор).
                JsonValues.ReadLong(root, "default_retention_ms") ?? 0,
                JsonValues.ReadLong(root, "created_unix"),
                KafkaClusterStates.Parse(JsonValues.ReadString(root, "state")));
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/kafka/clusters/{cluster}/config", "битый JSON config"));
            return (0, 0, 0, 0, 0, null, KafkaClusterState.Active);
        }
    }

    private static KafkaBrokerInfo BuildBroker(string cluster, BrokerAcc acc, List<KeyParseError> errors)
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
                        $"/kafka/clusters/{cluster}/brokers/{acc.Name}/resources", "поле cpu не число"));

                memGi = ParseGi(JsonValues.ReadString(root, "mem"));
                diskGi = ParseGi(JsonValues.ReadString(root, "disk"));
                if (memGi is null || diskGi is null)
                    errors.Add(new KeyParseError(
                        $"/kafka/clusters/{cluster}/brokers/{acc.Name}/resources",
                        "поле mem/disk не в формате <n>Gi"));
            }
            catch (JsonException)
            {
                errors.Add(new KeyParseError(
                    $"/kafka/clusters/{cluster}/brokers/{acc.Name}/resources", "битый JSON resources"));
            }
        }

        return new KafkaBrokerInfo(acc.Name, acc.State, acc.Role, cpu, memGi, diskGi);
    }

    // null — ключ пропущен с parseError-записью (arch/15 §6 «битый JSON → ключ пропускается»).
    private static KafkaTopicInfo? BuildTopic(
        string cluster, string topic, string raw, List<KeyParseError> errors)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            TopicDesiredDto? desired = null;
            if (root.TryGetProperty("desired", out var desiredElement)
                && desiredElement.ValueKind == JsonValueKind.Object)
            {
                desired = new TopicDesiredDto(
                    AsInt(JsonValues.ReadLong(desiredElement, "partitions")),
                    ConfigValue(desiredElement, "retention.ms"),
                    AsShort(ConfigValue(desiredElement, "min.insync.replicas")),
                    JsonValues.ReadLong(root, "desired_unix"),
                    JsonValues.ReadString(root, "desired_by"));
            }

            var missing = root.TryGetProperty("missing", out var missingElement)
                          && missingElement.ValueKind == JsonValueKind.True;

            return new KafkaTopicInfo(
                topic,
                AsInt(JsonValues.ReadLong(root, "partitions")),
                AsShort(JsonValues.ReadLong(root, "replication_factor")),
                ConfigValue(root, "retention.ms"),
                AsShort(ConfigValue(root, "min.insync.replicas")),
                desired,
                missing,
                JsonValues.ReadLong(root, "synced_unix"));
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/kafka/clusters/{cluster}/topics/{topic}", "битый JSON топика"));
            return null;
        }
    }

    // configs — словарь строковых значений (как отдаёт Kafka, arch/15 §3).
    private static long? ConfigValue(JsonElement root, string name)
        => root.TryGetProperty("configs", out var configs)
           && configs.ValueKind == JsonValueKind.Object
           && configs.TryGetProperty(name, out var value)
           && long.TryParse(value.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;

    private static int? ParseGi(string? raw)
        => raw is not null && raw.EndsWith("Gi", StringComparison.Ordinal)
           && int.TryParse(raw[..^2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : null;

    private static int AsInt(long? value) => value is null ? 0 : (int)value.Value;

    private static short? AsShort(long? value)
        => value is null ? null : value.Value is >= short.MinValue and <= short.MaxValue ? (short)value.Value : null;

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
