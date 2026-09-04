using System.Globalization;
using System.Text.Json;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Templates;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.Etcd.Parsing;

// Парсер контроль-плейна /kafka/clusters/ в доменную модель KafkaWorker
// (arch/15 §2–3; порт стиля ClusterSnapshotParser PgWorker). Чистая функция
// Kv[] → модель: битые значения не бросают исключений, а попадают в
// parseErrors; неизвестные ключи — в счётчик unknownKeys (arch/15 §6).
public static class KafkaSnapshotParser
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
        public string? AppUser;
        public string? AppPassword;
        public string? AdminUser;
        public string? AdminPassword;
        public string? CaPem;
        public string? CaKey;
        public readonly Dictionary<string, BrokerAcc> Brokers = [];
        public readonly List<(string Topic, string Raw)> TopicRaw = [];
        public readonly List<(string Topic, string Op, string Raw)> LifecycleRaw = [];
        public readonly List<string> Errors = [];
        public int UnknownKeys;
    }

    // kvs префикса /kafka/clusters/ → кластеры (config+brokers+topics+дискавери).
    public static Result<IReadOnlyList<KafkaClusterSnapshot>> Parse(IReadOnlyList<Kv> kvs)
    {
        var accs = new Dictionary<string, ClusterAcc>();

        foreach (var kv in kvs)
        {
            // "/kafka/clusters/<C>/leaf…" → ["", "kafka", "clusters", <C>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 5 || segments[1] != "kafka" || segments[2] != "clusters"
                || segments[3].Length == 0)
            {
                continue; // чужой префикс (в т.ч. /pgworker/, /kafkaworker/) — не наша забота
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

                // Поля безопасности t03 (arch/15 §2): admin-креды + per-cluster CA.
                case "admin_user" when segments.Length == 5:
                    acc.AdminUser = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                case "admin_password" when segments.Length == 5:
                    acc.AdminPassword = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                case "ca_pem" when segments.Length == 5:
                {
                    var value = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    // Битый PEM — не исключение: parseError + поле null (arch/15 §6).
                    if (value is null || !ClusterPki.TryParseCertificate(value, out _))
                    {
                        acc.Errors.Add($"/kafka/clusters/{acc.Name}/ca_pem: битый PEM сертификата");
                        acc.CaPem = null;
                    }
                    else
                    {
                        acc.CaPem = value;
                    }

                    break;
                }

                case "ca_key" when segments.Length == 5:
                {
                    var value = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    // Битый PEM-ключ — не исключение: parseError + поле null (arch/15 §6).
                    if (value is null || !ClusterPki.TryParseRsaKey(value, out _))
                    {
                        acc.Errors.Add($"/kafka/clusters/{acc.Name}/ca_key: битый PEM ключа");
                        acc.CaKey = null;
                    }
                    else
                    {
                        acc.CaKey = value;
                    }

                    break;
                }

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
                    acc.TopicRaw.Add((segments[5], kv.Value));
                    break;

                case "topics" when segments.Length == 7
                    && segments[5].Length > 0
                    && segments[6] is "desired.create" or "desired.delete":
                    acc.LifecycleRaw.Add((segments[5], segments[6] == "desired.create" ? TopicLifecycleOps.Create : TopicLifecycleOps.Delete, kv.Value));
                    break;

                default:
                    // система развивается — неизвестный ключ не ошибка: счётчик unknownKeys.
                    acc.UnknownKeys++;
                    break;
            }
        }

        var clusters = accs.Values
            .OrderBy(c => c.Name, StringComparer.Ordinal)
            .Select(BuildCluster)
            .ToList();

        return Result<IReadOnlyList<KafkaClusterSnapshot>>.Success(clusters);
    }

    private static KafkaClusterSnapshot BuildCluster(ClusterAcc acc)
        => new(
            acc.Name,
            ParseConfig(acc.Name, acc.ConfigRaw, acc.Errors),
            acc.Brokers
                .OrderBy(pair => pair.Key, StringComparer.Ordinal)
                .Select(pair => BuildBroker(pair.Value, acc.Errors))
                .ToList(),
            acc.TopicRaw
                .OrderBy(t => t.Topic, StringComparer.Ordinal)
                .Select(t => BuildTopic(acc.Name, t.Topic, t.Raw, acc.Errors))
                .ToList(),
            acc.Errors,
            acc.UnknownKeys,
            acc.Endpoints,
            acc.AppUser,
            acc.AppPassword,
            acc.AdminUser,
            acc.AdminPassword,
            acc.CaPem,
            acc.CaKey,
            acc.LifecycleRaw
                .OrderBy(t => t.Topic, StringComparer.Ordinal)
                .Select(t => BuildLifecycleTicket(acc.Name, t.Topic, t.Op, t.Raw, acc.Errors))
                .OfType<TopicLifecycleTicket>()
                .ToList());

    private static KafkaClusterConfig ParseConfig(string cluster, string? raw, List<string> errors)
    {
        if (raw is null)
            return new KafkaClusterConfig(0, 0, 0, 0, 0, null, null); // нет ключа — не ошибка

        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            return new KafkaClusterConfig(
                ReadInt(root, "brokers") ?? 0,
                ReadInt(root, "replication_factor") ?? 0,
                ReadInt(root, "min_insync_replicas") ?? 0,
                ReadInt(root, "default_partitions") ?? 0,
                ReadLong(root, "default_retention_ms") ?? 0,
                ReadLong(root, "created_unix"), // может отсутствовать (старые заявки)
                ReadString(root, "state")); // отсутствие state = Active (arch/15 §2.1)
        }
        catch (JsonException)
        {
            errors.Add($"/kafka/clusters/{cluster}/config: битый JSON config");
            return new KafkaClusterConfig(0, 0, 0, 0, 0, null, null);
        }
    }

    private static KafkaBrokerDecl BuildBroker(BrokerAcc broker, List<string> errors)
    {
        BrokerResources? resources = null;
        if (broker.ResourcesRaw is not null)
        {
            resources = ParseResources(broker.ResourcesRaw);
            if (resources is null)
                errors.Add(
                    $"/kafka/clusters/-/brokers/{broker.Name}/resources: битый JSON или неверный формат");
        }

        return new KafkaBrokerDecl(broker.Name, broker.State, broker.Role, resources);
    }

    private static BrokerResources? ParseResources(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var cpu = ReadString(root, "cpu");
            var mem = ReadString(root, "mem");
            var disk = ReadString(root, "disk");
            if (cpu is null || mem is null || disk is null)
                return null;

            if (!decimal.TryParse(cpu.Trim(), NumberStyles.Number, CultureInfo.InvariantCulture, out var cpuValue))
                return null;
            if (!TryGi(mem, out var memGi) || !TryGi(disk, out var diskGi))
                return null;

            return new BrokerResources(cpuValue, memGi, diskGi);
        }
        catch (JsonException)
        {
            return null;
        }

        static bool TryGi(string rawGi, out int gi)
        {
            gi = 0;
            return rawGi.Trim().EndsWith("Gi", StringComparison.Ordinal)
                && int.TryParse(rawGi.Trim()[..^2], NumberStyles.Integer, CultureInfo.InvariantCulture, out gi);
        }
    }

    private static KafkaTopicReg BuildTopic(string cluster, string topic, string raw, List<string> errors)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            TopicDesired? desired = null;
            long? desiredUnix = ReadLong(root, "desired_unix");
            if (root.TryGetProperty("desired", out var desiredElement)
                && desiredElement.ValueKind == JsonValueKind.Object)
            {
                desired = new TopicDesired(
                    ReadInt(desiredElement, "partitions"),
                    ReadConfigs(desiredElement));
            }

            return new KafkaTopicReg(
                topic,
                ReadInt(root, "partitions") ?? 0,
                (short?)ReadInt(root, "replication_factor"),
                ReadConfigs(root),
                desired,
                desiredUnix,
                ReadString(root, "desired_by"),
                ReadLong(root, "synced_unix"),
                root.TryGetProperty("missing", out var missing)
                    && missing.ValueKind == JsonValueKind.True);
        }
        catch (JsonException)
        {
            errors.Add($"/kafka/clusters/{cluster}/topics/{topic}: битый JSON топика");
            return new KafkaTopicReg(topic, 0, null, null, null, null, null, null, false);
        }
    }

    // Lifecycle-заявка topics/<T>/desired.{create,delete} (arch/15 §3.1):
    // толерантный разбор; битый JSON или отсутствие requested_unix → parseError,
    // null (заявка битая — воркер не исполняет).
    private static TopicLifecycleTicket? BuildLifecycleTicket(
        string cluster, string topic, string op, string raw, List<string> errors)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var requestedUnix = ReadLong(root, "requested_unix");
            if (requestedUnix is null)
            {
                errors.Add($"/kafka/clusters/{cluster}/topics/{topic}/desired.{op}: нет поля requested_unix");
                return null;
            }

            return new TopicLifecycleTicket(
                topic,
                op,
                ReadInt(root, "partitions") ?? 0,
                (short?)ReadInt(root, "replication_factor"),
                ReadConfigs(root),
                requestedUnix.Value,
                ReadString(root, "requested_by"));
        }
        catch (JsonException)
        {
            errors.Add($"/kafka/clusters/{cluster}/topics/{topic}/desired.{op}: битый JSON заявки");
            return null;
        }
    }

    // configs — объект строковых значений (как отдаёт Kafka, arch/15 §3).
    private static IReadOnlyDictionary<string, string>? ReadConfigs(JsonElement root)
    {
        if (!root.TryGetProperty("configs", out var configs)
            || configs.ValueKind != JsonValueKind.Object
            || configs.GetRawText() == "{}")
            return null;

        var result = new Dictionary<string, string>();
        foreach (var property in configs.EnumerateObject())
            result[property.Name] = property.Value.ToString();

        return result;
    }

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

    private static int? ReadInt(JsonElement root, string name)
    {
        var value = ReadLong(root, name);
        return value is null or > int.MaxValue or < int.MinValue ? null : (int?)value.Value;
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
