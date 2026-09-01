using KafkaWorker.Core;
using KafkaWorker.Core.Writing;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.App.Api.Operations;

// Общие хелперы чтения kafka-ключей для хендлеров API (task etcd-via-worker-api):
// порт панельного KafkaCommandHelpers с единственной заменой — активный endpoint
// не из снапшота панели, а свой список с failover (EtcdFailover).
internal static class KafkaApiHelpers
{
    // Чтение config-ключа с revision: (значение, mod_revision) — для RMW-мутаций.
    internal sealed record ConfigRead(KafkaConfigJson? Value, long? Revision, Exception? Error);

    internal static async Task<ConfigRead> ReadConfigAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, ConfigKey(cluster), ct));
        if (!range.IsSuccess)
            return new ConfigRead(null, null, range.Error!);

        var kv = range.Value.FirstOrDefault(k => k.Key == ConfigKey(cluster));
        if (kv is null)
            return new ConfigRead(null, null, null);
        var config = KafkaConfigJson.TryParse(kv.Value);
        return config is null
            ? new ConfigRead(null, null, new InvalidKafkaConfigException(cluster))
            : new ConfigRead(config, (long)kv.ModRevision, null);
    }

    internal static async Task<Result<IReadOnlyList<int>>> ReadBrokerNamesAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, $"/kafka/clusters/{cluster}/brokers/", ct));
        if (!range.IsSuccess)
            return Result<IReadOnlyList<int>>.Failed(range.Error!);

        var ids = new HashSet<int>();
        foreach (var kv in range.Value)
        {
            // /kafka/clusters/<C>/brokers/broker<k>/{state,role,resources}:
            // ["", "kafka", "clusters", <C>, "brokers", "broker<k>", leaf].
            var segments = kv.Key.Split('/');
            if (segments.Length == 7 && segments[5].StartsWith("broker", StringComparison.Ordinal)
                && int.TryParse(segments[5]["broker".Length..], out var id))
                ids.Add(id);
        }

        return Result<IReadOnlyList<int>>.Success((IReadOnlyList<int>)ids.OrderBy(i => i).ToList());
    }

    internal static async Task<Result<string?>> ReadKeyAsync(
        IEtcdGateway gateway, string[] endpoints, string key, CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, key, ct));
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!);
        return Result<string?>.Success(range.Value.FirstOrDefault(kv => kv.Key == key)?.Value);
    }

    internal static string ConfigKey(string cluster) => $"/kafka/clusters/{cluster}/config";

    internal static string BrokerKey(string cluster, string broker, string leaf)
        => $"/kafka/clusters/{cluster}/brokers/{broker}/{leaf}";

    internal static string TopicKey(string cluster, string topic)
        => $"/kafka/clusters/{cluster}/topics/{topic}";

    // Leaf-ключ lifecycle-заявки (arch/15 §3.1): тот же формат, что у воркера.
    internal static string LifecycleKey(string cluster, string topic, string op)
        => $"/kafka/clusters/{cluster}/topics/{topic}/desired.{op}";

    // Чтение ключа топика с revision для RMW: (json, mod_revision, ошибка).
    internal sealed record TopicKeyRead(KafkaTopicKeyJson? Json, long? Revision, Exception? Error);

    internal static async Task<TopicKeyRead> ReadTopicKeyAsync(
        IEtcdGateway gateway, string[] endpoints, string key, CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, key, ct));
        if (!range.IsSuccess)
            return new TopicKeyRead(null, null, range.Error!);

        var kv = range.Value.FirstOrDefault(k => k.Key == key);
        if (kv is null)
            return new TopicKeyRead(null, null, null);

        // /kafka/clusters/<C>/topics/<T>: ["", "kafka", "clusters", <C>, "topics", <T>].
        var segments = key.Split('/');
        var json = KafkaTopicKeyJson.TryParse(kv.Value);
        return json is null
            ? new TopicKeyRead(null, null, new InvalidKafkaTopicKeyException(segments[3], segments[5]))
            : new TopicKeyRead(json, (long)kv.ModRevision, null);
    }
}
