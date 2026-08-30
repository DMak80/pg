using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// Чистая decision-функция converge (arch/16 §5 E, порт describe→decide→act
/// Puzzle §7.2): маппинг полей заявки → Kafka-конфиги; возвращает только
/// отличающиеся от фактических (пусто = no-op, идемпотентность).
/// </summary>
public static class ConvergeDecider
{
    // Маппинг полей заявки → dynamic broker configs (arch/16 §5 E).
    public static readonly IReadOnlyDictionary<string, string> RequestToBrokerConfig = new Dictionary<string, string>
    {
        ["default_retention_ms"] = "log.retention.ms",
        ["default_partitions"] = "num.partitions",
        ["replication_factor"] = "default.replication.factor",
        ["min_insync_replicas"] = "min.insync.replicas",
    };

    public static IReadOnlyDictionary<string, string> Target(KafkaClusterConfig config)
        => new Dictionary<string, string>
        {
            ["log.retention.ms"] = config.DefaultRetentionMs.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["num.partitions"] = config.DefaultPartitions.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["default.replication.factor"] = config.ReplicationFactor.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["min.insync.replicas"] = config.MinInSyncReplicas.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };

    // Diff цели и факта: только отличающиеся значения (факт — строковые значения Kafka).
    public static IReadOnlyDictionary<string, string> Decide(
        KafkaClusterConfig config,
        IReadOnlyDictionary<string, string> current)
    {
        var result = new Dictionary<string, string>();
        foreach (var (name, target) in Target(config))
        {
            if (!current.TryGetValue(name, out var actual) || actual != target)
                result[name] = target;
        }

        return result;
    }
}

/// <summary>
/// Converge mutable-конфигов кластера как dynamic broker configs (arch/16 §5 E):
/// describe по одному брокеру → decide → при отличии IncrementalAlterConfigs
/// (Set) на ВСЕХ брокерах — применяется без рестартов. Совпадают → no-op.
/// </summary>
public sealed class ClusterConfigConverger(IKafkaAdminClientFactory adminFactory) : IClusterConfigConverger
{
    public async Task<Result> ApplyAsync(
        string cluster, string bootstrap, string user, string password, KafkaClusterConfig config, CancellationToken ct)
    {
        await using var admin = adminFactory.Create(bootstrap, user, password);

        // Describe: перечень брокеров + текущие dynamic-конфиги первого
        // (dynamic broker configs кластер-уровня: default.*, применяем на всех).
        var view = await admin.DescribeClusterAsync(ct);
        if (!view.IsSuccess)
            return Result.Failed(view.Error!);
        if (view.Value.Brokers.Count == 0)
            return Result.Failed(new ApplicationException($"converge {cluster}: список брокеров пуст"));

        var current = await admin.DescribeBrokerConfigsAsync(view.Value.Brokers[0].Id, ct);
        if (!current.IsSuccess)
            return Result.Failed(current.Error!);

        var changes = ConvergeDecider.Decide(config, current.Value);
        if (changes.Count == 0)
            return Result.Success(); // уже сошлось — no-op (не дёргаем alter)

        foreach (var broker in view.Value.Brokers)
        {
            var altered = await admin.AlterBrokerConfigsAsync(broker.Id, changes, ct);
            if (!altered.IsSuccess)
                return altered;
        }

        return Result.Success();
    }
}
