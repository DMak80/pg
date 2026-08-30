using AdminPanel.Core;

namespace AdminPanel.Core.Kafka;

// Домен-снапшот Kafka (arch/02 §10.1): отдельный от EtcdSnapshot (pg) — своя
// механика тика, теми же настройками endpoints. Immutable; refresher строит
// новый и атомарно заменяет в KafkaSnapshotStore.
public sealed record KafkaSnapshot(
    DateTimeOffset BuiltAtUtc,
    bool EtcdReachable,
    int ConsecutiveFailures,
    IReadOnlyList<KafkaClusterInfo> Clusters,
    IReadOnlyList<KafkaRotationTicket> Rotations,   // /kafkaworker/rotations/ (arch/15 §4)
    IReadOnlyList<ProbeResult> Probes,              // live-проба DescribeCluster (B6+)
    IReadOnlyList<Alert> Alerts,                    // KafkaAlertEngine (arch/03 §7.4)
    IReadOnlyList<KeyParseError> ParseErrors,       // битые JSON kafka-ключей (arch/15 §6)
    int UnknownKeyCount);

// Кластер /kafka/clusters/<C>/ (arch/15 §2): config + state + факт (brokers/topics/endpoints).
public sealed record KafkaClusterInfo(
    string Name,
    KafkaClusterState State,
    int Brokers,                 // config.brokers (заявка)
    int ReplicationFactor,
    int MinInSyncReplicas,
    int DefaultPartitions,
    long DefaultRetentionMs,
    long? CreatedUnix,
    string? Endpoints,           // null/пусто — воркер не дописал (алерт у Active)
    IReadOnlyList<KafkaBrokerInfo> BrokersList,
    IReadOnlyList<KafkaTopicInfo> Topics);

// Состояние кластера: config.state (arch/15 §2); отсутствие = Active.
public enum KafkaClusterState
{
    Active,
    NotInitialized,
    ToRemove,
}

// Маппинг config.state → enum (arch/15 §6: незнакомое значение — толерантно, Active).
public static class KafkaClusterStates
{
    public static KafkaClusterState Parse(string? raw) => raw switch
    {
        "NOT_INITIALIZED" => KafkaClusterState.NotInitialized,
        "TO_REMOVE" => KafkaClusterState.ToRemove,
        _ => KafkaClusterState.Active,
    };
}

// Брокер broker<k>: state — raw-строка (NOT_INITIALIZED|PROVISIONING|RUNNING|
// UNREACHABLE|REMOVING|TO_REMOVE; толерантно к новым), role пишет воркер (план
// provisioning фиксирует контроллера навсегда).
public sealed record KafkaBrokerInfo(
    string Name,
    string? State,
    string? Role,
    decimal? Cpu,
    int? MemGi,
    int? DiskGi);

// Топик topics/<T> (arch/15 §3): факт (partitions/RF/configs) + desired-заявка панели.
public sealed record KafkaTopicInfo(
    string Name,
    int Partitions,
    short? ReplicationFactor,
    long? RetentionMs,
    short? MinInSyncReplicas,
    TopicDesiredDto? Desired,
    bool Missing,
    long? SyncedUnix);

// desired-часть ключа топика (арх/15 §3): управляемые поля + аудит.
public sealed record TopicDesiredDto(
    int? Partitions,
    long? RetentionMs,
    short? MinInSyncReplicas,
    long? RequestedUnix,
    string? RequestedBy);

// Заявка ротации app-пароля /kafkaworker/rotations/<C> (arch/15 §4).
public sealed record KafkaRotationTicket(string Cluster, long RequestedUnix, string? RequestedBy);
