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
    IReadOnlyList<KafkaRebalanceTicket> Rebalances, // /kafkaworker/rebalances/ (t02, arch/15 §4)
    IReadOnlyList<KafkaReassignmentProgress> Reassignments, // /kafkaworker/reassignments/ (t02)
    IReadOnlyList<WorkerEndpoint> WorkerEndpoints,  // живые ключи /kafkaworker/api/ (arch/02 §2.3.2)
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
    IReadOnlyList<KafkaTopicInfo> Topics,
    // Lifecycle-заявки топиков (t01, arch/15 §3.1): create без факт-ключа —
    // «виртуальная» строка в DTO, delete — бейдж у живого топика.
    IReadOnlyList<KafkaTopicLifecycleTicket>? LifecycleTickets = null,
    // Live-группы из пробы (волна C): пусто — проба не знает кластер.
    IReadOnlyList<KafkaGroupInfo>? Groups = null);

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
    long? SyncedUnix,
    // Live-факт пробы (волна C): партиции с ISR < replicas; null — проба молчит.
    int? UnderReplicatedPartitions = null);

// desired-часть ключа топика (арх/15 §3): управляемые поля + аудит.
public sealed record TopicDesiredDto(
    int? Partitions,
    long? RetentionMs,
    short? MinInSyncReplicas,
    long? RequestedUnix,
    string? RequestedBy);

// Live-группа консьюмеров из пробы (волна C, arch/02 §10.1): state/members/
// totalLag (сумма end − committed по партициям назначения).
public sealed record KafkaGroupInfo(string Group, string? State, int Members, long TotalLag);

// Заявка ротации app-пароля /kafkaworker/rotations/<C> (arch/15 §4).
public sealed record KafkaRotationTicket(string Cluster, long RequestedUnix, string? RequestedBy);

// Lifecycle-заявка топика topics/<T>/desired.{create,delete} (arch/15 §3.1):
// create — параметры (configs развёрнуты в типизированные поля), delete — аудит.
public sealed record KafkaTopicLifecycleTicket(
    string Topic,
    string Op,                 // "create" | "delete" (raw-строка, толерантно)
    int? Partitions,
    short? ReplicationFactor,
    long? RetentionMs,
    short? MinInSyncReplicas,
    long RequestedUnix,
    string? RequestedBy);

/// <summary>Заявка ребалансировки /kafkaworker/rebalances/&lt;C&gt; (arch/15 §4, t02).</summary>
public sealed record KafkaRebalanceTicket(string Cluster, long RequestedUnix, string? RequestedBy);

/// <summary>
/// Прогресс reassignment /kafkaworker/reassignments/&lt;C&gt; (arch/15 §4, t02);
/// отсутствие ключа = операции нет.
/// </summary>
public sealed record KafkaReassignmentProgress(
    string Cluster,
    string Mode,
    string? DrainBroker,
    int PartitionsTotal,
    int PartitionsRemaining,
    long UpdatedUnix,
    string? LastError);
