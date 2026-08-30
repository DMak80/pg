using AdminPanel.Core;

namespace AdminPanel.Core.Kafka;

// Live-данные кластера из пробы DescribeCluster (B6, arch/02 §10.1): панель
// обогащает детали кластера (brokerId/live в DTO) поверх etcd-снапшота.
public sealed record KafkaClusterLive(
    string Cluster,
    DateTimeOffset AtUtc,
    IReadOnlyList<KafkaBrokerLive> Brokers,
    // Live-топики (волна C): партиции и under-replicated по ISR.
    IReadOnlyList<KafkaTopicRuntime>? Topics = null,
    // Live-группы: state/members/totalLag.
    IReadOnlyList<KafkaGroupInfo>? Groups = null);

public sealed record KafkaBrokerLive(int Id, string Host, bool Controller);

// Live-факт топика из пробы (волна C): партиции/RF и USR (ISR < replicas).
public sealed record KafkaTopicRuntime(
    string Topic,
    int Partitions,
    int? ReplicationFactor,
    int UnderReplicatedPartitions);

// Состояние kafka-проб (pg-аналог ProbeState): одна атомарная замена за тик;
// Results → KafkaSnapshot.Probes (refresher вносит), Clusters → обогащение DTO.
public sealed record KafkaProbeState(
    DateTimeOffset AtUtc,
    IReadOnlyList<ProbeResult> Results,
    IReadOnlyDictionary<string, KafkaClusterLive> Clusters);
