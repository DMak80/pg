namespace AdminPanel.Api.Operations.Kafka;

// ===== Тела запросов kafka-мутаций (arch/02 §10.3; arch/03 §7.2) =====
// Биндятся Minimal API как JSON и уходят в API KafkaWorker как есть (панель
// не валидирует — источник истины воркер, spec §3.4). Дубль воркерских DTO
// осознан (t08).

// Тело POST /api/kafka/clusters: nullable-поля с дефолтами 3/3/2/12/7д/2/2/20.
public sealed record CreateKafkaClusterRequest(
    string? Name,
    int? Brokers = null,
    int? ReplicationFactor = null,
    int? MinInSyncReplicas = null,
    int? DefaultPartitions = null,
    long? DefaultRetentionMs = null,
    decimal? Cpu = null,
    int? MemGi = null,
    int? DiskGi = null);

// Тело PUT /api/kafka/clusters/{c}/config — хотя бы одно поле (02 §10.2-3).
public sealed record KafkaConfigUpdateRequest(
    int? ReplicationFactor = null,
    int? MinInSyncReplicas = null,
    int? DefaultPartitions = null,
    long? DefaultRetentionMs = null);

// Тело POST /api/kafka/clusters/{c}/brokers — ресурсы нового брокера (02 §10.2-4).
public sealed record AddKafkaBrokerRequest(
    decimal? Cpu = null,
    int? MemGi = null,
    int? DiskGi = null);

// Тело PUT /api/kafka/clusters/{c}/topics/{t} (02 §10.2-7): хотя бы одно поле;
// partitions — только увеличение (проверяет воркер).
public sealed record TopicDesiredRequest(
    int? Partitions = null,
    long? RetentionMs = null,
    int? MinInSyncReplicas = null);

// Тело POST /api/kafka/clusters/{c}/topics (02 §10.2-9): name обязателен;
// partitions/RF дефолтятся из config кластера; retention/minISR опциональны.
public sealed record CreateTopicRequest(
    string? Name,
    int? Partitions = null,
    short? ReplicationFactor = null,
    long? RetentionMs = null,
    short? MinInSyncReplicas = null);
