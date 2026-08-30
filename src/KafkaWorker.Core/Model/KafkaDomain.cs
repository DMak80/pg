namespace KafkaWorker.Core.Model;

// Доменная модель контроль-плейна /kafka/clusters/<C>/ (arch/15 §2–3).
// Immutable records; state-значения — строки (толерантность к новым значениям,
// arch/15 §6): парсер не знает enum'ов состояний.

/// <summary>
/// Заявка-конфиг кластера (ключ config): B/R/M/P/X + created_unix;
/// State — только у невыполненных заявок (null = Active, arch/15 §2.1).
/// </summary>
public sealed record KafkaClusterConfig(
    int Brokers,
    int ReplicationFactor,
    int MinInSyncReplicas,
    int DefaultPartitions,
    long DefaultRetentionMs,
    long? CreatedUnix,
    string? State);

/// <summary>Заявка ресурсов ноды (ключ brokers/&lt;b&gt;/resources): "2", "4Gi", "40Gi".</summary>
public sealed record BrokerResources(decimal Cpu, int MemGi, int DiskGi);

/// <summary>Декларация брокера: state-строка + роль (controller|broker) + ресурсы.</summary>
public sealed record KafkaBrokerDecl(string Name, string? State, string? Role, BrokerResources? Resources);

/// <summary>Desired-заявка конфигов топика (управляемые поля, arch/15 §3).</summary>
public sealed record TopicDesired(
    int? Partitions,
    IReadOnlyDictionary<string, string>? Configs);

/// <summary>
/// Реестровая запись топика (ключ topics/&lt;T&gt;): факт (partitions/RF/configs/
/// synced_unix) + заявка desired (null = заявки нет) + missing (топик исчез
/// из Kafka при живой заявке).
/// </summary>
public sealed record KafkaTopicReg(
    string Topic,
    int Partitions,
    short? ReplicationFactor,
    IReadOnlyDictionary<string, string>? Configs,
    TopicDesired? Desired,
    long? DesiredUnix,
    string? DesiredBy,
    long? SyncedUnix,
    bool Missing);

/// <summary>Операции lifecycle-заявки топика (arch/15 §3.1).</summary>
public static class TopicLifecycleOps
{
    public const string Create = "create";
    public const string Delete = "delete";
}

/// <summary>
/// Lifecycle-заявка топика (leaf-ключ topics/&lt;T&gt;/desired.create|delete,
/// arch/15 §3.1): create — параметры создания (configs — начальные, управляемые),
/// delete — только аудит. RequestedUnix обязателен (панель пишет аудит всегда;
/// образец толерантности — KafkaRotationTicket панели).
/// </summary>
public sealed record TopicLifecycleTicket(
    string Topic,
    string Op,
    int Partitions,
    short? ReplicationFactor,
    IReadOnlyDictionary<string, string>? Configs,
    long RequestedUnix,
    string? RequestedBy);

/// <summary>
/// Снимок кластера после разбора префикса: config + брокеры + топики;
/// Endpoints/AppUser/AppPassword — дискавери-поля (arch/15 §2/§5), читаются
/// процессами для AdminClient-доступа и RMW endpoints.
/// </summary>
public sealed record KafkaClusterSnapshot(
    string Cluster,
    KafkaClusterConfig Config,
    IReadOnlyList<KafkaBrokerDecl> Brokers,
    IReadOnlyList<KafkaTopicReg> Topics,
    IReadOnlyList<string> ParseErrors,
    int UnknownKeys,
    string? Endpoints = null,
    string? AppUser = null,
    string? AppPassword = null,
    IReadOnlyList<TopicLifecycleTicket>? LifecycleTickets = null);
