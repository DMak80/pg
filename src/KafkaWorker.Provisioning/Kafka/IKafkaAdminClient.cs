using KafkaWorker.Core;

namespace KafkaWorker.Provisioning.Kafka;

// Факт кластера из DescribeCluster: живые брокеры + контроллер (K4 готовность).
public sealed record KafkaClusterView(IReadOnlyList<KafkaBrokerView> Brokers, int? ControllerId);

public sealed record KafkaBrokerView(int Id, string Host);

// Исход lifecycle-операции: адаптер классифицирует отчёты Confluent,
// процессы не парсят строки ошибок (arch/15 §3.1).
public enum TopicCreateOutcome
{
    Created,
    AlreadyExists,
}

public enum TopicDeleteOutcome
{
    Deleted,
    NotFound,
}

// Факт топика из DescribeTopics: партиции и реплики каждой партиции — данные
// для guard'а RemoveBroker «на брокере нет реплик» (arch/16 §5 G) и планов
// reassigner'а (arch/16 §5 I). IsrPerPartition — ISR каждой партиции
// (USR-критерий завершения drain, spec t02 §5.2 D4); опционален (= null —
// «ISR не задан», фейк без ISR): реальный адаптер заполняет всегда.
public sealed record KafkaTopicView(
    string Topic,
    int Partitions,
    IReadOnlyList<IReadOnlyList<int>> ReplicasPerPartition,
    IReadOnlyList<IReadOnlyList<int>>? IsrPerPartition = null);

// Группа консьюмеров (ListGroups): id + состояние (коллектор лагов, arch/18 §4).
public sealed record KafkaGroupView(string Group, string State);

// Оффсет на партиции: committed группы либо watermark (Latest).
public sealed record KafkaTopicPartition(string Topic, int Partition);
public sealed record KafkaTopicPartitionOffset(string Topic, int Partition, long Offset);

/// <summary>
/// Seam-интерфейс Kafka-доступа воркера (паттерн Puzzle §7: без Confluent-типов
/// в сигнатурах — юнит-тесты процессов работают на fake, единственное место с
/// Confluent.Kafka — адаптер KafkaAdminClient). Все вызовы возвращают Result:
/// исключения транспорта/таймауты — не паника процессов, а Failed с ошибкой.
/// </summary>
public interface IKafkaAdminClient : IAsyncDisposable
{
    // DescribeCluster: брокеры (id, host) + id контроллера; кластер не поднят → Failed.
    Task<Result<KafkaClusterView>> DescribeClusterAsync(CancellationToken ct);

    // DescribeTopics: топики с партициями, репликами и ISR. includeInternal:
    // false — без __-топиков (TopicSync D: реестр ведёт только юзер-топики);
    // true — все топики, включая __ (reassigner I: drain internal-реплик и
    // guard G по describe-all, arch/16 §5 I/G).
    Task<Result<IReadOnlyList<KafkaTopicView>>> DescribeTopicsAsync(bool includeInternal, CancellationToken ct);

    // Dynamic broker configs конкретного брокера: name → value (только заданные).
    Task<Result<IReadOnlyDictionary<string, string>>> DescribeBrokerConfigsAsync(int brokerId, CancellationToken ct);

    // IncrementalAlterConfigs (Set) на брокере — converge без рестартов (E).
    Task<Result> AlterBrokerConfigsAsync(int brokerId, IReadOnlyDictionary<string, string> configs, CancellationToken ct);

    // Факт-конфиги топика (name → значение; TopicSync волны C, arch/16 §5 D).
    Task<Result<IReadOnlyDictionary<string, string>>> DescribeTopicConfigsAsync(string topic, CancellationToken ct);

    // IncrementalAlterConfigs (Set) на топике — исполнение desired-заявок (D).
    Task<Result> AlterTopicConfigsAsync(string topic, IReadOnlyDictionary<string, string> configs, CancellationToken ct);

    // Увеличение партиций топика до итогового числа (уменьшение Kafka не умеет).
    Task<Result> CreatePartitionsAsync(string topic, int totalPartitions, CancellationToken ct);

    // Создание топика с начальными управляемыми конфигами (lifecycle create, t01):
    // AlreadyExists = исполнено ранее (идемпотентность, arch/15 §3.1).
    Task<Result<TopicCreateOutcome>> CreateTopicAsync(
        string topic, int partitions, short replicationFactor,
        IReadOnlyDictionary<string, string>? configs, CancellationToken ct);

    // Удаление топика (lifecycle delete, t01): NotFound = исполнено ранее
    // (идемпотентность, arch/15 §3.1).
    Task<Result<TopicDeleteOutcome>> DeleteTopicAsync(string topic, CancellationToken ct);

    // Группы консьюмеров кластера (коллектор лагов, arch/18 §4).
    Task<Result<IReadOnlyList<KafkaGroupView>>> ListGroupsAsync(CancellationToken ct);

    // Committed-оффсеты группы по партициям.
    Task<Result<IReadOnlyList<KafkaTopicPartitionOffset>>> ListConsumerGroupOffsetsAsync(
        string group, CancellationToken ct);

    // Watermark-оффсеты (Latest) набора партиций.
    Task<Result<IReadOnlyList<KafkaTopicPartitionOffset>>> ListOffsetsAsync(
        IReadOnlyList<KafkaTopicPartition> partitions, CancellationToken ct);

    // Все ACL кластера (ACL-converge E, t03): фильтрацию делает AclPlan.Diff.
    Task<Result<IReadOnlyList<KafkaAclBinding>>> DescribeAclsAsync(CancellationToken ct);

    // Создание недостающих ACL плана (CreateAcls, t03).
    Task<Result> CreateAclsAsync(IReadOnlyList<KafkaAclBinding> acls, CancellationToken ct);

    // Удаление лишних ACL роли app (DeleteAcls exact-match, t03).
    Task<Result> DeleteAclsAsync(IReadOnlyList<KafkaAclBinding> acls, CancellationToken ct);
}

// Свои ACL-типы (seam: без Confluent-типов в сигнатурах, arch/16 §2.3/E).
public enum KafkaAclResourceType { Unknown, Any, Topic, Group, Cluster, TransactionalId, DelegationToken, User }

public enum KafkaAclPatternType { Unknown, Any, Match, Literal, Prefixed }

public enum KafkaAclOperation { Unknown, Any, All, Read, Write, Create, Delete, Alter, Describe, IdempotentWrite, ClusterAction, DescribeConfigs, AlterConfigs }

public enum KafkaAclPermission { Unknown, Any, Allow, Deny }

public sealed record KafkaAclBinding(
    KafkaAclResourceType ResourceType,
    string ResourceName,
    KafkaAclPatternType PatternType,
    string Principal,
    KafkaAclOperation Operation,
    KafkaAclPermission Permission);

/// <summary>
/// Фабрика клиентов по bootstrap+кредам кластера (t03: SASL_SSL/PLAIN + доверие
/// per-cluster CA arch/15 §5; caPem null — без TLS-доверия, тесты/fake).
/// t05: ШАРЕНАЯ — возвращает кэшированный адаптер per
/// (bootstrap, user, password, caPem); Create — не «новый клиент», а «получить
/// клиент ключа». DisposeAsync возвращённого адаптера — no-op (владение у
/// фабрики-кэша); реальный Dispose — вытеснение из кэша (фон) и остановка
/// host'а. Смена endpoints/кредов/TLS-доверия — другой ключ → другой клиент
/// (инвалидация по построению; клиенты кластеров с разным CA не шарятся).
/// </summary>
public interface IKafkaAdminClientFactory
{
    IKafkaAdminClient Create(string bootstrap, string user, string password, string? caPem);
}
