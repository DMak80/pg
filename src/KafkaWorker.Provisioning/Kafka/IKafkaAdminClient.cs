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
// для guard'а RemoveBroker «на брокере нет реплик» (arch/16 §5 G).
public sealed record KafkaTopicView(
    string Topic,
    int Partitions,
    IReadOnlyList<IReadOnlyList<int>> ReplicasPerPartition);

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

    // DescribeTopics: все топики с партициями и репликами (волнам B/C).
    Task<Result<IReadOnlyList<KafkaTopicView>>> DescribeTopicsAsync(CancellationToken ct);

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
}

/// <summary>Фабрика клиентов по bootstrap+кредам кластера (SASL/PLAIN, arch/15 §5).</summary>
public interface IKafkaAdminClientFactory
{
    IKafkaAdminClient Create(string bootstrap, string user, string password);
}
