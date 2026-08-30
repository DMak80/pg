using KafkaWorker.Core;

namespace KafkaWorker.Provisioning.Kafka;

// Факт кластера из DescribeCluster: живые брокеры + контроллер (K4 готовность).
public sealed record KafkaClusterView(IReadOnlyList<KafkaBrokerView> Brokers, int? ControllerId);

public sealed record KafkaBrokerView(int Id, string Host);

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
}

/// <summary>Фабрика клиентов по bootstrap+кредам кластера (SASL/PLAIN, arch/15 §5).</summary>
public interface IKafkaAdminClientFactory
{
    IKafkaAdminClient Create(string bootstrap, string user, string password);
}
