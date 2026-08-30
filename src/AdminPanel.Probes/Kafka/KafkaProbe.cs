using AdminPanel.Core;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Probes.Kafka;

// [Config]-POCO kafka-пробы: секция AdminPanel:Probes:Kafka (план B6;
// HostMap — общий с pg-пробами AdminPanel:Probes:HostMap, симметрия advertised-
// паттерна: host.docker.internal:<port> → localhost:<port> на стенде).
[Config("AdminPanel:Probes:Kafka")]
public class KafkaProbeOptions
{
    // Проба DescribeCluster — включена по умолчанию (arch/02 §10.1).
    public bool Enabled { get; set; } = true;

    // Отдельный тик пробы. <= 0 — fallback 15 c с LogWarning.
    public double IntervalSeconds { get; set; } = 15;

    // Таймаут DescribeCluster. <= 0 — fallback 3 c.
    public double TimeoutSeconds { get; set; } = 3;
}

// Результат DescribeCluster пробы: id/host брокеров + id контроллера.
public sealed record KafkaProbeView(IReadOnlyList<KafkaProbeBroker> Brokers, int? ControllerId);

public sealed record KafkaProbeBroker(int Id, string Host);

/// <summary>
/// Seam-интерфейс kafka-пробы (план B6): единственное место с Confluent.Kafka —
/// адаптер; юнит-тесты работают на fake. Считает кластер по bootstrap+SASL.
/// </summary>
public interface IKafkaProbeClient
{
    Task<Result<KafkaProbeView>> DescribeClusterAsync(
        string bootstrap, string user, string password, TimeSpan timeout, CancellationToken ct);
}

// Live-факт топика из метаданных (волна C): ISR < replicas по партициям.
public sealed record KafkaProbeTopic(
    string Topic,
    int Partitions,
    int? ReplicationFactor,
    int UnderReplicatedPartitions);

// Группа из DescribeGroups: state/members/партиции назначения (лаги считает
// ProbeLoop по end/committed поверх KafkaGroupLag).
public sealed record KafkaProbeGroupDetail(
    string Group,
    string? State,
    int Members,
    IReadOnlyList<(string Topic, int Partition)> Assignment);

/// <summary>
/// Seam kafka-пробы (B6 + волна C): DescribeCluster — брокеры; DescribeTopics —
/// топики с USR; группы: ListGroups → DescribeGroups; лаги: EndOffsets +
/// Committed по партициям назначения (totalLag — чистая KafkaGroupLag).
/// </summary>
public interface IKafkaProbeRuntimeClient
{
    Task<Result<IReadOnlyList<KafkaProbeTopic>>> DescribeTopicsAsync(
        string bootstrap, string user, string password, TimeSpan timeout, CancellationToken ct);

    Task<Result<IReadOnlyList<string>>> ListGroupsAsync(
        string bootstrap, string user, string password, TimeSpan timeout, CancellationToken ct);

    Task<Result<IReadOnlyList<KafkaProbeGroupDetail>>> DescribeGroupsAsync(
        string bootstrap, string user, string password, IReadOnlyList<string> groups,
        TimeSpan timeout, CancellationToken ct);

    // end-оффсеты (latest) набора партиций.
    Task<Result<IReadOnlyDictionary<(string Topic, int Partition), long>>> EndOffsetsAsync(
        string bootstrap, string user, string password,
        IReadOnlyList<(string Topic, int Partition)> partitions, TimeSpan timeout, CancellationToken ct);

    // закоммиченные оффсеты группы (отсутствие партиции = нет коммита).
    Task<Result<IReadOnlyDictionary<(string Topic, int Partition), long>>> CommittedAsync(
        string bootstrap, string user, string password, string group,
        IReadOnlyList<(string Topic, int Partition)> partitions, TimeSpan timeout, CancellationToken ct);
}
