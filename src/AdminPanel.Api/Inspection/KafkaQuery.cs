using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Inspection;

// Запросы инспекции kafka-домена (arch/03 §7.1): сводный список и детали.
public sealed record KafkaClustersQuery : IQuery<IReadOnlyList<KafkaClusterSummaryDto>>;

public sealed record KafkaClusterDetailsQuery(string Cluster) : IQuery<KafkaClusterDto>;

// Сводная строка списка кластеров (arch/03 §7.2).
public sealed record KafkaClusterSummaryDto(
    string Name,
    string State,
    int BrokersTotal,
    int BrokersRunning,
    int TopicsCount,
    string? Endpoints,
    bool RotationPending);

// Детали кластера: config, брокеры, топики, ротация (groups — волна C).
public sealed record KafkaClusterDto(
    string Name,
    string State,
    int Brokers,
    int ReplicationFactor,
    int MinInSyncReplicas,
    int DefaultPartitions,
    long DefaultRetentionMs,
    long? CreatedUnix,
    string? Endpoints,
    IReadOnlyList<KafkaBrokerDto> BrokersList,
    IReadOnlyList<KafkaTopicDto> Topics,
    KafkaRotationTicketDto? Rotation);

public sealed record KafkaBrokerDto(
    string Name,
    string? State,
    string? Role,
    decimal? Cpu,
    int? MemGi,
    int? DiskGi);

public sealed record KafkaTopicDto(
    string Name,
    int Partitions,
    short? ReplicationFactor,
    long? RetentionMs,
    short? MinInSyncReplicas,
    TopicDesiredDto? Desired,
    bool Missing,
    long? SyncedUnix);

public sealed record TopicDesiredDto(
    int? Partitions,
    long? RetentionMs,
    short? MinInSyncReplicas,
    long? RequestedUnix,
    string? RequestedBy);

public sealed record KafkaRotationTicketDto(long RequestedUnix, string? RequestedBy);

// Core → DTO: чистые функции (arch/03 §7.2; camelCase-зеркало модели B2).
public static class KafkaMappers
{
    public static IReadOnlyList<KafkaClusterSummaryDto> MapSummaries(KafkaSnapshot snapshot)
        => [.. snapshot.Clusters.Select(c => MapSummary(
            c, snapshot.Rotations.Any(r => r.Cluster == c.Name)))];

    // Ротационный бейдж — только у живого кластера (заявка не переживает демонтаж).
    public static KafkaClusterSummaryDto MapSummary(KafkaClusterInfo cluster, bool rotationPending)
        => new(
            cluster.Name,
            StateName(cluster.State),
            cluster.BrokersList.Count,
            cluster.BrokersList.Count(b => b.State == "RUNNING"),
            cluster.Topics.Count,
            cluster.Endpoints,
            rotationPending);

    public static KafkaClusterDto MapDetails(
        KafkaClusterInfo cluster, IReadOnlyList<KafkaRotationTicket> rotations)
    {
        var rotation = rotations.FirstOrDefault(r => r.Cluster == cluster.Name);
        return new KafkaClusterDto(
            cluster.Name,
            StateName(cluster.State),
            cluster.Brokers,
            cluster.ReplicationFactor,
            cluster.MinInSyncReplicas,
            cluster.DefaultPartitions,
            cluster.DefaultRetentionMs,
            cluster.CreatedUnix,
            cluster.Endpoints,
            [.. cluster.BrokersList.Select(b => new KafkaBrokerDto(
                b.Name, b.State, b.Role, b.Cpu, b.MemGi, b.DiskGi))],
            [.. cluster.Topics.Select(t => new KafkaTopicDto(
                t.Name, t.Partitions, t.ReplicationFactor, t.RetentionMs, t.MinInSyncReplicas,
                t.Desired is null ? null : new TopicDesiredDto(
                    t.Desired.Partitions, t.Desired.RetentionMs, t.Desired.MinInSyncReplicas,
                    t.Desired.RequestedUnix, t.Desired.RequestedBy),
                t.Missing, t.SyncedUnix))],
            rotation is null ? null : new KafkaRotationTicketDto(rotation.RequestedUnix, rotation.RequestedBy));
    }

    public static string StateName(KafkaClusterState state) => state switch
    {
        KafkaClusterState.NotInitialized => "NOT_INITIALIZED",
        KafkaClusterState.ToRemove => "TO_REMOVE",
        _ => "ACTIVE",
    };
}

// Список: kafka-снапшот → сводки (отказ «снапшота нет» — 503-семантика pg).
[InjectAsScoped]
public sealed class KafkaClustersQueryHandler(IKafkaSnapshotReader store)
    : IQueryHandler<KafkaClustersQuery, IReadOnlyList<KafkaClusterSummaryDto>>
{
    public ValueTask<Result<IReadOnlyList<KafkaClusterSummaryDto>>> Handle(
        KafkaClustersQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<IReadOnlyList<KafkaClusterSummaryDto>>.Failed(new InspectionModule.SnapshotNotReadyException())
            : Result<IReadOnlyList<KafkaClusterSummaryDto>>.Success(KafkaMappers.MapSummaries(snapshot)));
    }
}

// Детали: 404 кластера нет в снапшоте (парсер собирает даже неполные префиксы).
[InjectAsScoped]
public sealed class KafkaClusterDetailsQueryHandler(IKafkaSnapshotReader store)
    : IQueryHandler<KafkaClusterDetailsQuery, KafkaClusterDto>
{
    public ValueTask<Result<KafkaClusterDto>> Handle(KafkaClusterDetailsQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        if (snapshot is null)
            return ValueTask.FromResult(Result<KafkaClusterDto>.Failed(
                new InspectionModule.SnapshotNotReadyException()));

        var cluster = snapshot.Clusters.FirstOrDefault(c => c.Name == query.Cluster);
        return ValueTask.FromResult(cluster is null
            ? Result<KafkaClusterDto>.Failed(new KafkaClusterNotFound(query.Cluster))
            : Result<KafkaClusterDto>.Success(KafkaMappers.MapDetails(cluster, snapshot.Rotations)));
    }
}

// Кластер отсутствует в kafka-снапшоте — 404 (детали).
public sealed class KafkaClusterNotFound(string cluster)
    : Exception($"kafka-кластер {cluster} не найден в снапшоте");
