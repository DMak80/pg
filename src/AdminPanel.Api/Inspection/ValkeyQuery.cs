using AdminPanel.Core.Valkey;
using AdminPanel.Etcd;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Inspection;

// Запросы инспекции valkey-домена (arch/03 §8.1): сводный список и детали.
public sealed record ValkeyClustersQuery : IQuery<IReadOnlyList<ValkeyClusterSummaryDto>>;

public sealed record ValkeyClusterDetailsQuery(string Cluster) : IQuery<ValkeyClusterDto>;

// Сводная строка списка кластеров (arch/03 §8.2).
public sealed record ValkeyClusterSummaryDto(
    string Name,
    string State,
    int NodesTotal,
    int NodesRunning,
    string? Endpoints,
    bool RotationPending,
    long MaxmemoryBytes,
    string MaxmemoryPolicy);

// Детали кластера: config, нода, ротация (arch/03 §8.2).
public sealed record ValkeyClusterDto(
    string Name,
    string State,
    int NodesTotal,
    long MaxmemoryBytes,
    string MaxmemoryPolicy,
    long? CreatedUnix,
    string? Endpoints,
    IReadOnlyList<ValkeyNodeDto> NodesList,
    ValkeyRotationDto? Rotation);

// Нода node1: state raw + ресурсы + live из PING-пробы (null — проба молчит).
public sealed record ValkeyNodeDto(
    string Name,
    string? State,
    decimal? Cpu,
    int? MemGi,
    int? DiskGi,
    bool? Live,
    string? ProbeError);

// Живая заявка ротации (бейдж UI).
public sealed record ValkeyRotationDto(string Role, long RequestedUnix, string? RequestedBy);

// Core → DTO: чистые функции (arch/03 §8.2; camelCase-зеркало модели).
public static class ValkeyMappers
{
    public static IReadOnlyList<ValkeyClusterSummaryDto> MapSummaries(ValkeySnapshot snapshot)
        => [.. snapshot.Clusters.Select(c => new ValkeyClusterSummaryDto(
            c.Name,
            StateName(c.State),
            c.NodesList.Count,
            c.NodesList.Count(n => n.State == "RUNNING"),
            c.Endpoints,
            c.Rotation is not null,
            c.MaxmemoryBytes,
            c.MaxmemoryPolicy))];

    public static ValkeyClusterDto MapDetails(ValkeyClusterInfo cluster)
        => new(
            cluster.Name,
            StateName(cluster.State),
            cluster.NodesList.Count,
            cluster.MaxmemoryBytes,
            cluster.MaxmemoryPolicy,
            cluster.CreatedUnix,
            cluster.Endpoints,
            [.. cluster.NodesList.Select(n => new ValkeyNodeDto(
                n.Name, n.State, n.Cpu, n.MemGi, n.DiskGi, n.Live, n.ProbeError))],
            cluster.Rotation is null
                ? null
                : new ValkeyRotationDto(cluster.Rotation.Role, cluster.Rotation.RequestedUnix, cluster.Rotation.RequestedBy));

    public static string StateName(ValkeyClusterState state) => state switch
    {
        ValkeyClusterState.NotInitialized => "NOT_INITIALIZED",
        ValkeyClusterState.ToRemove => "TO_REMOVE",
        _ => "ACTIVE",
    };
}

// Список: valkey-снапшот → сводки (отказ «снапшота нет» — 503-семантика pg/kafka).
[InjectAsScoped]
public sealed class ValkeyClustersQueryHandler(IValkeySnapshotReader store)
    : IQueryHandler<ValkeyClustersQuery, IReadOnlyList<ValkeyClusterSummaryDto>>
{
    public ValueTask<Result<IReadOnlyList<ValkeyClusterSummaryDto>>> Handle(
        ValkeyClustersQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<IReadOnlyList<ValkeyClusterSummaryDto>>.Failed(new InspectionModule.SnapshotNotReadyException())
            : Result<IReadOnlyList<ValkeyClusterSummaryDto>>.Success(ValkeyMappers.MapSummaries(snapshot)));
    }
}

// Детали: 404 кластера нет в снапшоте (парсер собирает даже неполные префиксы).
[InjectAsScoped]
public sealed class ValkeyClusterDetailsQueryHandler(IValkeySnapshotReader store)
    : IQueryHandler<ValkeyClusterDetailsQuery, ValkeyClusterDto>
{
    public ValueTask<Result<ValkeyClusterDto>> Handle(ValkeyClusterDetailsQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        if (snapshot is null)
            return ValueTask.FromResult(Result<ValkeyClusterDto>.Failed(
                new InspectionModule.SnapshotNotReadyException()));

        var cluster = snapshot.Clusters.FirstOrDefault(c => c.Name == query.Cluster);
        return ValueTask.FromResult(cluster is null
            ? Result<ValkeyClusterDto>.Failed(new ValkeyClusterNotFound(query.Cluster))
            : Result<ValkeyClusterDto>.Success(ValkeyMappers.MapDetails(cluster)));
    }
}

// Кластер отсутствует в valkey-снапшоте — 404 (детали).
public sealed class ValkeyClusterNotFound(string cluster)
    : Exception($"valkey-кластер {cluster} не найден в снапшоте");
