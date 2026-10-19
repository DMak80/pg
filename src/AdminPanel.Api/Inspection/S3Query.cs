using AdminPanel.Core.S3;
using AdminPanel.Etcd;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Inspection;

// Запросы инспекции s3-домена (arch/03 §12.1): сводный список и детали.
public sealed record S3ClustersQuery : IQuery<IReadOnlyList<S3ClusterSummaryDto>>;

public sealed record S3ClusterDetailsQuery(string Cluster) : IQuery<S3ClusterDto>;

// Сводная строка списка кластеров (arch/03 §12.2).
public sealed record S3ClusterSummaryDto(
    string Name,
    string State,
    int NodesTotal,
    int NodesRunning,
    string? Endpoints,
    string? ConnectivityState,   // OK|DEGRADED|DOWN|null
    string? SyncState,           // SYNCED|LAGGING|DIVERGED|null
    bool RotationPending);

// Детали кластера: config, ноды, links, denylist, endpoints, зонды, ротация.
public sealed record S3ClusterDto(
    string Name,
    string State,
    string Platform,
    int NodesDeclared,
    bool EtcdBackup,
    string? RootUser,                          // s3w-<C>-root (имя — не секрет)
    IReadOnlyList<S3AccessKeyDto>? AccessKeys, // [{name, policy}] БЕЗ значений
    long? CreatedUnix,
    string? Endpoints,
    IReadOnlyList<S3NodeDto> NodesList,
    IReadOnlyList<S3LinkDto> Links,
    IReadOnlyList<string> Denylist,
    S3ConnectivityDto? Connectivity,
    S3SyncDto? Sync,
    S3RotationDto? Rotation);

// Ключ доступа кластера в инспекции: имя + policy; значения AccessKey/SecretKey не попадают.
public sealed record S3AccessKeyDto(string Name, string Policy);

// Нода: raw state + размещение + ресурсы + занятость + denied (denylist).
public sealed record S3NodeDto(
    string Name,
    string? State,
    string? PlacedOn,
    S3PlacementDto? Placement,
    S3ResourcesDto? Resources,
    S3UsageDto? Usage,
    bool Denied);

public sealed record S3PlacementDto(
    string? DockerHost, string? SwarmNode, IReadOnlyDictionary<string, string>? SwarmLabels);
public sealed record S3ResourcesDto(decimal? Cpu, int? MemGi, int? DiskGi);
public sealed record S3UsageDto(long UsedBytes, long TotalBytes, long UpdatedUnix);
public sealed record S3LinkDto(string A, string B);
public sealed record S3ConnectivityDto(
    string? State, long? CheckedUnix,
    IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> Links, string? ProbeError);
public sealed record S3SyncDto(
    string? State, long? CheckedUnix, IReadOnlyList<S3SyncPairDto> Pairs, string? LastError);
public sealed record S3SyncPairDto(
    string From, string To, long Missing, long Extra, long? LagBytes, long? LagObjects);
public sealed record S3RotationDto(long RequestedUnix, string? RequestedBy);

// Пул S3 для грани «Хранилище бэкапов» (t36, adminpanel/02 §12.4): проекция
// ТОГО ЖЕ домена S3Snapshot — без ключей/секретов (GET /api/backups/s3-pools).
public sealed record S3PoolDto(
    string Name,
    bool EtcdBackup,
    string? RootUser,                          // s3w-<C>-root (имя — не секрет)
    IReadOnlyList<S3AccessKeyDto>? AccessKeys, // [{name, policy}] БЕЗ значений
    bool Active,
    IReadOnlyList<S3PoolNodeDto> Nodes,
    IReadOnlyList<string> Endpoints);

public sealed record S3PoolNodeDto(string Name, string? State);

// Core → DTO: чистые функции (arch/03 §12.2; camelCase-зеркало модели).
public static class S3Mappers
{
    // Пулы для модала «Пул бэкапов» (02 §12.4): Active = state отсутствует;
    // endpoints-строка "h1:p1,h2:p2" → упорядоченный массив (битые элементы — долой).
    public static IReadOnlyList<S3PoolDto> MapPools(IReadOnlyList<S3ClusterInfo> clusters)
        => [.. clusters.Select(c => new S3PoolDto(
            c.Name,
            c.EtcdBackup,
            c.Secrets?.RootUser,
            c.Secrets is null ? null : [.. c.Secrets.AccessKeys.Select(k => new S3AccessKeyDto(k.Name, k.Policy))],
            c.State == S3ClusterState.Active,
            [.. c.NodesList.Select(n => new S3PoolNodeDto(n.Name, n.State))],
            SplitEndpoints(c.Endpoints)))];

    // "h1:18001,h2:18002" → упорядоченный массив; без валидного host:port — элемент долой.
    private static string[] SplitEndpoints(string? raw)
        => raw is null ? []
            : [.. raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Where(e =>
                {
                    var colon = e.LastIndexOf(':');
                    return colon > 0 && colon < e.Length - 1
                        && int.TryParse(e[(colon + 1)..], out var port) && port > 0;
                })];

    public static IReadOnlyList<S3ClusterSummaryDto> MapSummaries(S3Snapshot snapshot)
        => [.. snapshot.Clusters.Select(c => new S3ClusterSummaryDto(
            c.Name,
            StateName(c.State),
            c.NodesList.Count,
            c.NodesList.Count(n => n.State == "RUNNING"),
            c.Endpoints,
            c.Connectivity?.State,
            c.Sync?.State,
            c.Rotation is not null))];

    public static S3ClusterDto MapDetails(S3ClusterInfo cluster) => new(
        cluster.Name,
        StateName(cluster.State),
        cluster.Platform,
        cluster.NodesDeclared,
        cluster.EtcdBackup,
        cluster.Secrets?.RootUser,
        cluster.Secrets is null ? null : [.. cluster.Secrets.AccessKeys.Select(k => new S3AccessKeyDto(k.Name, k.Policy))],
        cluster.CreatedUnix,
        cluster.Endpoints,
        [.. cluster.NodesList.Select(n => new S3NodeDto(
            n.Name, n.State, n.PlacedOn,
            n.Placement is null ? null : new S3PlacementDto(
                n.Placement.DockerHost, n.Placement.SwarmNode, n.Placement.SwarmLabels),
            n.Resources is null ? null : new S3ResourcesDto(
                n.Resources.Cpu, n.Resources.MemGi, n.Resources.DiskGi),
            n.Usage is null ? null : new S3UsageDto(
                n.Usage.UsedBytes, n.Usage.TotalBytes, n.Usage.UpdatedUnix),
            cluster.DenyList.Contains(n.Name, StringComparer.Ordinal)))],
        [.. cluster.Links.Select(l => new S3LinkDto(l.A, l.B))],
        cluster.DenyList,
        cluster.Connectivity is null ? null : new S3ConnectivityDto(
            cluster.Connectivity.State, cluster.Connectivity.CheckedUnix,
            cluster.Connectivity.Links, cluster.Connectivity.ProbeError),
        cluster.Sync is null ? null : new S3SyncDto(
            cluster.Sync.State, cluster.Sync.CheckedUnix,
            [.. cluster.Sync.Pairs.Select(p => new S3SyncPairDto(
                p.From, p.To, p.Missing, p.Extra, p.LagBytes, p.LagObjects))],
            cluster.Sync.LastError),
        cluster.Rotation is null ? null : new S3RotationDto(
            cluster.Rotation.RequestedUnix, cluster.Rotation.RequestedBy));

    public static string StateName(S3ClusterState state) => state switch
    {
        S3ClusterState.NotInitialized => "NOT_INITIALIZED",
        S3ClusterState.ToRemove => "TO_REMOVE",
        _ => "ACTIVE",
    };
}

// Список: отказ «снапшота нет» — 503-семантика pg/kafka/valkey.
[InjectAsScoped]
public sealed class S3ClustersQueryHandler(IS3SnapshotReader store)
    : IQueryHandler<S3ClustersQuery, IReadOnlyList<S3ClusterSummaryDto>>
{
    public ValueTask<Result<IReadOnlyList<S3ClusterSummaryDto>>> Handle(
        S3ClustersQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<IReadOnlyList<S3ClusterSummaryDto>>.Failed(new InspectionModule.SnapshotNotReadyException())
            : Result<IReadOnlyList<S3ClusterSummaryDto>>.Success(S3Mappers.MapSummaries(snapshot)));
    }
}

// Детали: 404 кластера нет в снапшоте (парсер собирает даже неполные префиксы).
[InjectAsScoped]
public sealed class S3ClusterDetailsQueryHandler(IS3SnapshotReader store)
    : IQueryHandler<S3ClusterDetailsQuery, S3ClusterDto>
{
    public ValueTask<Result<S3ClusterDto>> Handle(S3ClusterDetailsQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        if (snapshot is null)
            return ValueTask.FromResult(Result<S3ClusterDto>.Failed(
                new InspectionModule.SnapshotNotReadyException()));

        var cluster = snapshot.Clusters.FirstOrDefault(c => c.Name == query.Cluster);
        return ValueTask.FromResult(cluster is null
            ? Result<S3ClusterDto>.Failed(new S3ClusterNotFound(query.Cluster))
            : Result<S3ClusterDto>.Success(S3Mappers.MapDetails(cluster)));
    }
}

// Кластер отсутствует в s3-снапшоте — 404 (детали).
public sealed class S3ClusterNotFound(string cluster)
    : Exception($"S3 cluster {cluster} not found in snapshot");
