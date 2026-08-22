using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Inspection;

// Запрос сводного списка кластеров (arch/03 §1 GET /api/clusters).
public sealed record ClustersQuery : IQuery<IReadOnlyList<ClusterSummaryDto>>;

// Сводка кластера — UI-таблица Clusters (arch/03 §3; spec §3.2); dbname null у incomplete.
public sealed record ClusterSummaryDto(
    string Name,
    string? DbName,
    int BucketsCount,
    bool Incomplete,
    int ShardsTotal,
    int ShardsWithMaster,
    int ActiveMoves);

// Снапшот → сводки: чистая функция; порядок кластеров — как в снапшоте (spec §3.3).
public static class ClustersMapper
{
    public static IReadOnlyList<ClusterSummaryDto> Map(IReadOnlyList<ClusterInfo> clusters)
        => [.. clusters.Select(c => new ClusterSummaryDto(
            c.Name,
            c.DbName,
            c.BucketsCount,
            c.Incomplete,
            c.Shards.Count,
            c.Shards.Count(s => s.MasterAddress is not null),
            c.Buckets.Count(b => b.State != BucketState.Active)))];
}

// Хендлер: store → отказ «снапшота нет» или маппер (spec §3.12).
[InjectAsScoped]
public sealed class ClustersQueryHandler(ISnapshotStore store)
    : IQueryHandler<ClustersQuery, IReadOnlyList<ClusterSummaryDto>>
{
    public ValueTask<Result<IReadOnlyList<ClusterSummaryDto>>> Handle(ClustersQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<IReadOnlyList<ClusterSummaryDto>>.Failed(new InspectionModule.SnapshotNotReadyException())
            : Result<IReadOnlyList<ClusterSummaryDto>>.Success(ClustersMapper.Map(snapshot.Clusters)));
    }
}
