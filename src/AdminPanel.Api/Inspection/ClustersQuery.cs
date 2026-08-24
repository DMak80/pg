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
    bool NotInitialized,
    bool ToRemove,
    int ShardsTotal,
    int ShardsWithMaster,
    int ActiveMoves,
    // Как в деталях (arch/03 §2): false ⟺ 1 бакет и ≤1 шард — нешардированная
    // БД; список рисует прочерк в «Бакеты»/«Шарды».
    bool Sharded);

// Снапшот → сводки: чистая функция; порядок кластеров — как в снапшоте (spec §3.3).
public static class ClustersMapper
{
    public static IReadOnlyList<ClusterSummaryDto> Map(IReadOnlyList<ClusterInfo> clusters)
        => [.. clusters.Select(c => new ClusterSummaryDto(
            c.Name,
            c.DbName,
            c.BucketsCount,
            c.Incomplete,
            c.State == ClusterState.NotInitialized,
            c.State == ClusterState.ToRemove, // «к удалению» — config.state TO_REMOVE (arch/02 §9.4)
            c.Shards.Count,
            c.Shards.Count(s => s.MasterAddress is not null),
            // NOT_INITIALIZED — не переезд: только реальные состояния перемещения (spec t12 §3.6)
            c.Buckets.Count(b => b.State is BucketState.Syncing or BucketState.Frozen or BucketState.Aborting),
            !(c.BucketsCount == 1 && c.Shards.Count <= 1)))];
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
