using AdminPanel.Core;
using AdminPanel.Core.Alerting.Rules;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Api.Inspection;

// Запрос сводки дашборда (arch/03 §1 GET /api/overview).
public sealed record OverviewQuery : IQuery<OverviewDto>;

// Ответ GET /api/overview: etcd-часть реальна, кластерная часть — t05 (spec §3.15).
public sealed record OverviewDto(
    int AlertsCritical,
    int AlertsWarning,
    OverviewEtcdDto Etcd,
    IReadOnlyList<OverviewClusterDto> Clusters,
    IReadOnlyList<OverviewMoveDto> ActiveMoves,
    long SnapshotAgeMs,
    bool Stale);

public sealed record OverviewEtcdDto(bool Reachable, int EndpointsOk, int EndpointsTotal);

// Заглушки контракта t05 (arch/03 §2): поля полные, значения — всегда пусто в t04.
// Наполнены из снапшота в t05 (spec §6.2).
public sealed record OverviewClusterDto(
    string Name, int Shards, int Buckets, int ActiveMoves, int MasterlessShards, bool NotInitialized);

public sealed record OverviewMoveDto(
    string Cluster, int Bucket, string State, string? Owner, string? Target, long? UpdatedUnix);

// Снапшот → сводку: чистая функция (spec §6.2).
public static class OverviewMapper
{
    public static OverviewDto Map(EtcdSnapshot snapshot, DateTimeOffset nowUtc, double refreshIntervalSeconds)
    {
        var age = nowUtc - snapshot.BuiltAtUtc;
        return new OverviewDto(
            snapshot.Alerts.Count(a => a.Severity == AlertSeverity.Critical),
            snapshot.Alerts.Count(a => a.Severity == AlertSeverity.Warning),
            new OverviewEtcdDto(
                snapshot.Etcd.Reachable,
                snapshot.Etcd.Endpoints.Count(e => e.Reachable),
                snapshot.Etcd.Endpoints.Count),
            [.. snapshot.Clusters.Select(c => new OverviewClusterDto(
                c.Name,
                c.Shards.Count,
                c.BucketsCount,
                // NOT_INITIALIZED — не переезд: только реальные состояния перемещения (spec t12 §3.6)
                c.Buckets.Count(b => b.State is BucketState.Syncing or BucketState.Frozen or BucketState.Aborting),
                c.State == ClusterState.NotInitialized
                    ? 0 // без мастера у не поднятого кластера — норма (arch/03 §2)
                    : c.Shards.Count(s => s.MasterAddress is null),
                c.State == ClusterState.NotInitialized))],
            [.. snapshot.Clusters
                .SelectMany(c => c.Buckets
                    .Where(b => b.State is BucketState.Syncing or BucketState.Frozen or BucketState.Aborting)
                    .OrderBy(b => b.Id) // внутри кластера — по Id (spec §3.6): модель порядка Buckets не гарантирует
                    .Select(b => new OverviewMoveDto(
                        c.Name, b.Id, BucketStates.Name(b.State),
                        b.Move?.Owner, b.Move?.Target, b.Move?.UpdatedUnix)))],
            Math.Max(0L, (long)Math.Round(age.TotalMilliseconds)),
            age > TimeSpan.FromSeconds(SnapshotStaleRule.Multiplier * refreshIntervalSeconds));
    }
}

// Хендлер: store → отказ «снапшота нет» или маппер (spec §3.12).
[InjectAsScoped]
public sealed class OverviewQueryHandler(
    ISnapshotStore store,
    TimeProvider time,
    IOptions<EtcdOptions> etcdOptions) : IQueryHandler<OverviewQuery, OverviewDto>
{
    public ValueTask<Result<OverviewDto>> Handle(OverviewQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<OverviewDto>.Failed(new InspectionModule.SnapshotNotReadyException())
            : Result<OverviewDto>.Success(OverviewMapper.Map(
                snapshot, time.GetUtcNow(), EffectiveInterval(etcdOptions))));
    }

    // Эффективный интервал: fallback 3 c при опечатке конфига — как в refresher (t03 §3.3).
    private static double EffectiveInterval(IOptions<EtcdOptions> options)
        => options.Value.RefreshIntervalSeconds > 0
            ? options.Value.RefreshIntervalSeconds
            : 3;
}
