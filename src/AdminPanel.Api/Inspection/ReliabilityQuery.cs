using AdminPanel.Core;
using AdminPanel.Etcd;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Inspection;

// Запрос грани «Надёжность» (arch/03 §1): RPO/RTO-числа per-cluster/per-shard.
public sealed record ReliabilityQuery : IQuery<ReliabilityDto>;

public sealed record ReliabilityDto(IReadOnlyList<ReliabilityClusterDto> Clusters);
public sealed record ReliabilityClusterDto(string Cluster, IReadOnlyList<ReliabilityShardDto> Shards);
public sealed record ReliabilityShardDto(string Shard, bool Declared, RpoDto? Rpo, RtoDto? Rto);
public sealed record RpoDto(
    string Mode, long? FullAgeSec, string? FullId, long? WalLagSegments,
    long? WalAgeSec, long? RpoPotentialSec, long ThresholdFullAgeSec);
public sealed record HaFactRtoDto(
    string Shard, string Node, string Cause, long DetectedUnix, long? ResolvedUnix,
    long? DurationSec, bool Ongoing, long OngoingSec);
public sealed record OpRtoDto(string State, long? DurationSec, long? OngoingSec, long? FinishedUnix, string? Error);
public sealed record RtoDto(
    HaFactRtoDto? LastFailover, HaFactRtoDto? LastRebuild, OpRtoDto? LastDrill, OpRtoDto? LastRestore);

// Core → DTO: чистые функции.
public static class ReliabilityMappers
{
    public static ReliabilityDto Map(IReadOnlyList<ClusterReliability> clusters)
        => new(clusters.Select(MapCluster).ToList());

    private static ReliabilityClusterDto MapCluster(ClusterReliability c)
        => new(c.Cluster, c.Shards.Select(MapShard).ToList());

    private static ReliabilityShardDto MapShard(ShardReliability s)
        => new(s.Shard, s.Declared, MapRpo(s.Rpo), MapRto(s.Rto));

    private static RpoDto? MapRpo(RpoBlock? rpo)
        => rpo is null ? null
            : new RpoDto(rpo.Mode.ToString().ToLowerInvariant(), rpo.FullAgeSec, rpo.FullId,
                rpo.WalLagSegments, rpo.WalAgeSec, rpo.RpoPotentialSec, rpo.ThresholdFullAgeSec);

    private static RtoDto? MapRto(RtoBlock? rto)
        => rto is null ? null
            : new RtoDto(
                MapFact(rto.LastFailover), MapFact(rto.LastRebuild),
                MapOp(rto.LastDrill), MapOp(rto.LastRestore));

    private static OpRtoDto? MapOp(OpRto? op)
        => op is null ? null
            : new OpRtoDto(op.State, op.DurationSec, op.OngoingSec, op.FinishedUnix, op.Error);

    private static HaFactRtoDto? MapFact(HaFactRto? fact)
        => fact is null ? null
            : new HaFactRtoDto(fact.Shard, fact.Node, fact.Cause, fact.DetectedUnix,
                fact.ResolvedUnix, fact.DurationSec, fact.Ongoing, fact.OngoingSec);
}

// 503 «снапшота нет»; при наличии снапшота всегда 200 (пустые секции —
// подсистема выключена/надзор старой версии — толерантность, spec §3.5).
[InjectAsScoped]
public sealed class ReliabilityQueryHandler(ISnapshotStore store, TimeProvider clock)
    : IQueryHandler<ReliabilityQuery, ReliabilityDto>
{
    public ValueTask<Result<ReliabilityDto>> Handle(ReliabilityQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<ReliabilityDto>.Failed(new InspectionModule.SnapshotNotReadyException())
            : Result<ReliabilityDto>.Success(
                ReliabilityMappers.Map(ReliabilityCalculator.Calculate(snapshot, clock.GetUtcNow()))));
    }
}
