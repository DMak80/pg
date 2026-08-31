using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Moves;

/// <summary>
/// Репарация брошенных переездов (spec §3.5, arch/14 §5 K MR0–MR3): статус-ключ
/// без живого владельца (нет заявки, updated_unix постарел) закрывается
/// синтетической заявкой put-if-absent в существующий MoveProcess — механика
/// доведения/журналов/идемпотентности переиспользуется 1:1. Живой владелец
/// (свежий статус или заявка) неприкосновенен (spec §2.4).
/// </summary>
public sealed class MoveRepairProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    ClaimStore claims,
    WorkJournal journal,
    MovesRuntimeOptions options,
    TimeProvider clock,
    ILogger<MoveRepairProcess>? logger = null)
{
    private readonly MoveRequestsStore requests = new(etcd, endpoints);

    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только под живым клэймом (MR0).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"repair {cluster}: клэйм не наш (или потерян) — мутации запрещены"));
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var stale = snap.Routing.Where(r => r.Status is not null).ToList();
        if (stale.Count == 0)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var listing = await requests.ListAsync(cluster, ct);
        if (!listing.IsSuccess)
            return Result<ProcessOutcome>.Failed(listing.Error!);
        var claimed = listing.Value.Requests.Select(r => r.Bucket).ToHashSet(StringComparer.Ordinal);

        var now = clock.GetUtcNow().ToUnixTimeSeconds();
        var dispatched = new List<string>();
        foreach (var route in stale)
        {
            var bucket = $"bucket_{route.Id}";
            if (claimed.Contains(bucket))
                continue; // живая заявка — домен MoveProcess (MR1-гвард)

            var repair = Classify(route, route.Owner, now, options);
            if (repair is null)
                continue;

            // put-if-absent: оператор успел раньше — его заявка живёт (spec §3.5).
            var put = await requests.PutIfAbsentAsync(cluster, bucket, repair, ct);
            if (!put.IsSuccess)
                return Result<ProcessOutcome>.Failed(put.Error!);
            if (put.Value)
            {
                dispatched.Add(bucket);
                logger?.LogInformation(
                    "repair {cluster}/{bucket}: синтетическая заявка {op} (force={force}) — доведёт MoveProcess",
                    cluster, bucket, repair.Op, repair.Force);
            }
        }

        if (dispatched.Count > 0)
            await journal.WritePhaseAsync(cluster, "repair", "dispatched", claims.InstanceId,
                $"статусы: {string.Join(", ", dispatched)}", ct);

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    /// <summary>MR1-классификация (spec §3.5): брошенный статус → синтетическая
    /// заявка; null = не трогаем (свежий/чужой домен). routingOwner — текущий
    /// владелец из ROUTING (единственный авторитет «где бакет»).</summary>
    internal static MoveRequest? Classify(
        BucketRoute route, string? routingOwner, long nowUnix, MovesRuntimeOptions o)
    {
        if (route.Status is not ({ } state and not BucketMoveState.NotInitialized))
            return null;

        var age = nowUnix - (route.MoveUpdatedUnix ?? 0);

        // Фаза доведения отката: заявка rollback — MoveProcess продолжит по фазе.
        if (route.MovePhase == MovePhases.RollbackPostFlip)
            return age <= o.RepairFrozenSec ? null : RepairRequest(route, MoveOp.Rollback, force: false, nowUnix);

        return state switch
        {
            BucketMoveState.Aborting => age > o.RepairStaleSec
                ? RepairRequest(route, MoveOp.Abort, force: false, nowUnix) : null,

            // routing==target: flip прошёл, статус завис — доведение перевода;
            // без force AbortSequence даёт permanent-отказ (цикл), spec §3.5.
            BucketMoveState.Syncing when route.MoveTarget == routingOwner => age > o.RepairStaleSec
                ? RepairRequest(route, MoveOp.Abort, force: true, nowUnix) : null,
            BucketMoveState.Frozen when route.MoveTarget == routingOwner => age > o.RepairFrozenSec
                ? RepairRequest(route, MoveOp.Abort, force: true, nowUnix) : null,

            // routing==owner: откат на владельца — уборка артефактов + re-GRANT
            // (разморозка). Свежесть пройдёт сама: порог ≥ AbortMinAgeSec (Д12).
            BucketMoveState.Syncing => age > o.RepairStaleSec
                ? RepairRequest(route, MoveOp.Abort, force: false, nowUnix) : null,
            BucketMoveState.Frozen => age > o.RepairFrozenSec
                ? RepairRequest(route, MoveOp.Abort, force: false, nowUnix) : null,

            _ => null,
        };

        static MoveRequest RepairRequest(BucketRoute r, MoveOp op, bool force, long now)
            => new($"bucket_{r.Id}", op, To: null, OldShard: null, SkipReverse: false,
                Resume: false, Force: force, RequestedUnix: now, RequestedBy: "pgworker-repair");
    }
}
