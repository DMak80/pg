using Microsoft.Extensions.Options;
using Shared.Core.HealthChecks;
using Shared.Core.Hosting;

namespace PgWorker.App;

/// <summary>Живость циклов PgWorker для watchdog: снимок HealthState + пороги
/// LoopStaleness × Loops:Watchdog:Multiplier; перечень — все четыре цикла
/// (reconcile/keepalive/snapshot/orphan-sweep), имена = loops-alive healthz.</summary>
public sealed class PgWorkerLoopsVitality(
    IOptionsMonitor<PgWorkerOptions> options,
    HealthState health) : ILoopsVitality
{
    public IReadOnlyList<LoopHeartbeat> Snapshot()
    {
        var loops = options.CurrentValue.Loops;
        var multiplier = Math.Max(1, loops.Watchdog.Multiplier);
        var fast = TimeSpan.FromTicks(LoopStaleness.FastLoops(loops.ScanIntervalSec, loops.KeepaliveSec).Ticks * multiplier);
        var snapshotLoop = TimeSpan.FromTicks(LoopStaleness.SnapshotLoop(loops.ScanIntervalSec, loops.SnapshotIntervalMin).Ticks * multiplier);
        var snap = health.Snapshot();
        return
        [
            // активность = тик или прогресс-отметка; keepalive/snapshot/orphan-sweep —
            // активность = тик (долгих фаз нет)
            new LoopHeartbeat("reconcile", snap.LastReconcileActivity, fast),
            new LoopHeartbeat("keepalive", snap.LastKeepaliveTick, fast),
            new LoopHeartbeat("snapshot", snap.LastSnapshotTick, snapshotLoop),
            new LoopHeartbeat("orphan-sweep", snap.LastOrphanSweepTick, fast),
        ];
    }
}
