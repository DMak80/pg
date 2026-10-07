using Microsoft.Extensions.Options;
using Shared.Core.HealthChecks;
using Shared.Core.Hosting;

namespace KafkaWorker.App;

/// <summary>Живость циклов KafkaWorker для watchdog: снимок HealthState + пороги
/// LoopStaleness × Loops:Watchdog:Multiplier; перечень — reconcile/keepalive/snapshot
/// (имена = loops-alive healthz).</summary>
public sealed class KafkaWorkerLoopsVitality(
    IOptionsMonitor<KafkaWorkerOptions> options,
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
            new LoopHeartbeat("reconcile", snap.LastReconcileTick, fast),
            new LoopHeartbeat("keepalive", snap.LastKeepaliveTick, fast),
            new LoopHeartbeat("snapshot", snap.LastSnapshotTick, snapshotLoop),
        ];
    }
}
