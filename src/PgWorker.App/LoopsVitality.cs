using Microsoft.Extensions.Options;
using Shared.Core.Hosting;

namespace PgWorker.App;

/// <summary>Живость циклов PgWorker для watchdog: снимок HealthState + единый
/// порог сноса от собственных опций watchdog (Multiplier × CheckIntervalSec);
/// перечень — все четыре цикла (reconcile/keepalive/snapshot/orphan-sweep),
/// имена = loops-alive healthz.</summary>
public sealed class PgWorkerLoopsVitality(
    IOptionsMonitor<PgWorkerOptions> options,
    HealthState health) : ILoopsVitality
{
    public IReadOnlyList<LoopHeartbeat> Snapshot()
    {
        // Порог сноса — от собственных опций watchdog (arch/14 §6):
        // Multiplier × CheckIntervalSec = 30 c при дефолтах, ЕДИНЫЙ для всех
        // циклов; healthz-порог (LoopStaleness) в формуле не участвует.
        var watchdog = options.CurrentValue.Loops.Watchdog;
        var staleAfter = TimeSpan.FromSeconds(
            Math.Max(1, watchdog.Multiplier) * Math.Max(1, watchdog.CheckIntervalSec));
        var snap = health.Snapshot();
        return
        [
            // активность = тик или прогресс-отметка; keepalive/snapshot/orphan-sweep —
            // активность = тик (долгих фаз нет; сон snapshot — пульс B2)
            new LoopHeartbeat("reconcile", snap.LastReconcileActivity, staleAfter),
            new LoopHeartbeat("keepalive", snap.LastKeepaliveTick, staleAfter),
            new LoopHeartbeat("snapshot", snap.LastSnapshotTick, staleAfter),
            new LoopHeartbeat("orphan-sweep", snap.LastOrphanSweepTick, staleAfter),
        ];
    }
}
