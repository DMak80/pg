using Microsoft.Extensions.Options;
using Shared.Core.Hosting;

namespace ValkeyWorker.App;

/// <summary>Живость циклов ValkeyWorker для watchdog: снимок HealthState + единый
/// порог сноса от собственных опций watchdog (Multiplier × CheckIntervalSec);
/// перечень — reconcile/keepalive/snapshot (имена = loops-alive healthz).</summary>
public sealed class ValkeyWorkerLoopsVitality(
    IOptionsMonitor<ValkeyWorkerOptions> options,
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

        // snapshot: тик ИЛИ пульс сна — позднейший факт активности (сны длиннее
        // порога 30 c пульсируют MarkSnapshotActivity, B2)
        var snapshotActivity = snap.LastSnapshotTick is { } tick && snap.LastSnapshotActivity is { } pulse
            ? (tick > pulse ? tick : pulse)
            : snap.LastSnapshotTick ?? snap.LastSnapshotActivity;
        return
        [
            // активность = тик или прогресс-отметка; keepalive — активность = тик
            // (долгих фаз нет; сон snapshot — пульс B2)
            new LoopHeartbeat("reconcile", snap.LastReconcileActivity, staleAfter),
            new LoopHeartbeat("keepalive", snap.LastKeepaliveTick, staleAfter),
            new LoopHeartbeat("snapshot", snapshotActivity, staleAfter),
        ];
    }
}
