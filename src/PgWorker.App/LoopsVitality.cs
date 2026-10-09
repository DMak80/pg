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

        // snapshot: тик ИЛИ пульс — позднейший факт активности (сон длиннее
        // порога пульсирует MarkSnapshotActivity, B2; S3-вызовы выгрузки —
        // MarkSnapshotActivity вокруг вызова, arch/14 §6)
        var snapshotActivity = snap.LastSnapshotTick is { } tick && snap.LastSnapshotActivity is { } pulse
            ? (tick > pulse ? tick : pulse)
            : snap.LastSnapshotTick ?? snap.LastSnapshotActivity;
        // orphan-sweep: тик ИЛИ пульс — зеркально snapshot (S3-проход сверки
        // пульсирует MarkOrphanSweepActivity вокруг вызовов, arch/14 §6);
        // тики ≠ активность — healthz loops-alive читает только тики
        var orphanActivity = snap.LastOrphanSweepTick is { } oTick && snap.LastOrphanSweepActivity is { } oPulse
            ? (oTick > oPulse ? oTick : oPulse)
            : snap.LastOrphanSweepTick ?? snap.LastOrphanSweepActivity;
        return
        [
            // активность = тик или прогресс-отметка; keepalive — только тик
            // (долгих фаз нет)
            new LoopHeartbeat("reconcile", snap.LastReconcileActivity, staleAfter),
            new LoopHeartbeat("keepalive", snap.LastKeepaliveTick, staleAfter),
            new LoopHeartbeat("snapshot", snapshotActivity, staleAfter),
            new LoopHeartbeat("orphan-sweep", orphanActivity, staleAfter),
        ];
    }
}
