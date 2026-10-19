using Microsoft.Extensions.Options;
using Shared.Core.Hosting;

namespace EtcdWorker.App;

/// <summary>Живость циклов EtcdWorker для watchdog: снимок HealthState + единый
/// порог сноса от собственных опций watchdog (Multiplier × CheckIntervalSec);
/// перечень — reconcile/keepalive/snapshot (имена = loops-alive healthz).</summary>
public sealed class EtcdWorkerLoopsVitality(
    IOptionsMonitor<EtcdWorkerOptions> options,
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

        // Шаги 9/13 t32: snapshot и reconcile живут; перечень = все циклы.
        return
        [
            // активность = тик или прогресс-отметка; keepalive — активность = тик
            // (долгих фаз нет)
            new LoopHeartbeat("keepalive", snap.LastKeepaliveTick, staleAfter),
            new LoopHeartbeat("snapshot", snap.LastSnapshotTick ?? snap.LastSnapshotActivity, staleAfter),
            new LoopHeartbeat("reconcile", snap.LastReconcileActivity ?? snap.LastReconcileTick, staleAfter),
        ];
    }
}
