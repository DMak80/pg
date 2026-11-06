using Microsoft.Extensions.Options;
using Shared.Core.Hosting;

namespace EtcdWorker.App;

/// <summary>Живость циклов EtcdWorker для watchdog: снимок HealthState + единый
/// порог сноса от собственных опций watchdog (Multiplier × CheckIntervalSec);
/// перечень — reconcile/keepalive (имена = loops-alive healthz).</summary>
public sealed class EtcdWorkerLoopsVitality(
    IOptionsMonitor<EtcdWorkerOptions> options,
    HealthState health) : ILoopsVitality
{
    public IReadOnlyList<LoopHeartbeat> Snapshot()
    {
        // Порог сноса — от собственных опций watchdog (arch/pgworker/08-operability.md §3):
        // Multiplier × CheckIntervalSec = 30 c при дефолтах, ЕДИНЫЙ для всех
        // циклов; healthz-порог (LoopStaleness) в формуле не участвует.
        var watchdog = options.CurrentValue.Loops.Watchdog;
        var staleAfter = TimeSpan.FromSeconds(
            Math.Max(1, watchdog.Multiplier) * Math.Max(1, watchdog.CheckIntervalSec));
        var snap = health.Snapshot();

        return
        [
            // активность = тик или прогресс-отметка; keepalive — активность = тик
            // (долгих фаз нет)
            new LoopHeartbeat("keepalive", snap.LastKeepaliveTick, staleAfter),
            new LoopHeartbeat("reconcile", snap.LastReconcileActivity ?? snap.LastReconcileTick, staleAfter),
        ];
    }
}
