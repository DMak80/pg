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

        // Каркас (шаг 4 t32): живёт только keepalive — watchdog сносит и по
        // null-активности (2×порог), сердцебиения reconcile/snapshot добавляют
        // их фазы (шаги 13/9) вместе с самими циклами.
        return
        [
            // активность = тик или прогресс-отметка; keepalive — активность = тик
            // (долгих фаз нет)
            new LoopHeartbeat("keepalive", snap.LastKeepaliveTick, staleAfter),
        ];
    }
}
