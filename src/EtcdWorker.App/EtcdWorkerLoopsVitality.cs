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
            // snapshot: тик ИЛИ пульс Retry-сна (застрявший тик после первого
            // прохода игнорировал пульс — watchdog сносил цикл в законном
            // RetryIntervalSec-ожидании при отстающей выгрузке, arch/etcdworker/00-etcd-contract.md/§6)
            new LoopHeartbeat("snapshot", Latest(snap.LastSnapshotTick, snap.LastSnapshotActivity), staleAfter),
            new LoopHeartbeat("reconcile", snap.LastReconcileActivity ?? snap.LastReconcileTick, staleAfter),
        ];
    }

    // Последняя из двух отметок (тик/пульс) — живость цикла в длинном сне.
    private static DateTimeOffset? Latest(DateTimeOffset? a, DateTimeOffset? b)
        => a is null ? b : b is null ? a : a > b ? a : b;
}
