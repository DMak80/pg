namespace ValkeyWorker.App.Api.Operations;

// Эффективный конфиг установки (arch/valkeyworker/05-api.md §1 GET /api/config, u15): read-only,
// БЕЗ секретов/TLS/endpoints (Etcd/Docker/Api/Metrics-секции и любые креды
// наружу не выходят никогда); поле source=env — значения применяются
// рестартом воркера. Панель отдаёт это как есть через on-demand прокси
// GET /api/workers/{worker}/effective-config (adminpanel/02 §2.3.5).
public static class ValkeyWorkerEffectiveConfig
{
    public static object From(ValkeyWorkerOptions o) => new
    {
        source = "env",
        thresholds = new
        {
            nodeBootSec = o.Thresholds.NodeBootSec,
            nodeDeadSec = o.Thresholds.NodeDeadSec,
            rotationTicketTimeoutSec = o.Thresholds.RotationTicketTimeoutSec,
        },
        loops = new
        {
            scanIntervalSec = o.Loops.ScanIntervalSec,
            keepaliveSec = o.Loops.KeepaliveSec,
            snapshotIntervalMin = o.Loops.SnapshotIntervalMin,
            errorDelayMs = o.Loops.ErrorDelayMs,
        },
        parallelism = new { maxClusters = o.Parallelism.MaxClusters },
    };
}
