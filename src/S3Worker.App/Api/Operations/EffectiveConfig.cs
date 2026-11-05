namespace S3Worker.App.Api.Operations;

// Эффективный конфиг установки (arch/s3worker/07-api.md §1 GET /api/config, u15): read-only,
// БЕЗ секретов/TLS/endpoints (Etcd/Docker/Api/OwnS3/Metrics-секции и любые
// креды наружу не выходят никогда); поле source=env — значения применяются
// рестартом воркера. Панель отдаёт это как есть через on-demand прокси
// GET /api/workers/{worker}/effective-config (adminpanel/02 §2.3.5).
public static class S3WorkerEffectiveConfig
{
    public static object From(S3WorkerOptions o) => new
    {
        source = "env",
        thresholds = new
        {
            nodeBootSec = o.Thresholds.NodeBootSec,
            nodeDeadSec = o.Thresholds.NodeDeadSec,
            healthIntervalSec = o.Thresholds.HealthIntervalSec,
            syncIntervalSec = o.Thresholds.SyncIntervalSec,
            syncMaxPages = o.Thresholds.SyncMaxPages,
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
