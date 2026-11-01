namespace KafkaWorker.App.Api.Operations;

// Эффективный конфиг установки (arch/16 §1.1 GET /api/config, u15): read-only,
// БЕЗ секретов/TLS/endpoints (Etcd/Docker/Api/Metrics-секции и любые креды
// наружу не выходят никогда); поле source=env — значения применяются
// рестартом воркера. Панель отдаёт это как есть через on-demand прокси
// GET /api/workers/{worker}/effective-config (adminpanel/02 §2.3.5).
public static class KafkaWorkerEffectiveConfig
{
    public static object From(KafkaWorkerOptions o) => new
    {
        source = "env",
        thresholds = new
        {
            brokerBootSec = o.Thresholds.BrokerBootSec,
            nodeDeadSec = o.Thresholds.NodeDeadSec,
            reassignSubmitSec = o.Thresholds.ReassignSubmitSec,
            reassignRetrySubmitSec = o.Thresholds.ReassignRetrySubmitSec,
            rotationTicketTimeoutSec = o.Thresholds.RotationTicketTimeoutSec,
        },
        loops = new
        {
            scanIntervalSec = o.Loops.ScanIntervalSec,
            keepaliveSec = o.Loops.KeepaliveSec,
            topicSyncIntervalSec = o.Loops.TopicSyncIntervalSec,
            reassignIntervalSec = o.Loops.ReassignIntervalSec,
            reassignBatchPartitions = o.Loops.ReassignBatchPartitions,
            snapshotIntervalMin = o.Loops.SnapshotIntervalMin,
            errorDelayMs = o.Loops.ErrorDelayMs,
        },
        parallelism = new { maxClusters = o.Parallelism.MaxClusters },
    };
}
