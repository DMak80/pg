namespace PgWorker.App.Api.Operations;

// Эффективный конфиг установки (arch/pgworker/07-api.md §1 GET /api/config, u15): read-only,
// БЕЗ секретов/TLS/endpoints (Etcd/Docker/Api/Metrics-секции и любые креды
// наружу не выходят никогда); поле source=env — значения применяются
// рестартом воркера. Панель отдаёт это как есть через on-demand прокси
// GET /api/workers/{worker}/effective-config (adminpanel/02 §2.3.5).
public static class PgWorkerEffectiveConfig
{
    public static object From(PgWorkerOptions o) => new
    {
        source = "env",
        thresholds = new
        {
            nodeDeadSec = o.Thresholds.NodeDeadSec,
            shardDeadSec = o.Thresholds.ShardDeadSec,
            patroniBootSec = o.Thresholds.PatroniBootSec,
            cutoverTimeoutSec = o.Thresholds.CutoverTimeoutSec,
            connFailBudgetSec = o.Thresholds.ConnFailBudgetSec,
            provisionRetryBaseSec = o.Thresholds.ProvisionRetryBaseSec,
            provisionRetryMaxSec = o.Thresholds.ProvisionRetryMaxSec,
        },
        loops = new
        {
            scanIntervalSec = o.Loops.ScanIntervalSec,
            keepaliveSec = o.Loops.KeepaliveSec,
            snapshotIntervalMin = o.Loops.SnapshotIntervalMin,
            errorDelayMs = o.Loops.ErrorDelayMs,
        },
        moves = new
        {
            pollIntervalSec = o.Moves.PollIntervalSec,
            freezeWaitSec = o.Moves.FreezeWaitSec,
            freezeLockTimeoutSec = o.Moves.FreezeLockTimeoutSec,
            freezeLockTries = o.Moves.FreezeLockTries,
            abortMinAgeSec = o.Moves.AbortMinAgeSec,
        },
        parallelism = new { maxClusters = o.Parallelism.MaxClusters },
        backups = new { enabled = o.Backups.Enabled },
        snapshots = new
        {
            // Локальные слепки P12 (arch/pgworker/08-operability.md §5); выгрузка в S3 и обслуживание
            // контура (compact/defrag) — зона EtcdNode (arch/etcdnode/02-snapshots.md), секции
            // Export у PgWorker нет.
            dir = o.Snapshots.Dir,
            retentionFiles = o.Snapshots.RetentionFiles,
        },
        pgtune = new
        {
            dbVersion = o.Pgtune.DbVersion,
            dbType = o.Pgtune.DbType,
            hdType = o.Pgtune.HdType,
            dbSize = o.Pgtune.DbSize,
            connections = o.Pgtune.Connections,
        },
    };
}
