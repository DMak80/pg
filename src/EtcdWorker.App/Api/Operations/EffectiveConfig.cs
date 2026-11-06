namespace EtcdWorker.App.Api.Operations;

// Эффективный конфиг установки (arch/etcdworker/05-api.md §1 GET /api/config): read-only,
// БЕЗ секретов/TLS/endpoints (Etcd-endpoints, секция Api/TLS, Docker Tls/Ssh,
// S3 CA-бандл наружу не выходит); поле source=env — значения
// применяются рестартом воркера. Панель отдаёт это как есть через on-demand
// прокси GET /api/workers/{worker}/effective-config (adminpanel/02 §2.3.5).
public static class EtcdWorkerEffectiveConfig
{
    public static object From(EtcdWorkerOptions o) => new
    {
        source = "env",
        thresholds = new
        {
            nodeBootSec = o.Thresholds.NodeBootSec,
            nodeDeadSec = o.Thresholds.NodeDeadSec,
            healthIntervalSec = o.Thresholds.HealthIntervalSec,
        },
        loops = new
        {
            scanIntervalSec = o.Loops.ScanIntervalSec,
            keepaliveSec = o.Loops.KeepaliveSec,
            watchdog = new
            {
                enabled = o.Loops.Watchdog.Enabled,
                multiplier = o.Loops.Watchdog.Multiplier,
                checkIntervalSec = o.Loops.Watchdog.CheckIntervalSec,
            },
        },
        nodeSnapshot = new
        {
            intervalMin = o.NodeSnapshot.IntervalMin,
            minIntervalSec = o.NodeSnapshot.MinIntervalSec,
            maintenanceIntervalMin = o.NodeSnapshot.MaintenanceIntervalMin,
            retentionFiles = o.NodeSnapshot.RetentionFiles,
            retentionObjects = o.NodeSnapshot.RetentionObjects,
            retryIntervalSec = o.NodeSnapshot.RetryIntervalSec,
            timeoutSec = o.NodeSnapshot.TimeoutSec,
        },
        docker = new
        {
            mode = o.Docker.Mode,
            network = o.Docker.Network,
            advertisedHost = o.Docker.AdvertisedHost,
            portRange = new
            {
                from = o.Docker.PortRange.From,
                to = o.Docker.PortRange.To,
            },
            images = new
            {
                etcdNode = o.Docker.Images.EtcdNode,
            },
        },
    };
}
