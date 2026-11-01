namespace EtcdWorker.App.Api.Operations;

// Эффективный конфиг установки (arch/25 §1.1 GET /api/config): read-only,
// БЕЗ секретов/TLS/endpoints (Etcd-endpoints, секция Api/TLS, Docker Tls/Ssh,
// Snapshots.Dir наружу не выходят никогда); поле source=env — значения
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
        snapshots = new
        {
            intervalMin = o.Snapshots.IntervalMin,
            minIntervalSec = o.Snapshots.MinIntervalSec,
            maintenanceIntervalMin = o.Snapshots.MaintenanceIntervalMin,
            retentionFiles = o.Snapshots.RetentionFiles,
            retentionObjects = o.Snapshots.RetentionObjects,
            retryIntervalSec = o.Snapshots.RetryIntervalSec,
            timeoutSec = o.Snapshots.TimeoutSec,
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
                etcd = o.Docker.Images.Etcd,
            },
        },
    };
}
