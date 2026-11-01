namespace EtcdWorker.App;

/// <summary>
///     Fail-fast валидации старта (arch/25 §8) — чистые предикаты EtcdWorkerOptions → bool
///     (юнит-тестируемы): Program проверяет их на ValidateOnStart.
/// </summary>
internal static class EtcdStartupChecks
{
    // etcd-endpoints: без них воркер бессмысленен.
    public static bool EtcdEndpoints(EtcdWorkerOptions o) => o.Etcd.Endpoints is { Length: > 0 };

    // AdvertiseUrl: непустой; https:// (mTLS-only) либо AllowInsecureHttp (WAF-тесты).
    public static bool AdvertiseUrl(EtcdWorkerOptions o)
        => !string.IsNullOrWhiteSpace(o.Api.AdvertiseUrl)
           && (o.Api.Tls.AllowInsecureHttp
               || o.Api.AdvertiseUrl.StartsWith("https://", StringComparison.Ordinal));

    // Docker-топология (arch/25 §2): Plain → РОВНО один хост (мультихост — Swarm)
    // + непустая сеть контура; Swarm → SwarmManager.
    public static bool DockerTopology(EtcdWorkerOptions o)
    {
        if (string.Equals(o.Docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
            return !string.IsNullOrWhiteSpace(o.Docker.SwarmManager);
        var hosts = o.Docker.Hosts;
        return hosts.Length == 1
               && !string.IsNullOrWhiteSpace(hosts[0].Name)
               && !string.IsNullOrWhiteSpace(hosts[0].Endpoint)
               && !string.IsNullOrWhiteSpace(o.Docker.Network);
    }

    // Пороги: положительные; NodeBootSec ≤ 100.
    public static bool Thresholds(EtcdWorkerOptions o)
        => o.Thresholds.NodeBootSec > 0 && o.Thresholds.NodeBootSec <= 100
           && o.Thresholds.NodeDeadSec > 0 && o.Thresholds.HealthIntervalSec > 0;

    // Диапазон клиентских портов: 0 < From < To ≤ 65535.
    public static bool PortRange(EtcdWorkerOptions o)
        => o.Docker.PortRange.From > 0
           && o.Docker.PortRange.From < o.Docker.PortRange.To
           && o.Docker.PortRange.To <= 65535;

    // Интервалы слепков (arch/25 §8): положительные; ретенция ≥ 1.
    public static bool Snapshots(EtcdWorkerOptions o)
        => o.Snapshots.IntervalMin > 0 && o.Snapshots.MinIntervalSec > 0
           && o.Snapshots.RetentionFiles >= 1 && o.Snapshots.RetentionObjects >= 1
           && o.Snapshots.RetryIntervalSec > 0 && o.Snapshots.TimeoutSec > 0
           && o.Snapshots.MaintenanceIntervalMin > 0;
}
