using System.Text.RegularExpressions;

namespace S3Worker.App;

// Fail-fast валидации старта (arch/s3worker/08-operability.md §5) — чистые предикаты (юнит-тестируемы):
// Program проверяет их на ValidateOnStart; сообщения — с env-подсказкой оператору.
internal static partial class S3StartupChecks
{
    [GeneratedRegex(@"^[a-z0-9][a-z0-9_.-]{0,62}$")]
    private static partial Regex BucketRegex();

    // etcd-endpoints: без них воркер бессмысленен (единый контур — arch/synchronization/01-truth-and-convergence.md §S1).
    public static bool EtcdEndpoints(S3WorkerOptions o)
        => o.Etcd.Endpoints is { Length: > 0 };

    // AdvertiseUrl: непустой; https:// (HTTPS-грань, mTLS — только /api,
    // arch/s3worker/07-api.md §1) либо AllowInsecureHttp (только WAF-тесты).
    public static bool AdvertiseUrl(S3WorkerOptions o)
        => !string.IsNullOrWhiteSpace(o.Api.AdvertiseUrl)
           && (o.Api.Tls.AllowInsecureHttp
               || o.Api.AdvertiseUrl.StartsWith("https://", StringComparison.Ordinal));

    // Docker-топология: Swarm → SwarmManager обязателен; Plain → таблица Hosts
    // непуста и без дубликатов имён (имя — атрибут docker_host placement, arch/s3-clusters/04-placement.md).
    public static bool DockerTopology(S3WorkerOptions o)
    {
        if (string.Equals(o.Docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
            return !string.IsNullOrWhiteSpace(o.Docker.SwarmManager);
        var names = o.Docker.Hosts.Select(h => h.Name).ToList();
        return names.Count > 0 && names.All(n => !string.IsNullOrWhiteSpace(n))
            && names.Count == names.Distinct(StringComparer.Ordinal).Count();
    }

    // Пороги: положительные; NodeBootSec ≤ 100 (spec §12.2/§14.12).
    public static bool Thresholds(S3WorkerOptions o)
        => o.Thresholds.NodeBootSec > 0
           && o.Thresholds.NodeBootSec <= 100
           && o.Thresholds.NodeDeadSec > 0
           && o.Thresholds.HealthIntervalSec > 0
           && o.Thresholds.SyncIntervalSec > 0
           && o.Thresholds.SyncMaxPages > 0;

    // Bucket per-install (arch/backups/06-s3-layout.md): непустое имя S3-бакета.
    public static bool Bucket(S3WorkerOptions o)
        => !string.IsNullOrEmpty(o.Bucket) && BucketRegex().IsMatch(o.Bucket);

    // Диапазон S3-портов нод: 0 < From < To ≤ 65535.
    public static bool PortRange(S3WorkerOptions o)
        => o.Docker.PortRange.From > 0
           && o.Docker.PortRange.From < o.Docker.PortRange.To
           && o.Docker.PortRange.To <= 65535;

    // Контурная сеть (arch/s3worker/01-placement.md): не поддерживается в Mode=Swarm — сеть
    // overlay назначает сам Swarm (fail-fast старта, канон PgWorker).
    public static void EnsureContourNetworkMode(DockerOptions docker)
    {
        if (!string.IsNullOrWhiteSpace(docker.ContourNetwork)
            && string.Equals(docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
            throw new ApplicationException(
                "S3Worker:Docker:ContourNetwork is not supported in Mode=Swarm — contour network mode is implemented for plain containers");
    }
}
