namespace PgWorker.Backups;

/// <summary>Runtime-опции подсистемы бэкапов (arch/19 §9, t02): склейка секции
/// PgWorker:Backups (маппер BackupsOptions.ToRuntime() в App; образец
/// MovesRuntimeOptions). S3-креды — per-install, env воркера, дальше — env
/// джоба (в etcd/логи не попадают).</summary>
public sealed record BackupsRuntimeOptions(
    bool Enabled = false,
    long FullMaxAgeSec = 86400,
    bool VerifyOnCreate = true,
    string S3Endpoint = "",
    // t03: S3 из контейнеров агентов (single-host: host.docker.internal;
    // null → S3Endpoint — паттерн Etcd:AdvertisedEndpoints).
    string? S3AdvertisedEndpoint = null,
    string? S3Region = null,
    string S3Bucket = "",
    string S3AccessKey = "",
    string S3SecretKey = "",
    // t03: path-style — MinIO и облако одним клиентом (BackupS3).
    bool S3PathStyle = true,
    string JobImage = "pgworker-backup:dev",
    int RetryBaseSec = 300,
    int RetryMaxSec = 3600,
    string StagingDir = "/backup-staging",
    long? StagingQuotaBytes = null,
    double? AgentCpu = null,
    long? AgentMem = null,
    // t03 (arch/19 §3/§9): расписание контроля цепочки и пороги деградаций.
    int WalVerifyIntervalSec = 30,
    int WalLagMaxSegments = 1024,
    int WalStaleSec = 300)
{
    /// <summary>Endpoint S3 для env контейнера агента/джоба (t03, §7: адресация
    /// env, не строка команды; advertised-fallback).</summary>
    public string AgentS3Endpoint
        => S3AdvertisedEndpoint is { Length: > 0 } ? S3AdvertisedEndpoint : S3Endpoint;
}
