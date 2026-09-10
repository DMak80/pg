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
    string? S3Region = null,
    string S3Bucket = "",
    string S3AccessKey = "",
    string S3SecretKey = "",
    string JobImage = "pgworker-backup:dev",
    int RetryBaseSec = 300,
    int RetryMaxSec = 3600,
    string StagingDir = "/backup-staging",
    long? StagingQuotaBytes = null,
    double? AgentCpu = null,
    long? AgentMem = null);
