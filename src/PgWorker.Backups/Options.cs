namespace PgWorker.Backups;

/// <summary>Runtime-опции подсистемы бэкапов (arch/19 §9, t02): склейка секции
/// PgWorker:Backups (маппер BackupsOptions.ToRuntime() в App; образец
/// MovesRuntimeOptions). S3-креды — per-cluster secrets пула
/// (/s3/clusters/<C>/secrets, выбирается policy.s3), дальше — env джоба
/// (в etcd/логи не попадают).</summary>
public sealed record BackupsRuntimeOptions(
    bool Enabled = false,
    long FullMaxAgeSec = 86400,
    bool VerifyOnCreate = true,
    // arch/19 §7: только bucket per-install + CA; endpoint/креды — пул
    // per-cluster (policy.s3 + secrets-ключ).
    string S3Bucket = "",
    // t42 (arch/19 §7): путь ca.pem per-install CA — доверие https S3-endpoint;
    // null — http-endpoint, поведение прежнее; при заданном пути BackupS3 ставит
    // фабрику доверия S3CaHttpClientFactory (env AWS_CA_BUNDLE линейкой
    // AWSSDK.Core 3.7.x не читается).
    string? S3CaBundlePath = null,
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
    int WalStaleSec = 300,
    // t06 (arch/19 §9): ретенция и квота; t18 — дефолт-политика GFS для кластеров
    // без policy-ключа: 1/1/1 — целевая схема (последний + недельная + месячная).
    int PolicyRetentionDays = 1,
    int PolicyRetentionWeeks = 1,
    int PolicyRetentionMonths = 1,
    int RetentionIntervalSec = 600,
    int RetentionKeepFailed = 20,
    long QuotaBytes = 0,
    int QuotaWarnPercent = 80,
    int QuotaCritPercent = 90,
    // t04 (arch/19 §4/§8): период перепроверки оставшихся полных; policy-ключ
    // verify.interval_sec перекрывает; <= 0 — периодика выключена.
    long VerifyIntervalSec = 604800,
    // t05 (arch/19 §9): бюджет локального наката WAL restore-джобом
    // (фаза recovering; исчерпание → FAILED «recovery-бюджет исчерпан»).
    int RestoreRecoveryTimeoutSec = 1800,
    // t07 (arch/19 §9): супервизор бэкапов (период сверок per-cluster и
    // глобального лидер-прохода; TTL сирот, 0 — только алерт) и бюджеты
    // зависших джобов (arch/19 §6).
    int SupervisorIntervalSec = 600,
    long SupervisorOrphanTtlSec = 604800,
    int JobFullTimeoutSec = 21600,
    int JobVerifyTimeoutSec = 21600,
    int JobRestoreTimeoutSec = 86400,
    // reliability t02 (arch/19 §9, spec §3.2): дрилл восстановимости — дефолт
    // периода (суток; policy.drill.interval_days кластера перекрывает; 0 —
    // глобальное выключение новых запусков) и бюджет активного дрилла от
    // started_unix → FAILED drill-timeout.
    int DrillIntervalDays = 1,
    int DrillTimeoutSec = 21600,
    // t41 (arch/14 §2.1): сеть контура поставки — задана → ноды/ephemeral-джобы
    // бэкапов создаются в ней одной (pgw-net-<C> не используется, extra_hosts
    // спек не применяются); null — прод-режим.
    string? ContourNetwork = null);
