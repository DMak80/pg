namespace PgWorker.Backups;

/// <summary>Runtime-опции подсистемы бэкапов (arch/19 §9, /): склейка
/// секции PgWorker:Backups (маппер BackupsOptions.ToRuntime() в App; образец
/// MovesRuntimeOptions). S3-креды — per-cluster secrets пула
/// (/s3/clusters/&lt;C&gt;/secrets, выбирается policy.s3); ops-операции —
/// S3-контекст в POST-команде (секреты не в etcd/логах/argv). Контейнеров/
/// джобов бэкапов нет (+): операции исполняют приёмники WalReceiver нод —
/// Agent/NodeImage/ContourNetwork-опций нет.</summary>
public sealed record BackupsRuntimeOptions(
    bool Enabled = false,
    long FullMaxAgeSec = 86400,
    bool VerifyOnCreate = true,
    // arch/19 §7: только bucket per-install + CA; endpoint/креды — пул
    // per-cluster (policy.s3 + secrets-ключ).
    string S3Bucket = "",
    //  (arch/19 §7): путь ca.pem per-install CA — доверие https S3-endpoint;
    // null — http-endpoint, поведение прежнее; при заданном пути BackupS3 ставит
    // фабрику доверия S3CaHttpClientFactory (env AWS_CA_BUNDLE линейкой
    // AWSSDK.Core 3.7.x не читается).
    string? S3CaBundlePath = null,
    int RetryBaseSec = 300,
    int RetryMaxSec = 3600,
    string StagingDir = "/home/postgres/pgdata/backup-staging",
    //  (arch/19 §3/§9): расписание контроля цепочки и пороги деградаций.
    int WalVerifyIntervalSec = 30,
    int WalLagMaxSegments = 1024,
    int WalStaleSec = 300,
    //  (arch/19 §9): ретенция и квота; — дефолт-политика GFS для кластеров
    // без policy-ключа: 1/1/1 — целевая схема (последний + недельная + месячная).
    int PolicyRetentionDays = 1,
    int PolicyRetentionWeeks = 1,
    int PolicyRetentionMonths = 1,
    int RetentionIntervalSec = 600,
    int RetentionKeepFailed = 20,
    long QuotaBytes = 0,
    int QuotaWarnPercent = 80,
    int QuotaCritPercent = 90,
    //  (arch/19 §4/§8): период перепроверки оставшихся полных; policy-ключ
    // verify.interval_sec перекрывает; <= 0 — периодика выключена.
    long VerifyIntervalSec = 604800,
    //  (arch/19 §9): бюджет локального наката WAL restore-джобом
    // (фаза recovering; исчерпание → FAILED «recovery-бюджет исчерпан»).
    int RestoreRecoveryTimeoutSec = 1800,
    //  (arch/19 §9): супервизор бэкапов (период сверок per-cluster и
    // глобального лидер-прохода; TTL сирот, 0 — только алерт) и бюджеты
    // зависших джобов (arch/19 §6).
    int SupervisorIntervalSec = 600,
    long SupervisorOrphanTtlSec = 604800,
    //  (arch/19 §4 «Гейты мусора»): дефолт возрастного гейта мусора
    // супервизора — policy-ключ garbage_min_age_sec кластера перекрывает;
    // защита заливающегося полного от устаревшего снапшота тика.
    long GarbageMinAgeSec = 600,
    int JobFullTimeoutSec = 21600,
    int JobVerifyTimeoutSec = 21600,
    int JobRestoreTimeoutSec = 86400,
    // reliability (arch/19 §9, spec §3.2): дрилл восстановимости — дефолт
    // периода (суток; policy.drill.interval_days кластера перекрывает; 0 —
    // глобальное выключение новых запусков) и бюджет активного дрилла от
    // started_unix → FAILED drill-timeout.
    int DrillIntervalDays = 1,
    int DrillTimeoutSec = 21600);
