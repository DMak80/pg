namespace PgWorker.Backups;

/// <summary>Runtime-опции подсистемы бэкапов (arch/backups/09-operability.md §1, /): склейка
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
    // arch/backups/08-secrets.md: только bucket per-install + CA; endpoint/креды — пул
    // per-cluster (policy.s3 + secrets-ключ).
    string S3Bucket = "",
    //  (arch/backups/08-secrets.md): путь ca.pem per-install CA — доверие https S3-endpoint;
    // null — http-endpoint, поведение прежнее; при заданном пути BackupS3 ставит
    // фабрику доверия S3CaHttpClientFactory (env AWS_CA_BUNDLE линейкой
    // AWSSDK.Core 3.7.x не читается).
    string? S3CaBundlePath = null,
    int RetryBaseSec = 300,
    int RetryMaxSec = 3600,
    string StagingDir = "/home/postgres/pgdata/backup-staging",
    //  (arch/backups/02-wal-archive.md §1/§9): расписание контроля цепочки и пороги деградаций.
    int WalVerifyIntervalSec = 30,
    int WalLagMaxSegments = 1024,
    int WalStaleSec = 300,
    //  (arch/backups/02-wal-archive.md §1/§9): период `.partial`-доотправки префикса живого сегмента
    // приёмником; доставляется env ноды PGW_WAL_PARTIAL_INTERVAL_SEC.
    int WalPartialIntervalSec = 3,
    // t15 (arch/backups/09 §1): период компакции дельт незакрытого сегмента
    // (база-компак вместо накопления дельт); доставляется env ноды
    // PGW_WAL_PARTIAL_COMPACT_INTERVAL_SEC; < 60 — fail-fast старта воркера.
    int WalPartialCompactIntervalSec = 3600,
    //  (arch/backups/09-operability.md §1): ретенция и квота; — дефолт-политика GFS для кластеров
    // без policy-ключа: 1/1/1 — целевая схема (последний + недельная + месячная).
    int PolicyRetentionDays = 1,
    int PolicyRetentionWeeks = 1,
    int PolicyRetentionMonths = 1,
    int RetentionIntervalSec = 600,
    int RetentionKeepFailed = 20,
    long QuotaBytes = 0,
    int QuotaWarnPercent = 80,
    int QuotaCritPercent = 90,
    //  (arch/backups/05-etcd-contract.md/§8): период перепроверки оставшихся полных; policy-ключ
    // verify.interval_sec перекрывает; <= 0 — периодика выключена.
    long VerifyIntervalSec = 604800,
    //  (arch/backups/09-operability.md §1): бюджет локального наката WAL restore-джобом
    // (фаза recovering; исчерпание → FAILED «recovery-бюджет исчерпан»).
    int RestoreRecoveryTimeoutSec = 1800,
    //  (arch/backups/09-operability.md §1): супервизор бэкапов (период сверок per-cluster и
    // глобального лидер-прохода; TTL сирот, 0 — только алерт) и бюджеты
    // зависших джобов (arch/backups/09-operability.md).
    int SupervisorIntervalSec = 600,
    long SupervisorOrphanTtlSec = 604800,
    //  (arch/backups/05-etcd-contract.md «Гейты мусора»): дефолт возрастного гейта мусора
    // супервизора — policy-ключ garbage_min_age_sec кластера перекрывает;
    // защита заливающегося полного от устаревшего снапшота тика.
    long GarbageMinAgeSec = 600,
    int JobFullTimeoutSec = 21600,
    int JobVerifyTimeoutSec = 21600,
    int JobRestoreTimeoutSec = 86400,
    // reliability (arch/backups/09-operability.md §1, spec §3.2): дрилл восстановимости — дефолт
    // периода (суток; policy.drill.interval_days кластера перекрывает; 0 —
    // глобальное выключение новых запусков) и бюджет активного дрилла от
    // started_unix → FAILED drill-timeout.
    int DrillIntervalDays = 1,
    int DrillTimeoutSec = 21600);
