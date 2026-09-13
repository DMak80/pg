using PgWorker.Backups;
using PgWorker.Core.Tuning;
using PgWorker.Docker.Engine;
using PgWorker.Moves;
using PgWorker.Provisioning.Processes;

namespace PgWorker.App;

// Конфигурация PgWorker (spec §10): секция "PgWorker" в appsettings.json +
// env-оверрайды (PgWorker__Etcd__Endpoints__0=http://…). Секреты установки —
// ТОЛЬКО env (Д7), здесь их нет. Полный пример значений — appsettings.json.

/// <summary>Корневые настройки сервиса.</summary>
public sealed class PgWorkerOptions
{
    public EtcdOptions Etcd { get; set; } = new();

    public DockerOptions Docker { get; set; } = new();

    public LoopsOptions Loops { get; set; } = new();

    public ThresholdsOptions Thresholds { get; set; } = new();

    public ParallelismOptions Parallelism { get; set; } = new();

    public SnapshotOptions Snapshots { get; set; } = new();

    /// <summary>Параметры процессов переезда бакетов (t01, spec §9).</summary>
    public MovesOptions Moves { get; set; } = new();

    /// <summary>Per-node серверные параметры подключения (app_params, spec §3.1;
    /// P17: doorman tls_mode=require → клиентский sslmode=require).</summary>
    public AppParamsOptions AppParams { get; set; } = new();

    /// <summary>HTTP API воркера (arch/14 §1.1): advertise-URL + стендовый сид.</summary>
    public ApiOptions Api { get; set; } = new();

    /// <summary>Экспозиция метрик (arch/18 §3): /metrics на том же порту, что /healthz.</summary>
    public Shared.Metrics.MetricsOptions Metrics { get; set; } = new();

    /// <summary>Подсистема бэкапов шардов (arch/19, t01): каркас конфигурации;
    /// процессная логика — t02–t07. Default Enabled=false.</summary>
    public BackupsOptions Backups { get; set; } = new();

    /// <summary>Входы PGTune-расчёта параметров PG (spec.md §4.2): константы
    /// воркера + exclude-параметры. Fail-fast валидация старта.</summary>
    public PgtuneOptions Pgtune { get; set; } = new();
}

/// <summary>
/// Входы расчёта PGTune (spec.md §4.2, алгоритм — docs/pgtune-calculation-spec.md):
/// параметры postgresql.conf нод рассчитываются ядром PgTune.Calculate от
/// характеристик ноды и этих констант, вместо сегодняшнего хардкода
/// SPILO_CONFIGURATION. Память/CPU — НЕ конфигурация: их источник — etcd-заявки
/// /service/&lt;scope&gt;/request_{cpu,mem} на ноду (arch/14 §2.1 п.4, панель
/// пишет при создании шарда); всё остальное — константы этого узла.
/// </summary>
public sealed class PgtuneOptions
{
    /// <summary>Версия PostgreSQL образа pgworker-node (Spilo-18) — вход dbVersion.</summary>
    public int DbVersion { get; set; } = 18;

    /// <summary>Тип нагрузки: web|oltp|dw|mixed (desktop запрещён — его
    /// wal_level=minimal/max_wal_senders=0 несовместимы с P3: логическое
    /// декодирование и переезды бакетов).</summary>
    public string DbType { get; set; } = "oltp";

    /// <summary>Тип дисковой подсистемы: ssd|san|hdd|nvme (влияет на
    /// effective_io_concurrency/random_page_cost).</summary>
    public string HdType { get; set; } = "ssd";

    /// <summary>Ожидаемый размер базы относительно RAM: less_ram|mid_ram|greater_ram
    /// (коррекция work_mem/random_page_cost).</summary>
    public string DbSize { get; set; } = "mid_ram";

    /// <summary>Вход connectionNum PGTune (P15: doorman-бюджет = Connections − 5,
    /// floor 10). 60 = 55 + 2 админ/mover + 3 reserved.</summary>
    public int Connections { get; set; } = 60;

    /// <summary>Имена PGTune-параметров, НЕ применяемые при сборке YAML
    /// (SpiloEnvBuilder; ядро всегда даёт полный вывод). Дефолт
    /// [io_method, io_workers]: io_method=io_uring требует сборки PG с
    /// --with-liburing — для образа pgworker-node/Spilo-18 не проверено;
    /// отсутствие параметра → PG18 default io_method=worker — безопасно.
    /// После проверки сборки оператор убирает из exclude.</summary>
    public string[] ExcludeParams { get; set; } = ["io_method", "io_workers"];

    /// <summary>Fail-fast старта (образец BackupsOptions.IsValid): границы §2
    /// спецификации алгоритма + домены строк. DbType=desktop запрещён (P3).
    /// ExcludeParams — только известные имена вывода ядра (25 имён §5.2).</summary>
    public bool IsValid() =>
        DbVersion is >= 10 and <= 18
        && Connections is >= 20 and <= 999999
        && InDomain(DbType, "web", "oltp", "dw", "mixed")
        && InDomain(HdType, "ssd", "san", "hdd", "nvme")
        && InDomain(DbSize, "less_ram", "mid_ram", "greater_ram")
        && ExcludeParams is not null
        && ExcludeParams.All(PgTune.KnownParameterNames.Contains);

    /// <summary>Runtime-склейка (паттерн MovesOptions.ToRuntime): Provisioning
    /// не зависит от PgWorker.App — строки домена передаются как есть,
    /// маппинг в enum ядра — фабрика входов (PgtuneInputsFactory).</summary>
    public PgtuneSettings ToRuntime() => new(
        DbVersion, DbType, HdType, DbSize, Connections,
        new HashSet<string>(ExcludeParams, StringComparer.Ordinal));

    // Регистронезависимая принадлежность домену строк (маппинг фабрики — тоже
    // case-insensitive; десктоп в домен не входит — запрещён целиком).
    private static bool InDomain(string value, params string[] domain) =>
        !string.IsNullOrWhiteSpace(value)
        && domain.Contains(value, StringComparer.OrdinalIgnoreCase);
}

/// <summary>HTTP API воркера (arch/14 §1.1): advertise-URL в /pgworker/api/&lt;id&gt;
/// + стендовый сид-эндпоинт.</summary>
public sealed class ApiOptions
{
    /// <summary>URL API, достижимый клиентами (панелью); пусто → fail-fast старта.</summary>
    public string AdvertiseUrl { get; set; } = "";

    /// <summary>Демо-сид-эндпоинт POST /api/seed/demo (стенд; default false).</summary>
    public bool EnableSeedEndpoint { get; set; }

    /// <summary>mTLS-грань API (arch/14 §1.1, t03): серверный серт + ClientCA;
    /// AllowInsecureHttp — ТОЛЬКО WAF-тесты (warning при старте).</summary>
    public TlsOptions Tls { get; set; } = new();
}

/// <summary>mTLS HTTP API (arch/14 §1.1, t03): PEM/PATH-дуализм env-секретов
/// PGW_API_TLS_{CERT,KEY,CLIENT_CA}[_PATH] (env → конфиг — ApiTlsEndpoints).</summary>
public sealed class TlsOptions
{
    /// <summary>PEM серверного серта (или CERT_PATH файл).</summary>
    public string? ServerCertPem { get; set; }

    public string? ServerCertPath { get; set; }

    /// <summary>PEM приватного ключа PKCS#8 (или KEY_PATH файл).</summary>
    public string? ServerKeyPem { get; set; }

    public string? ServerKeyPath { get; set; }

    /// <summary>PEM per-install API-CA клиентских сертов (или CA_PATH файл).</summary>
    public string? ClientCaPem { get; set; }

    public string? ClientCaPath { get; set; }

    /// <summary>Отключить mTLS (HTTP без TLS) — ТОЛЬКО WAF-тесты (default false).</summary>
    public bool AllowInsecureHttp { get; set; }
}

/// <summary>etcd-кластер: HTTP JSON gateway endpoints (failover по списку).</summary>
public sealed class EtcdOptions
{
    public string[] Endpoints { get; set; } = [];

    /// <summary>
    /// Адреса etcd для КОНТЕЙНЕРОВ нод (Patroni DCS): когда PgWorker ходит в etcd
    /// по localhost/tunnel, а ноды — через docker-сеть (host.docker.internal:порт).
    /// Null/пусто → используются Endpoints.
    /// </summary>
    public string[]? AdvertisedEndpoints { get; set; }
}

/// <summary>docker: режим (Plain|Swarm), хосты plain / manager swarm, порты, образы.</summary>
public sealed class DockerOptions
{
    /// <summary>Plain | Swarm (регистронезависимо).</summary>
    public string Mode { get; set; } = "Plain";

    /// <summary>Таблица хостов plain-режима (spec §5.2): имя + endpoint Engine API.</summary>
    public DockerHostOptions[] Hosts { get; set; } = [];

    /// <summary>Endpoint manager-узла swarm (spec §5.3).</summary>
    public string? SwarmManager { get; set; }

    public PortRangeOptions PortRange { get; set; } = new();

    public DockerImagesOptions Images { get; set; } = new();

    /// <summary>R1: false → узел без pg_doorman (порт 6432 не публикуется).</summary>
    public bool EnableDoorman { get; set; } = true;

    /// <summary>
    /// Advertised-имя docker-хоста в записях etcd (portalloc/dsn): адреса нод
    /// обязаны быть резолвимы КЛИЕНТАМИ записей — панелью (arch/16 advertised-
    /// правило, прецедент KafkaWorker:AdvertisedClientHost). Внутреннее имя
    /// docker-хоста (напр. "local") резолвится только контейнерами воркеров
    /// (extra_hosts) — пробы панели уходили в DNS-таймаут. Single-host/tunnel-
    /// развёртывания (стенд: host.docker.internal); null → имя docker-хоста как
    /// есть (прод: имена хостов резолвимы клиентами сами). Требует Mode=Plain и
    /// ровно один хост в Hosts (fail-fast старта).
    /// </summary>
    public string? AdvertisedHost { get; set; }

    /// <summary>TLS к Engine API (arch/14 §2.2.1, t03); null — без TLS (unix/dev).</summary>
    public DockerTlsOptions? Tls { get; set; }

    /// <summary>SSH-туннели ssh://-хостов (arch/14 §2.2.1, t03); null — дефолты.</summary>
    public SshTunnelOptions? Ssh { get; set; }
}

/// <summary>Хост plain-режима: {Name, Endpoint} (tcp://host:2375 | unix:///var/run/docker.sock).</summary>
public sealed class DockerHostOptions
{
    public string Name { get; set; } = "";

    public string Endpoint { get; set; } = "";
}

/// <summary>Диапазон базовых портов нод [From, To): pg=base, patroni=+3000, doorman=+1500.</summary>
public sealed class PortRangeOptions
{
    public int From { get; set; } = 15000;

    public int To { get; set; } = 16000;
}

/// <summary>Образы контейнеров (узел кластера).</summary>
public sealed class DockerImagesOptions
{
    public string Node { get; set; } = "pgworker-node:dev";
}

/// <summary>Интервалы циклов (spec §6.2/§10) и задержка после ошибки тика.</summary>
public sealed class LoopsOptions
{
    public int ScanIntervalSec { get; set; } = 5;

    public int KeepaliveSec { get; set; } = 5;

    public int SnapshotIntervalMin { get; set; } = 360;

    public int ErrorDelayMs { get; set; } = 2000;
}

/// <summary>Пороги надзора: rebuild ноды / эвакуация шарда / бюджет ожидания Patroni
/// + пороги cutover-переездов (t01, spec §9).</summary>
public sealed class ThresholdsOptions
{
    public int NodeDeadSec { get; set; } = 90;

    public int ShardDeadSec { get; set; } = 300;

    public int PatroniBootSec { get; set; } = 300;

    /// <summary>Бюджет ожидания слота на догон LSN при cutover (t01, spec §9).</summary>
    public int CutoverTimeoutSec { get; set; } = 90;

    /// <summary>Бюджет недоступности шарда в ожиданиях переезда (t01, spec §9).</summary>
    public int ConnFailBudgetSec { get; set; } = 120;

    /// <summary>Бэкофф ретраев provision (arch/14 §5 A): база задержки (n-й фейл подряд → Base·2^(n−1)).</summary>
    public int ProvisionRetryBaseSec { get; set; } = 5;

    /// <summary>Кап задержки бэкоффа provision (spec §3.5 E4).</summary>
    public int ProvisionRetryMaxSec { get; set; } = 60;
}

/// <summary>Параметры процессов переезда бакетов (t01, spec §9; дефолты — из скриптов
/// move-bucket.sh/abort-move.sh). Склейка с порогами Thresholds — ToRuntime.</summary>
public sealed class MovesOptions
{
    /// <summary>Поллинг внутри ожиданий (copy-wait, слот).</summary>
    public int PollIntervalSec { get; set; } = 2;

    /// <summary>Пауза после FROZEN (TTL кэша роутера).</summary>
    public int FreezeWaitSec { get; set; } = 5;

    /// <summary>lock_timeout барьера заморозки P1.</summary>
    public int FreezeLockTimeoutSec { get; set; } = 5;

    /// <summary>Попытки заморозки (lock_timeout → пауза → повтор).</summary>
    public int FreezeLockTries { get; set; } = 3;

    /// <summary>Защита abort от живого mover (по updated_unix, Д12).</summary>
    public int AbortMinAgeSec { get; set; } = 120;

    /// <summary>failover=true у подписок (PG17+; false для PG16-образа, R1/Д11).</summary>
    public bool FailoverSlots { get; set; } = true;

    /// <summary>host CONNECTION-строк подписок, как издатель виден ИЗ контейнеров
    /// приёмников (single-docker-host стенды: host.docker.internal; в проде null —
    /// адреса dsn-ключа достижимы из контейнеров напрямую; паттерн уже
    /// используемого Etcd:AdvertisedEndpoints).</summary>
    public string? AdvertisedPublisherHost { get; set; }

    /// <summary>Репарация брошенных статусов: возраст без заявки для
    /// SYNCING/ABORTING (600 = StaleMoveSeconds панели, spec §2.3).</summary>
    public int RepairStaleSec { get; set; } = 600;

    /// <summary>Репарация FROZEN (заморозка режет запись — чиним быстрее;
    /// 120 = AbortMinAgeSec, spec §2.3).</summary>
    public int RepairFrozenSec { get; set; } = 120;

    /// <summary>Runtime-опции процессов переезда: склейка Moves + Thresholds (t01 задача 17).</summary>
    public MovesRuntimeOptions ToRuntime(ThresholdsOptions thresholds) => new(
        PollIntervalSec, FreezeWaitSec, FreezeLockTimeoutSec, FreezeLockTries,
        AbortMinAgeSec, FailoverSlots, thresholds.CutoverTimeoutSec, thresholds.ConnFailBudgetSec,
        AdvertisedPublisherHost, RepairStaleSec, RepairFrozenSec);
}

/// <summary>Параллелизм процессов разных кластеров (SemaphoreSlim).</summary>
public sealed class ParallelismOptions
{
    public int MaxClusters { get; set; } = 4;
}

/// <summary>Снапшоты P12: каталог тома и ретеншн файлов.</summary>
public sealed class SnapshotOptions
{
    public string Dir { get; set; } = "/snapshots";

    public int RetentionFiles { get; set; } = 10;

    /// <summary>Интервал обслуживания etcd: compact + defrag (минуты, по умолчанию 60).</summary>
    public int MaintenanceIntervalMin { get; set; } = 60;
}

/// <summary>Дефолт значения ключа nodes/&lt;n&gt;/app_params (spec §3.1): libpq-строка
/// keyword=value; применяется put-if-absent (P2.5'/A5/надзор-C).</summary>
public sealed class AppParamsOptions
{
    public string Default { get; set; } = "sslmode=require";
}

/// <summary>Подсистема бэкапов шардов (arch/19, t01): каркас без процессной
/// логики (джобы/агенты — t02–t07); Enabled=false — поведение воркера не
/// меняется.</summary>
public sealed class BackupsOptions
{
    /// <summary>Вкл/выкл подсистемы (t01: только каркас; потребители — t02+).</summary>
    public bool Enabled { get; set; }

    public BackupsS3Options S3 { get; set; } = new();

    public BackupsPolicyOptions Policy { get; set; } = new();

    public BackupsStagingOptions Staging { get; set; } = new();

    public BackupsAgentOptions Agent { get; set; } = new();

    public BackupsJobOptions Job { get; set; } = new();

    public BackupsRetryOptions Retry { get; set; } = new();

    /// <summary>WAL-поток (t03, arch/19 §3/§9): расписание контроля, пороги.</summary>
    public BackupsWalOptions Wal { get; set; } = new();

    /// <summary>Ретенционный проход (t06, arch/19 §9): период и FAILED-глубина.</summary>
    public BackupsRetentionPassOptions Retention { get; set; } = new();

    /// <summary>Квота bucket установки (t06, arch/19 §9).</summary>
    public BackupsQuotaOptions Quota { get; set; } = new();

    /// <summary>Восстановление шарда (t05, arch/19 §3.5/§9): бюджеты.</summary>
    public BackupsRestoreOptions Restore { get; set; } = new();

    /// <summary>Супервизор бэкапов (t07, arch/19 §4/§9): период сверок S3↔etcd
    /// per-cluster и глобального лидер-прохода; TTL сирот S3.</summary>
    public BackupsSupervisorOptions Supervisor { get; set; } = new();

    /// <summary>Runtime-опции подсистемы бэкапов: склейка Backups-секции
    /// (t02: джобы/ретраи; t03: advertised-S3/Wal-пороги; t06: ретенция/квота)
    /// — именованными аргументами: record расширялся с обеих сторон.</summary>
    public BackupsRuntimeOptions ToRuntime() => new(
        Enabled: Enabled,
        FullMaxAgeSec: Policy.FullMaxAgeSec,
        VerifyOnCreate: Policy.VerifyOnCreate,
        VerifyIntervalSec: Policy.VerifyIntervalSec,
        S3Endpoint: S3.Endpoint,
        S3AdvertisedEndpoint: S3.AdvertisedEndpoint,
        S3Region: S3.Region,
        S3Bucket: S3.Bucket,
        S3AccessKey: S3.AccessKey,
        S3SecretKey: S3.SecretKey,
        S3PathStyle: S3.PathStyle,
        JobImage: Job.Image,
        RetryBaseSec: Retry.BaseSec,
        RetryMaxSec: Retry.MaxSec,
        StagingDir: Staging.Dir,
        StagingQuotaBytes: Staging.QuotaBytes,
        AgentCpu: Agent.Cpu,
        AgentMem: Agent.Mem,
        WalVerifyIntervalSec: Wal.VerifyIntervalSec,
        WalLagMaxSegments: Wal.LagMaxSegments,
        WalStaleSec: Wal.StaleSec,
        PolicyRetentionDays: Policy.Retention.Days,
        PolicyRetentionWeeks: Policy.Retention.Weeks,
        PolicyRetentionMonths: Policy.Retention.Months,
        RetentionIntervalSec: Retention.IntervalSec,
        RetentionKeepFailed: Retention.KeepFailed,
        QuotaBytes: Quota.Bytes,
        QuotaWarnPercent: Quota.WarnPercent,
        QuotaCritPercent: Quota.CritPercent,
        RestoreRecoveryTimeoutSec: Restore.RecoveryTimeoutSec,
        SupervisorIntervalSec: Supervisor.IntervalSec,
        SupervisorOrphanTtlSec: Supervisor.OrphanTtlSec,
        JobFullTimeoutSec: Job.FullTimeoutSec,
        JobVerifyTimeoutSec: Job.VerifyTimeoutSec,
        JobRestoreTimeoutSec: Job.RestoreTimeoutSec);

    /// <summary>Fail-fast старта (образец TLS arch/14 §2.2.1): Enabled=true
    /// обязан иметь полный S3-комплект; false — подсистема не активна.
    /// Диапазоны ретенции/квоты (t06) — всегда: мусорный конфиг виден на старте.</summary>
    public bool IsValid()
        => (!Enabled
            || (!string.IsNullOrWhiteSpace(S3.Endpoint)
                && !string.IsNullOrWhiteSpace(S3.Bucket)
                && !string.IsNullOrWhiteSpace(S3.AccessKey)
                && !string.IsNullOrWhiteSpace(S3.SecretKey)
                && !string.IsNullOrWhiteSpace(Job.Image)))
           && Quota.WarnPercent < Quota.CritPercent
           && Quota.CritPercent <= 100
           && Retention.IntervalSec >= 60
           && Retention.KeepFailed >= 5
           && Supervisor.IntervalSec >= 60          // как Retention.IntervalSec — тик не молотит
           && Supervisor.OrphanTtlSec >= 0          // 0 — авто-удаление выключено (только алерт)
           && Job.FullTimeoutSec > 0
           && Job.VerifyTimeoutSec > 0
           && Job.RestoreTimeoutSec > 0;
}

/// <summary>Параметры ретенционного прохода (t06, arch/19 §9): период и
/// FAILED-глубина. Имя НЕ BackupsRetentionOptions — то занято GFS-гранулами
/// политики (BackupsPolicyOptions.Retention).</summary>
public sealed class BackupsRetentionPassOptions
{
    public int IntervalSec { get; set; } = 600;

    public int KeepFailed { get; set; } = 20;
}

/// <summary>Квота bucket установки (t06, arch/19 §9): Bytes=0 — квота не задана.</summary>
public sealed class BackupsQuotaOptions
{
    public long Bytes { get; set; }

    public int WarnPercent { get; set; } = 80;

    public int CritPercent { get; set; } = 90;
}

/// <summary>Restore-подсистема (t05, arch/19 §9): бюджет локального наката
/// WAL restore-джобом (фаза recovering); env-переменная не нужна — не секрет.</summary>
public sealed class BackupsRestoreOptions
{
    public int RecoveryTimeoutSec { get; set; } = 1800;
}

/// <summary>Супервизор бэкапов (t07, arch/19 §4/§9): период сверок S3↔etcd
/// per-cluster и глобального лидер-прохода (реестр сирот
/// /pgworker/backups/orphans); OrphanTtlSec=0 — авто-удаление выключено,
/// только алерт панели.</summary>
public sealed class BackupsSupervisorOptions
{
    public int IntervalSec { get; set; } = 600;

    public long OrphanTtlSec { get; set; } = 604800;
}

/// <summary>Образ джоба полного бэкапа (arch/19 §2/§9, t02): собирается из
/// docker/PgWorker.Backup.Dockerfile; запуск — воркер, содержимое — без .NET.
/// Таймауты (t07, arch/19 §6/§9) — бюджеты зависших джобов: полный/verify/restore.</summary>
public sealed class BackupsJobOptions
{
    public string Image { get; set; } = "pgworker-backup:dev";

    /// <summary>Бюджет активного полного (PLANNED/RUNNING/UPLOADING), c (t07):
    /// возраст > лимита → kill+rm + FAILED job-timeout → переснятие по бэкоффу.</summary>
    public int FullTimeoutSec { get; set; } = 21600;

    /// <summary>Бюджет running verify-джоба по docker-факту StartedAt, c (t07):
    /// kill+rm, кандидат остаётся PENDING с checked_unix = now (попытка зачтена).</summary>
    public int VerifyTimeoutSec { get; set; } = 21600;

    /// <summary>Бюджет RUNNING-фазы restore-заявки, c (t07): kill+rm + FAILED
    /// restore-job-timeout с чисткой щита initialize; REJOINING не таймаутится.</summary>
    public int RestoreTimeoutSec { get; set; } = 86400;
}

/// <summary>Бэкофф переснятия FAILED-полного (arch/19 §2, t02):
/// задержка n-й попытки после последнего COMPLETED =
/// min(BaseSec·2^(n−1), MaxSec), без лимита попыток.</summary>
public sealed class BackupsRetryOptions
{
    public int BaseSec { get; set; } = 300;

    public int MaxSec { get; set; } = 3600;
}

/// <summary>S3-хранилище бэкапов (arch/19 §5/§7): per-install креды — ТОЛЬКО
/// env воркера (PGW_BACKUP_S3_*), не etcd/не git; PathStyle=true — клиент
/// MinIO-режима.</summary>
public sealed class BackupsS3Options
{
    public string Endpoint { get; set; } = "";

    /// <summary>Регион (опц.; MinIO не требует — null).</summary>
    public string? Region { get; set; }

    public string Bucket { get; set; } = "";

    public string AccessKey { get; set; } = "";

    public string SecretKey { get; set; } = "";

    public bool PathStyle { get; set; } = true;

    /// <summary>Endpoint, как S3 виден ИЗ контейнеров агентов/джобов (single-host:
    /// host.docker.internal; null → Endpoint как есть — паттерн Etcd:AdvertisedEndpoints,
    /// Moves:AdvertisedPublisherHost). Реализация-деталь t03 (env-адресация агента,
    /// arch/19 §7); стенд-включение подсистемы — t02.</summary>
    public string? AdvertisedEndpoint { get; set; }
}

/// <summary>Дефолт per-cluster политики: ключ
/// /pgworker/backups/&lt;C&gt;/policy отсутствует → эти значения (arch/19 §4).</summary>
public sealed class BackupsPolicyOptions
{
    public BackupsRetentionOptions Retention { get; set; } = new();

    public long FullMaxAgeSec { get; set; } = 86400;

    public bool VerifyOnCreate { get; set; } = true;

    /// <summary>Период перепроверки оставшихся полных, c (t04, arch/19 §4):
    /// verify.interval_sec policy-ключа перекрывает; &lt;= 0 — периодика выключена.</summary>
    public long VerifyIntervalSec { get; set; } = 604800;
}

/// <summary>GFS-ретенция полных бэкапов (дни/недели/месяцы, t06).</summary>
public sealed class BackupsRetentionOptions
{
    public int Days { get; set; } = 7;

    public int Weeks { get; set; } = 4;

    public int Months { get; set; } = 6;
}

/// <summary>Staging джобов/агентов бэкапов (arch/19 §6): ephemeral volume с
/// квотой (guard «нет места» → FAILED, реализация t02; null — без квоты).</summary>
public sealed class BackupsStagingOptions
{
    public string Dir { get; set; } = "/backup-staging";

    public long? QuotaBytes { get; set; }
}

/// <summary>Ресурсные лимиты джоба/агента бэкапов (arch/19 §6) →
/// HostConfig.NanoCPUs/Memory; null — без лимита (образец request_* нод,
/// arch/14 §2.4 п.4). Cpu — ядра, Mem — байты.</summary>
public sealed class BackupsAgentOptions
{
    public double? Cpu { get; set; }

    public long? Mem { get; set; }
}

/// <summary>WAL-поток шарда (t03, arch/19 §3/§9): период list/контроля цепочки,
/// порог отставания в сегментах, порог тишины загрузок.</summary>
public sealed class BackupsWalOptions
{
    public int VerifyIntervalSec { get; set; } = 30;

    public int LagMaxSegments { get; set; } = 1024;

    public int StaleSec { get; set; } = 300;
}
