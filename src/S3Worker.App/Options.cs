namespace S3Worker.App;

// Конфигурация S3Worker (arch/24 §8): секция "S3Worker" в appsettings.json +
// env-оверрайды (S3Worker__Etcd__Endpoints__0=http://…). Per-install env-секреты —
// TLS HTTP API (S3W_API_TLS_*), docker-транспорт (S3W_DOCKER_*) и спец-ключ
// бэкапа etcd (S3W_ETCD_BACKUP_{ACCESSKEY,SECRETKEY}, arch/23 §6); per-cluster
// значимое состояние и клиентские креды — в etcd.

/// <summary>Корневые настройки сервиса.</summary>
public sealed class S3WorkerOptions
{
    public EtcdOptions Etcd { get; set; } = new();

    /// <summary>Имя per-install bucket (arch/19 §5), создаётся оркестратором
    /// на каждой ноде кластера (arch/24 §8).</summary>
    public string Bucket { get; set; } = "";

    public DockerOptions Docker { get; set; } = new();

    public LoopsOptions Loops { get; set; } = new();

    public OwnS3Options OwnS3 { get; set; } = new();

    public ThresholdsOptions Thresholds { get; set; } = new();

    public ParallelismOptions Parallelism { get; set; } = new();

    public SnapshotOptions Snapshots { get; set; } = new();

    /// <summary>HTTP API воркера (arch/24 §1.1): advertise-URL + стендовый сид.</summary>
    public ApiOptions Api { get; set; } = new();
}

/// <summary>HTTP API воркера (arch/24 §1.1): advertise-URL в /s3worker/api/&lt;id&gt;
/// + стендовый сид-эндпоинт.</summary>
public sealed class ApiOptions
{
    /// <summary>URL API, достижимый клиентами (панелью); пусто → fail-fast старта.</summary>
    public string AdvertiseUrl { get; set; } = "";

    /// <summary>mTLS-конфигурация HTTP API (arch/24 §1.1).</summary>
    public TlsOptions Tls { get; set; } = new();

    /// <summary>Демо-сид-эндпоинт POST /api/seed/demo (стенд; default false).</summary>
    public bool EnableSeedEndpoint { get; set; }
}

/// <summary>
/// mTLS HTTP API воркера (arch/24 §1.1): серверный серт+ключ и CA клиентских
/// сертов per-install API-CA; env S3W_API_TLS_{CERT,KEY,CLIENT_CA}[_PATH]
/// (таблица TlsEndpoints.EnvBindings). AllowInsecureHttp — ТОЛЬКО WAF-тесты.
/// </summary>
public sealed class TlsOptions
{
    /// <summary>PEM серверного серта (или *_PATH файл).</summary>
    public string? ServerCertPem { get; set; }

    public string? ServerCertPath { get; set; }

    /// <summary>PEM приватного ключа сервера PKCS#8 (или *_PATH файл).</summary>
    public string? ServerKeyPem { get; set; }

    public string? ServerKeyPath { get; set; }

    /// <summary>PEM CA клиентских сертов (per-install API-CA).</summary>
    public string? ClientCaPem { get; set; }

    public string? ClientCaPath { get; set; }

    /// <summary>true — HTTP без TLS; ТОЛЬКО WAF-фикстуры тестов (arch/24 §1.1).</summary>
    public bool AllowInsecureHttp { get; set; }
}

/// <summary>etcd-кластер: HTTP JSON gateway endpoints (failover по списку).</summary>
public sealed class EtcdOptions
{
    public string[] Endpoints { get; set; } = [];
}

/// <summary>docker: режим (Plain|Swarm), хосты plain / manager swarm, порты, образы.</summary>
public sealed class DockerOptions
{
    /// <summary>Plain | Swarm (регистронезависимо).</summary>
    public string Mode { get; set; } = "Plain";

    /// <summary>Таблица хостов plain-режима: имя + endpoint Engine API (arch/23 §4 —
    /// атрибут docker_host).</summary>
    public DockerHostOptions[] Hosts { get; set; } = [];

    /// <summary>Endpoint manager-узла swarm.</summary>
    public string? SwarmManager { get; set; }

    /// <summary>Диапазон S3-портов нод (ряд pg/kafka/valkey; arch/23 §2.1 — 18001+).</summary>
    public PortRangeOptions PortRange { get; set; } = new();

    /// <summary>Образы контейнеров (нода ownS3 — arch/23 §5).</summary>
    public DockerImagesOptions Images { get; set; } = new();

    /// <summary>TLS к Docker Engine API (env S3W_DOCKER_TLS_*; arch/24 §2).</summary>
    public Shared.Docker.DockerTlsOptions? Tls { get; set; }

    /// <summary>SSH-туннель к Engine API (env S3W_DOCKER_SSH_*; arch/24 §2).</summary>
    public Shared.Docker.SshTunnelOptions? Ssh { get; set; }

    /// <summary>Стенд/E2E: ноды создаются в сети контура поставки; сеть воркером
    /// не создаётся/не удаляется (arch/24 §2).</summary>
    public string? ContourNetwork { get; set; }

    /// <summary>Advertised-хост S3-порта нод (arch/24 §2): null → имя docker-хоста
    /// размещения. Значение обязано резолвиться клиентами (попадает в endpoints).
    /// single-host/стенд: host.docker.internal; multi-host — имена хостов.</summary>
    public string? AdvertisedHost { get; set; }
}

/// <summary>Хост plain-режима: {Name, Endpoint} (tcp://host:2375 | unix:///var/run/docker.sock).</summary>
public sealed class DockerHostOptions
{
    public string Name { get; set; } = "";

    public string Endpoint { get; set; } = "";
}

/// <summary>Диапазон S3-портов нод [From, To]: 18000–18999 (arch/23 §2.1 — 18001+).</summary>
public sealed class PortRangeOptions
{
    public int From { get; set; } = 18000;

    public int To { get; set; } = 18999;
}

/// <summary>Образы контейнеров (нода ownS3; arch/24 §8).</summary>
public sealed class DockerImagesOptions
{
    public string OwnS3 { get; set; } = "owns3:dev";
}

/// <summary>Интервалы циклов и задержка после ошибки тика (arch/24 §8).</summary>
public sealed class LoopsOptions
{
    public int ScanIntervalSec { get; set; } = 5;

    public int KeepaliveSec { get; set; } = 5;

    public int ErrorDelayMs { get; set; } = 2000;

    // Ритм SnapshotLoop лидера — дефолт 360 мин как у образцов pg/kfw/vwk
    // (в arch/24 §8 поле не указано — транспорентно для домена).
    public int SnapshotIntervalMin { get; set; } = 360;

    /// <summary>Watchdog зависших циклов (arch/24 §6): Enabled=false — компонент
    /// не регистрируется; порог сноса = Multiplier × CheckIntervalSec.</summary>
    public Shared.Core.Hosting.WatchdogOptions Watchdog { get; set; } = new();
}

/// <summary>Параметры инстансов ownS3 (arch/24 §8).</summary>
public sealed class OwnS3Options
{
    /// <summary>Период reconcile mesh-репликации (с) — env
    /// OWNS3_REPLICATION_RECONCILE_INTERVAL_SECONDS инстансов (owns3/07 §9);
    /// 0/не задана — переменная не ставится (прод-дефолт OwnS3 3 ч).</summary>
    public int ReplicationReconcileIntervalSec { get; set; }
}

/// <summary>Пороги процессов (arch/24 §8).</summary>
public sealed class ThresholdsOptions
{
    /// <summary>Бюджет готовности ноды (/healthz ownS3) при provisioning — ≤100 с
    /// (spec §12.2/§14.12).</summary>
    public int NodeBootSec { get; set; } = 100;

    /// <summary>Молчание ноды дольше порога → UNREACHABLE, деструктива нет (C).</summary>
    public int NodeDeadSec { get; set; } = 90;

    /// <summary>Период сбора usage (GET /admin/storage ноды → nodes/node&lt;k&gt;/usage).</summary>
    public int HealthIntervalSec { get; set; } = 15;

    /// <summary>Период зонда F (arch/24 §8): гейт по checked_unix ключа connectivity.</summary>
    public int SyncIntervalSec { get; set; } = 60;

    /// <summary>Бюджет страниц сверки на сторону пары (spec §3.3; 20 = 20 000 объектов).</summary>
    public int SyncMaxPages { get; set; } = 20;
}

/// <summary>Параллелизм обработки кластеров тика.</summary>
public sealed class ParallelismOptions
{
    public int MaxClusters { get; set; } = 4;
}

/// <summary>Снапшоты etcd P12 (каталог тома + ретеншн).</summary>
public sealed class SnapshotOptions
{
    public string Dir { get; set; } = "/snapshots";

    public int RetentionFiles { get; set; } = 10;

    // Обслуживание ретенции не чаще раза в интервал (SnapshotJob.MaintainAsync,
    // образец kfw/pg) — транспорентно для домена.
    public int MaintenanceIntervalMin { get; set; } = 60;
}
