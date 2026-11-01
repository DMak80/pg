namespace EtcdWorker.App;

// Конфигурация EtcdWorker (arch/25 §8): секция "EtcdWorker" в appsettings.json +
// env-оверрайды (EtcdWorker__Etcd__Endpoints__0=http://…). Per-install env-секреты —
// TLS HTTP API (ETCDW_API_TLS_*) и docker-транспорт (ETCDW_DOCKER_*); per-install
// S3-пар НЕТ — цель DR-слепков резолвится из /s3/clusters/ (arch/25 §4), значимое
// состояние — в etcd.

/// <summary>Корневые настройки сервиса.</summary>
public sealed class EtcdWorkerOptions
{
    public EtcdOptions Etcd { get; set; } = new();
    public DockerOptions Docker { get; set; } = new();
    public LoopsOptions Loops { get; set; } = new();
    public ThresholdsOptions Thresholds { get; set; } = new();
    public SnapshotOptions Snapshots { get; set; } = new();
    public ApiOptions Api { get; set; } = new();
}

/// <summary>HTTP API воркера (arch/25 §1.1): advertise-URL в /etcdworker/api/&lt;id&gt;.</summary>
public sealed class ApiOptions
{
    /// <summary>URL API, достижимый клиентами (панелью); пусто → fail-fast старта.</summary>
    public string AdvertiseUrl { get; set; } = "";

    /// <summary>mTLS-конфигурация HTTP API (arch/25 §1.1).</summary>
    public TlsOptions Tls { get; set; } = new();
}

/// <summary>
/// mTLS HTTP API воркера (arch/25 §1.1): серверный серт+ключ и CA клиентских
/// сертов per-install API-CA; env ETCDW_API_TLS_{CERT,KEY,CLIENT_CA}[_PATH]
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

    /// <summary>true — HTTP без TLS; ТОЛЬКО WAF-фикстуры тестов (arch/25 §1.1).</summary>
    public bool AllowInsecureHttp { get; set; }
}

/// <summary>etcd-кластер: HTTP JSON gateway endpoints (failover по списку).</summary>
public sealed class EtcdOptions
{
    public string[] Endpoints { get; set; } = [];
}

/// <summary>docker: режим (Plain|Swarm), сеть контура, порты, образы (arch/25 §2/§8).</summary>
public sealed class DockerOptions
{
    /// <summary>Plain | Swarm (регистронезависимо).</summary>
    public string Mode { get; set; } = "Plain";

    /// <summary>Таблица хостов plain-режима: Plain — РОВНО ОДИН хост (fail-fast
    /// иначе, arch/25 §2: мультихост — Swarm).</summary>
    public DockerHostOptions[] Hosts { get; set; } = [];

    /// <summary>Endpoint manager-узла swarm.</summary>
    public string? SwarmManager { get; set; }

    /// <summary>Общая сеть контура etcd (arch/25 §2): Plain — ensure-bridge
    /// воркером; Swarm — overlay воркера; стенд/E2E — сеть контура/сценария.</summary>
    public string Network { get; set; } = "etcdw-net";

    /// <summary>Диапазон КЛИЕНТСКИХ host-портов managed-узлов (peer — имена
    /// сети, host-портов не имеет).</summary>
    public PortRangeOptions PortRange { get; set; } = new();

    /// <summary>Образ etcd-узла (внешний, зеркалирован).</summary>
    public DockerImagesOptions Images { get; set; } = new();

    public Shared.Docker.DockerTlsOptions? Tls { get; set; }

    public Shared.Docker.SshTunnelOptions? Ssh { get; set; }

    /// <summary>Advertised-хост КЛИЕНТСКОЙ грани (только client-advertise,
    /// arch/25 §2): null → имя docker-хоста размещения.</summary>
    public string? AdvertisedHost { get; set; }
}

/// <summary>Хост plain-режима: {Name, Endpoint} (tcp://host:2375 | unix:///var/run/docker.sock).</summary>
public sealed class DockerHostOptions
{
    public string Name { get; set; } = "";

    public string Endpoint { get; set; } = "";
}

/// <summary>Диапазон host-портов клиентской грани managed-узлов [From, To]: 18000–18999.</summary>
public sealed class PortRangeOptions
{
    public int From { get; set; } = 18000;

    public int To { get; set; } = 18999;
}

/// <summary>Образ etcd-узла (arch/25 §2).</summary>
public sealed class DockerImagesOptions
{
    public string Etcd { get; set; } = "quay.io/coreos/etcd:v3.5.21";
}

/// <summary>Интервалы циклов и задержка после ошибки тика (arch/25 §8).</summary>
public sealed class LoopsOptions
{
    public int ScanIntervalSec { get; set; } = 5;

    public int KeepaliveSec { get; set; } = 5;

    public int ErrorDelayMs { get; set; } = 2000;

    /// <summary>Watchdog зависших циклов (arch/25 §6): Enabled=false — компонент
    /// не регистрируется; порог сноса = Multiplier × CheckIntervalSec.</summary>
    public Shared.Core.Hosting.WatchdogOptions Watchdog { get; set; } = new();
}

/// <summary>Пороги процессов (arch/25 §8).</summary>
public sealed class ThresholdsOptions
{
    /// <summary>Бюджет готовности ноды (health-проба) при provisioning — ≤100 с
    /// (arch/25 §3.1/§8).</summary>
    public int NodeBootSec { get; set; } = 100;

    /// <summary>Молчание ноды дольше порога → UNREACHABLE, деструктива нет
    /// (S7, arch/25 §3.2).</summary>
    public int NodeDeadSec { get; set; } = 300;

    /// <summary>Период health-проб managed-узлов (/v3/maintenance/status, arch/25 §3.2).</summary>
    public int HealthIntervalSec { get; set; } = 15;
}

/// <summary>DR-слепки (arch/25 §4/§8): расписание, ретенция, выгрузка;
/// креды/endpoint'ы/bucket НЕ здесь — цель резолвится из /s3/clusters/.</summary>
public sealed class SnapshotOptions
{
    public string Dir { get; set; } = "/snapshots";
    public int RetentionFiles { get; set; } = 10;
    public int IntervalMin { get; set; } = 360;
    public int MinIntervalSec { get; set; } = 300;
    public int MaintenanceIntervalMin { get; set; } = 60;
    public int RetentionObjects { get; set; } = 28;
    public int RetryIntervalSec { get; set; } = 300;
    public int TimeoutSec { get; set; } = 30;

    /// <summary>CA-bundle https S3-endpoint цели слепков (опц.).</summary>
    public SnapshotS3Options S3 { get; set; } = new();
}

public sealed class SnapshotS3Options
{
    public string? CaBundlePath { get; set; }
}
