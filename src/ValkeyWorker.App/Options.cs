namespace ValkeyWorker.App;

// Конфигурация ValkeyWorker (arch/21 §8): секция "ValkeyWorker" в appsettings.json +
// env-оверрайды (ValkeyWorker__Etcd__Endpoints__0=http://…). Per-install env-секреты —
// только TLS HTTP API (VWK_API_TLS_*, arch/21 §4); per-cluster секреты (app/admin) —
// в etcd.

/// <summary>Корневые настройки сервиса.</summary>
public sealed class ValkeyWorkerOptions
{
    public EtcdOptions Etcd { get; set; } = new();

    public DockerOptions Docker { get; set; } = new();

    public LoopsOptions Loops { get; set; } = new();

    public ThresholdsOptions Thresholds { get; set; } = new();

    public ParallelismOptions Parallelism { get; set; } = new();

    public SnapshotOptions Snapshots { get; set; } = new();

    /// <summary>
    /// Advertised-хост клиентского порта ноды (arch/21 §2): null → имя docker-хоста
    /// размещения. Значение обязано резолвиться клиентами (попадает в endpoints).
    /// Локальные стенды: host.docker.internal.
    /// </summary>
    public string? AdvertisedClientHost { get; set; }

    /// <summary>HTTP API воркера (arch/21 §1.1): advertise-URL + стендовый сид.</summary>
    public ApiOptions Api { get; set; } = new();

    /// <summary>Метрики воркера (arch/18 §2.2/§2.6/§4.2): экспозиция + тик
    /// коллектора INFO.</summary>
    public ValkeyWorkerMetricsOptions Metrics { get; set; } = new();
}

/// <summary>Метрики ValkeyWorker (arch/18 §2.6/§4.2): базовая экспозиция +
/// интервал тика коллектора INFO.</summary>
public sealed class ValkeyWorkerMetricsOptions : Shared.Metrics.MetricsOptions
{
    /// <summary>Тик коллектора INFO, сек (default 30; arch/18 §4.2).</summary>
    public int CollectIntervalSec { get; set; } = 30;
}

/// <summary>HTTP API воркера (arch/21 §1.1): advertise-URL в /valkeyworker/api/&lt;id&gt;
/// + стендовый сид-эндпоинт.</summary>
public sealed class ApiOptions
{
    /// <summary>URL API, достижимый клиентами (панелью); пусто → fail-fast старта.</summary>
    public string AdvertiseUrl { get; set; } = "";

    /// <summary>mTLS-конфигурация HTTP API (arch/21 §1.1).</summary>
    public TlsOptions Tls { get; set; } = new();

    /// <summary>Демо-сид-эндпоинт POST /api/seed/demo (стенд; default false).</summary>
    public bool EnableSeedEndpoint { get; set; }
}

/// <summary>
/// mTLS HTTP API воркера (arch/21 §1.1): серверный серт+ключ и CA клиентских
/// сертов per-install API-PKI; env VWK_API_TLS_{CERT,KEY,CLIENT_CA}[_PATH]
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

    /// <summary>true — HTTP без TLS; ТОЛЬКО WAF-фикстуры тестов (arch/21 §1.1).</summary>
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

    /// <summary>Таблица хостов plain-режима: имя + endpoint Engine API.</summary>
    public DockerHostOptions[] Hosts { get; set; } = [];

    /// <summary>Endpoint manager-узла swarm.</summary>
    public string? SwarmManager { get; set; }

    public PortRangeOptions PortRange { get; set; } = new();

    public DockerImagesOptions Images { get; set; } = new();
}

/// <summary>Хост plain-режима: {Name, Endpoint} (tcp://host:2375 | unix:///var/run/docker.sock).</summary>
public sealed class DockerHostOptions
{
    public string Name { get; set; } = "";

    public string Endpoint { get; set; } = "";
}

/// <summary>Диапазон клиентских портов нод [From, To): 17000–17999 (arch/21 §2).</summary>
public sealed class PortRangeOptions
{
    public int From { get; set; } = 17000;

    public int To { get; set; } = 17999;
}

/// <summary>Образы контейнеров (нода valkey).</summary>
public sealed class DockerImagesOptions
{
    public string Node { get; set; } = "valkey/valkey:9.1.2";
}

/// <summary>Интервалы циклов и задержка после ошибки тика (arch/21 §8).</summary>
public sealed class LoopsOptions
{
    public int ScanIntervalSec { get; set; } = 5;

    public int KeepaliveSec { get; set; } = 5;

    public int ErrorDelayMs { get; set; } = 2000;

    // Перенос механики снапшотов образца kfw/pg (SnapshotLoop-ритм лидера):
    // spec §4.1 и arch/21 §8 поле не указывают — дефолт 360 мин как у образцов,
    // транспорентно для домена (прецедент примечания плана про
    // Snapshots.MaintenanceIntervalMin).
    public int SnapshotIntervalMin { get; set; } = 360;
}

/// <summary>Пороги процессов (arch/21 §8).</summary>
public sealed class ThresholdsOptions
{
    /// <summary>Бюджет готовности ноды (PING) при provisioning (V4); тесты ≤ 100.</summary>
    public int NodeBootSec { get; set; } = 120;

    /// <summary>Молчание ноды дольше порога → UNREACHABLE + пересоздание (C).</summary>
    public int NodeDeadSec { get; set; } = 90;
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

    // Перенос механики снапшотов образца kfw (SnapshotJob.MaintainAsync —
    // обслуживание не чаще раза в интервал); в spec §4.1/arch/21 §8 не указано —
    // осознанное решение плана (транспорентно для домена).
    public int MaintenanceIntervalMin { get; set; } = 60;
}
