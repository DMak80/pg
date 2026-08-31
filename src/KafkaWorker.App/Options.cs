namespace KafkaWorker.App;

// Конфигурация KafkaWorker (arch/16 §8): секция "KafkaWorker" в appsettings.json +
// env-оверрайды (KafkaWorker__Etcd__Endpoints__0=http://…). Env-секретов per-install
// НЕТ (единственный секрет — per-cluster app_password в etcd, arch/16 §4).

/// <summary>Корневые настройки сервиса.</summary>
public sealed class KafkaWorkerOptions
{
    public EtcdOptions Etcd { get; set; } = new();

    public DockerOptions Docker { get; set; } = new();

    public LoopsOptions Loops { get; set; } = new();

    public ThresholdsOptions Thresholds { get; set; } = new();

    public ParallelismOptions Parallelism { get; set; } = new();

    public SnapshotOptions Snapshots { get; set; } = new();

    /// <summary>
    /// Advertised-хост CLIENT-listener (arch/16 §2.1): null → имя docker-хоста
    /// ноды (placement). Значение обязано резолвиться клиентами (попадает в
    /// endpoints → bootstrap). Локальные стенды: host.docker.internal.
    /// </summary>
    public string? AdvertisedClientHost { get; set; }

    /// <summary>HTTP API воркера (arch/16 §1.1): advertise-URL + стендовый сид.</summary>
    public ApiOptions Api { get; set; } = new();
}

/// <summary>HTTP API воркера (arch/16 §1.1): advertise-URL в /kafkaworker/api/&lt;id&gt;
/// + стендовый сид-эндпоинт.</summary>
public sealed class ApiOptions
{
    /// <summary>URL API, достижимый клиентами (панелью); пусто → fail-fast старта.</summary>
    public string AdvertiseUrl { get; set; } = "";

    /// <summary>Демо-сид-эндпоинт POST /api/seed/demo (стенд; default false).</summary>
    public bool EnableSeedEndpoint { get; set; }
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

/// <summary>Диапазон клиентских портов брокеров [From, To): 16000–16999 (arch/16 §2.1).</summary>
public sealed class PortRangeOptions
{
    public int From { get; set; } = 16000;

    public int To { get; set; } = 16999;
}

/// <summary>Образы контейнеров (брокер kafka).</summary>
public sealed class DockerImagesOptions
{
    public string Node { get; set; } = "apache/kafka:4.0.0";
}

/// <summary>Интервалы циклов и задержка после ошибки тика (arch/16 §8).</summary>
public sealed class LoopsOptions
{
    public int ScanIntervalSec { get; set; } = 5;

    public int KeepaliveSec { get; set; } = 5;

    /// <summary>Тик автосинка топиков (волна C, arch/16 §5 D).</summary>
    public int TopicSyncIntervalSec { get; set; } = 15;

    /// <summary>Тик reassignment партиций (t02, arch/16 §5 I: drain/balance).</summary>
    public int ReassignIntervalSec { get; set; } = 15;

    /// <summary>Максимум партиций в одной подаче reassignment (батчи, t02).</summary>
    public int ReassignBatchPartitions { get; set; } = 10;

    public int SnapshotIntervalMin { get; set; } = 360;

    public int ErrorDelayMs { get; set; } = 2000;
}

/// <summary>Пороги процессов (arch/16 §8).</summary>
public sealed class ThresholdsOptions
{
    /// <summary>Бюджет ожидания сборки кластера при provisioning (K4).</summary>
    public int BrokerBootSec { get; set; } = 600;

    /// <summary>Молчание брокера дольше порога → UNREACHABLE + пересоздание (C).</summary>
    public int NodeDeadSec { get; set; } = 90;

    /// <summary>Бюджет exec kafka-reassign-partitions CLI в контейнере (t02).</summary>
    public int ReassignExecSec { get; set; } = 180;

    /// <summary>Дедуп переподачи одного батча reassignment (t02, KIP-455).</summary>
    public int ReassignRetrySubmitSec { get; set; } = 120;
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

    public int MaintenanceIntervalMin { get; set; } = 60;
}
