using PgWorker.Moves;

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

    public int PatroniBootSec { get; set; } = 600;

    /// <summary>Бюджет ожидания слота на догон LSN при cutover (t01, spec §9).</summary>
    public int CutoverTimeoutSec { get; set; } = 90;

    /// <summary>Бюджет недоступности шарда в ожиданиях переезда (t01, spec §9).</summary>
    public int ConnFailBudgetSec { get; set; } = 120;
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
