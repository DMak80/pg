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

/// <summary>Пороги надзора: rebuild ноды / эвакуация шарда / бюджет ожидания Patroni.</summary>
public sealed class ThresholdsOptions
{
    public int NodeDeadSec { get; set; } = 90;

    public int ShardDeadSec { get; set; } = 300;

    public int PatroniBootSec { get; set; } = 600;
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
}
