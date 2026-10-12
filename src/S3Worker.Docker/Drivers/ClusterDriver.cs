namespace S3Worker.Docker.Drivers;

// Хост plain-режима: имя + endpoint Engine API (tcp://… | unix://…).
public sealed record HostEndpoint(string Name, string Endpoint);

/// <summary>
/// Спецификация ноды для docker-драйвера: env — секреты/конфиг ownS3
/// (OWNS3_*, arch/23 §6); драйвер только размещает (хост, host-порт
/// публикации 9000, том данных, лимиты; per-cluster сетей нет — arch/24 §2).
/// </summary>
public sealed record S3NodeSpec(
    string Cluster,
    string NodeName,
    string Host,
    int S3HostPort,
    string Image,
    IReadOnlyDictionary<string, string> Env,
    decimal? CpuCores,
    long? MemoryBytes);

// Инспекция размещения ноды (E9-реконструкция portalloc / надзор C): Host —
// хост размещения, S3HostPort — published host-порт ноды (9000), Running —
// факт docker-инспекта (false = контейнер есть, но остановлен: молчание ноды,
// не слепота воркера).
public sealed record S3NodeInspection(string Host, int S3HostPort, bool Running = true);

// Атрибуты хоста для PlacementResolver (arch/23 §4): plain — Name из Hosts[],
// Labels пуст (plain-селекторы — только docker_host); swarm — Name=hostname,
// Labels=node.labels.*, UsedSlots=running tasks.
public sealed record S3HostAttributes(string Name, int UsedSlots, IReadOnlyDictionary<string, string> Labels);

// Унифицированное управление нодой в обоих режимах (порт драйверов valkey с
// упрощениями домена — arch/23 §2/§7): объекты — контейнер/сервис
// s3w-<C>-node<k> с томом данных s3w-<C>-node<k>-data (/data); пер-кластерных
// сетей нет (arch/24 §2). Идемпотентность: существующий объект сверяется по
// имени и не пересоздаётся (решение о сверке/замене — у процессов);
// 404 на удалении / 409 на создании — успех (движок).
public interface IS3ClusterDriver
{
    // Контейнерный порт S3-грани ownS3 (arch/23 §2.1: published 9000).
    int S3ContainerPort { get; }

    // Живые хосты для PlacementResolver: plain — конфиг (UsedSlots по числу
    // контейнеров s3w-*), swarm — ListNodes (ready, running tasks, labels).
    Task<Result<IReadOnlyList<S3HostAttributes>>> GetHostsAsync(CancellationToken ct);

    // Занятые host:port (для PortAllocator).
    Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct);

    // Идемпотентно создать ноду: том данных → контейнер/сервис
    // (contourNetwork — сеть стенда/E2E, null = сеть по умолчанию).
    Task<Result> EnsureNodeAsync(S3NodeSpec spec, string? contourNetwork, CancellationToken ct);

    // Остановить и удалить ноду (removeVolume=false — данные сохраняются:
    // пересоздание надзора/ротация не теряют объекты bucket'а).
    Task<Result> RemoveNodeAsync(string cluster, string node, bool removeVolume, CancellationToken ct);

    // Фактические лимиты контейнера/сервиса ноды (автоконверге C): null =
    // объекта нет; ошибка инспекта → Failed (надзор не решает вслепую).
    Task<Result<Shared.Docker.NodeLimits?>> NodeResourcesAsync(string cluster, string node, CancellationToken ct);

    // Инспекция размещения ноды для E9-реконструкции portalloc: null =
    // docker-объекта нет — положительное свидетельство смерти (S7);
    // ошибка инспекта → Failed — надзор не решает вслепую.
    Task<Result<S3NodeInspection?>> InspectNodeAsync(string cluster, string node, CancellationToken ct);

    // Env живой ноды — источник секрета факта (сверка ротации R): plain —
    // перебор хостов InspectContainerEnvAsync, swarm — InspectServiceEnvAsync;
    // null = объекта нет.
    Task<Result<IReadOnlyDictionary<string, string>?>> NodeEnvAsync(string cluster, string node, CancellationToken ct);

    // Имена объектов нод кластера (s3w-<C>-*: контейнеры/сервисы + тома
    // -data): сверка декларации + сироты (X2).
    Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct);
}

// Plain-режим: контейнеры на перечисленных хостах, per-host Engine API.
public sealed class PlainClusterDriver(
    IReadOnlyList<HostEndpoint> hosts,
    DockerEngineFactory factory) : IS3ClusterDriver
{
    // Контейнерный порт S3-грани ownS3 → выделенный host-порт (arch/23 §2.1).
    public const int S3ContainerPortConst = 9000;

    // Label-ключ контейнеров s3w-домена (arch/23 §2: label "s3worker").
    internal const string LabelKey = "s3worker";

    // Число нод кластера ограничено 1..9 (arch/23 §2) — перебор детерминированных
    // имён томов в ListNodeObjectsAsync.
    private const int MaxNodes = 9;

    public int S3ContainerPort => S3ContainerPortConst;

    private readonly Dictionary<string, IDockerEngine> _engines = hosts.ToDictionary(
        h => h.Name,
        h => factory.Create(h.Endpoint, hostAlias: h.Name));

    public async Task<Result<IReadOnlyList<S3HostAttributes>>> GetHostsAsync(CancellationToken ct)
    {
        return await Result<IReadOnlyList<S3HostAttributes>>.FromAsync(async () =>
        {
            var result = new List<S3HostAttributes>();
            foreach (var (name, engine) in _engines)
            {
                var containers = await engine.ListContainersAsync("s3w-", all: true, ct);
                if (!containers.IsSuccess)
                    throw containers.Error!; // один хост недоступен — не тихий список
                result.Add(new S3HostAttributes(name, containers.Value.Count, EmptyLabels));
            }

            return (IReadOnlyList<S3HostAttributes>)result;
        });
    }

    public async Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct)
    {
        return await Result<IReadOnlySet<(string Host, int Port)>>.FromAsync(async () =>
        {
            var busy = new HashSet<(string, int)>();
            foreach (var engine in _engines.Values)
            {
                var ports = await engine.BusyPortsAsync(ct);
                if (!ports.IsSuccess)
                    throw ports.Error!;
                foreach (var pair in ports.Value)
                    busy.Add(pair); // pair.Host — host-алиас движка
            }

            return (IReadOnlySet<(string, int)>)busy;
        });
    }

    public async Task<Result> EnsureNodeAsync(S3NodeSpec spec, string? contourNetwork, CancellationToken ct)
    {
        if (!_engines.TryGetValue(spec.Host, out var engine))
            return Result.Failed(new ApplicationException(
                $"хост {spec.Host} не в таблице Docker:Hosts (кластер {spec.Cluster}/{spec.NodeName})"));

        return await Result.FromAsync(async () =>
        {
            var name = NodeName(spec.Cluster, spec.NodeName);
            var volumeName = VolumeName(spec.Cluster, spec.NodeName);

            // Идемпотентность: существующий контейнер не пересоздаётся (надзор C /
            // ротация R сверяют env/лимиты отдельно и меняют через remove+ensure).
            var existing = await engine.ListContainersAsync(name, all: true, ct);
            if (!existing.IsSuccess)
                throw existing.Error!;
            if (existing.Value.Any(c => c.Names.Contains(name)))
                return;

            // Том данных: данные объектов bucket'а переживают пересоздание ноды.
            var volume = await engine.EnsureVolumeAsync(volumeName, ct);
            if (!volume.IsSuccess)
                throw volume.Error!;

            var containerSpec = new ContainerSpec(
                spec.Image,
                [new PortMap(S3ContainerPortConst, spec.S3HostPort)],
                spec.NodeName,
                Env: spec.Env,
                VolumeName: volumeName,
                VolumeDest: "/data",
                Network: contourNetwork,
                CpuCores: (double?)spec.CpuCores,
                MemoryBytes: spec.MemoryBytes,
                RestartPolicy: "unless-stopped",
                LabelKey: LabelKey,
                Label: spec.Cluster);

            var created = await engine.CreateContainerAsync(containerSpec, name, ct);
            if (!created.IsSuccess)
                throw created.Error!;
            var started = await engine.StartContainerAsync(name, ct);
            if (!started.IsSuccess)
                throw started.Error!;
        });
    }

    public async Task<Result> RemoveNodeAsync(string cluster, string node, bool removeVolume, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            var name = NodeName(cluster, node);
            foreach (var engine in _engines.Values)
            {
                // Нода одна (hostname/имя уникальны по кластеру): убираем на том
                // хосте, где объект найден; у остальных — 404 = успех.
                var stopped = await engine.StopContainerAsync(name, timeoutSec: 10, ct);
                if (!stopped.IsSuccess)
                    throw stopped.Error!;
                var removed = await engine.RemoveContainerAsync(name, force: true, ct);
                if (!removed.IsSuccess)
                    throw removed.Error!;
            }

            if (!removeVolume)
                return;

            foreach (var engine in _engines.Values)
            {
                var volume = await engine.DeleteVolumeAsync(VolumeName(cluster, node), ct);
                if (!volume.IsSuccess)
                    throw volume.Error!;
            }
        });
    }

    public async Task<Result<Shared.Docker.NodeLimits?>> NodeResourcesAsync(string cluster, string node, CancellationToken ct)
    {
        var name = NodeName(cluster, node);
        foreach (var engine in _engines.Values)
        {
            var limits = await engine.InspectContainerResourcesAsync(name, ct);
            if (!limits.IsSuccess)
                return Result<Shared.Docker.NodeLimits?>.Failed(limits.Error!);
            if (limits.Value is not null)
                return Result<Shared.Docker.NodeLimits?>.Success(limits.Value);
        }

        return Result<Shared.Docker.NodeLimits?>.Success(null);
    }

    public async Task<Result<S3NodeInspection?>> InspectNodeAsync(string cluster, string node, CancellationToken ct)
    {
        var name = NodeName(cluster, node);
        foreach (var (host, engine) in _engines)
        {
            var endpoint = await engine.InspectNodeEndpointAsync(name, S3ContainerPortConst, ct);
            if (!endpoint.IsSuccess)
                return Result<S3NodeInspection?>.Failed(endpoint.Error!);
            if (endpoint.Value is { } found)
                return Result<S3NodeInspection?>.Success(
                    new S3NodeInspection(host, found.ClientHostPort, found.Running));
        }

        return Result<S3NodeInspection?>.Success(null);
    }

    public async Task<Result<IReadOnlyDictionary<string, string>?>> NodeEnvAsync(
        string cluster, string node, CancellationToken ct)
    {
        var name = NodeName(cluster, node);
        foreach (var engine in _engines.Values)
        {
            var env = await engine.InspectContainerEnvAsync(name, ct);
            if (!env.IsSuccess)
                return env;
            if (env.Value is not null)
                return env;
        }

        return Result<IReadOnlyDictionary<string, string>?>.Success(null);
    }

    public async Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
    {
        return await Result<IReadOnlyList<string>>.FromAsync(async () =>
        {
            var names = new List<string>();
            var prefix = $"s3w-{cluster}-";
            foreach (var engine in _engines.Values)
            {
                var containers = await engine.ListContainersAsync(prefix, all: true, ct);
                if (!containers.IsSuccess)
                    throw containers.Error!;
                names.AddRange(containers.Value.SelectMany(c => c.Names)
                    .Where(n => n.StartsWith(prefix, StringComparison.Ordinal)));
            }

            // Тома данных: детерминированные имена node1..node9 (предел arch/23 §2);
            // VolumeExistsAsync дешевле листинга, которого движок не несёт.
            foreach (var (node, engine) in NodeEnginesByVolume())
            {
                var exists = await engine.VolumeExistsAsync(VolumeName(cluster, node), ct);
                if (!exists.IsSuccess)
                    throw exists.Error!;
                if (exists.Value)
                    names.Add(VolumeName(cluster, node));
            }

            return (IReadOnlyList<string>)names.Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList();
        });
    }

    // Том нод-локален, но размещение plain-таблица не отслеживает: сирота-том
    // (утраченный контейнер) может остаться на ЛЮБОМ хосте — существование
    // проверяется на всех движках (объединение инспекций); имена томов
    // детерминированы node1..node9 (предел arch/23 §2).
    private IEnumerable<(string Node, IDockerEngine Engine)> NodeEnginesByVolume()
    {
        foreach (var engine in _engines.Values)
            for (var k = 1; k <= MaxNodes; k++)
                yield return ($"node{k}", engine);
    }

    internal static string NodeName(string cluster, string nodeName)
        => $"s3w-{cluster}-{nodeName}";

    internal static string VolumeName(string cluster, string nodeName)
        => $"s3w-{cluster}-{nodeName}-data";

    private static readonly IReadOnlyDictionary<string, string> EmptyLabels =
        new Dictionary<string, string>();
}

// Swarm-режим: сервисы через manager endpoint, replicas=1, constraint node.id==<id>
// (порт SwarmClusterDriver valkey). hosts — endpoint'ы Engine API swarm-нод
// (по имени ноды размещения): том данных нод-локален — снос объема идёт по
// таблице + manager.
public sealed class SwarmClusterDriver(
    string managerEndpoint,
    DockerEngineFactory factory,
    IReadOnlyList<HostEndpoint>? hosts = null) : IS3ClusterDriver
{
    private readonly IDockerEngine _engine = factory.Create(managerEndpoint, hostAlias: null);

    // Engines нод из таблицы (по имени ноды размещения — как EnsureNodeAsync
    // матчит spec.Host по Hostname ноды).
    private readonly Dictionary<string, IDockerEngine> _nodeEngines = (hosts ?? [])
        .ToDictionary(h => h.Name, h => factory.Create(h.Endpoint, hostAlias: h.Name));

    public int S3ContainerPort => PlainClusterDriver.S3ContainerPortConst;

    public async Task<Result<IReadOnlyList<S3HostAttributes>>> GetHostsAsync(CancellationToken ct)
    {
        return await Result<IReadOnlyList<S3HostAttributes>>.FromAsync(async () =>
        {
            var nodes = await _engine.ListNodesAsync(ct);
            if (!nodes.IsSuccess)
                throw nodes.Error!;
            return (IReadOnlyList<S3HostAttributes>)nodes.Value
                .Where(n => n.State == "ready") // недоступные swarm-ноды не участвуют в placement
                .Select(n => new S3HostAttributes(n.Hostname, n.RunningTasks, n.Labels ?? EmptyLabels))
                .ToList();
        });
    }

    public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct)
        => _engine.BusyPortsAsync(ct);

    public async Task<Result> EnsureNodeAsync(S3NodeSpec spec, string? contourNetwork, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            // constraint: node.id==<id>, id ищем по Hostname==spec.Host.
            var nodes = await _engine.ListNodesAsync(ct);
            if (!nodes.IsSuccess)
                throw nodes.Error!;
            var target = nodes.Value.FirstOrDefault(n => n.Hostname == spec.Host);
            if (target is null)
                throw new ApplicationException($"swarm-нода с Hostname={spec.Host} не найдена");

            // Идемпотентность: 409 already-exists — успех (движок); сверка/замена —
            // решение процессов (надзор C/ротация R).
            var volume = await _engine.EnsureVolumeAsync(PlainClusterDriver.VolumeName(spec.Cluster, spec.NodeName), ct);
            if (!volume.IsSuccess)
                throw volume.Error!;

            var template = new ContainerSpec(
                spec.Image,
                [new PortMap(PlainClusterDriver.S3ContainerPortConst, spec.S3HostPort)],
                spec.NodeName,
                Env: spec.Env,
                VolumeName: PlainClusterDriver.VolumeName(spec.Cluster, spec.NodeName),
                VolumeDest: "/data",
                CpuCores: (double?)spec.CpuCores,
                MemoryBytes: spec.MemoryBytes,
                LabelKey: PlainClusterDriver.LabelKey,
                Label: spec.Cluster);
            var serviceSpec = new ServiceSpec(
                PlainClusterDriver.NodeName(spec.Cluster, spec.NodeName),
                template,
                target.Id);

            var created = await _engine.CreateServiceAsync(serviceSpec, ct);
            if (!created.IsSuccess)
                throw created.Error!;
        });
    }

    public async Task<Result> RemoveNodeAsync(string cluster, string node, bool removeVolume, CancellationToken ct)
    {
        var removed = await _engine.RemoveServiceAsync(PlainClusterDriver.NodeName(cluster, node), ct);
        if (!removed.IsSuccess)
            return removed;

        if (!removeVolume)
            return Result.Success();

        foreach (var engine in _nodeEngines.Values.Append(_engine))
        {
            var volume = await engine.DeleteVolumeAsync(PlainClusterDriver.VolumeName(cluster, node), ct);
            if (!volume.IsSuccess)
                return volume;
        }

        return Result.Success();
    }

    public async Task<Result<Shared.Docker.NodeLimits?>> NodeResourcesAsync(string cluster, string node, CancellationToken ct)
    {
        var limits = await _engine.InspectServiceResourcesAsync(PlainClusterDriver.NodeName(cluster, node), ct);
        if (!limits.IsSuccess)
            return Result<Shared.Docker.NodeLimits?>.Failed(limits.Error!);
        return Result<Shared.Docker.NodeLimits?>.Success(limits.Value);
    }

    public async Task<Result<S3NodeInspection?>> InspectNodeAsync(string cluster, string node, CancellationToken ct)
    {
        var name = PlainClusterDriver.NodeName(cluster, node);
        var endpoint = await _engine.InspectNodeEndpointAsync(name, PlainClusterDriver.S3ContainerPortConst, ct);
        if (!endpoint.IsSuccess)
            return Result<S3NodeInspection?>.Failed(endpoint.Error!);
        if (endpoint.Value is not { } found)
            return Result<S3NodeInspection?>.Success(null);

        return found.TaskHost is { } host
            ? Result<S3NodeInspection?>.Success(
                new S3NodeInspection(host, found.ClientHostPort, found.Running))
            : Result<S3NodeInspection?>.Success(null); // хоста таска нет — факта нет
    }

    public Task<Result<IReadOnlyDictionary<string, string>?>> NodeEnvAsync(
        string cluster, string node, CancellationToken ct)
        => _engine.InspectServiceEnvAsync(PlainClusterDriver.NodeName(cluster, node), ct);

    // Объекты нод кластера в swarm — СЕРВИСЫ + тома данных (предел 1..9 нод).
    public async Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
    {
        var services = await _engine.ListServicesAsync($"s3w-{cluster}-", ct);
        if (!services.IsSuccess)
            return services;

        var names = new List<string>(services.Value);
        foreach (var (node, engine) in NodeEnginesByVolume())
        {
            var exists = await engine.VolumeExistsAsync(PlainClusterDriver.VolumeName(cluster, node), ct);
            if (!exists.IsSuccess)
                return Result<IReadOnlyList<string>>.Failed(exists.Error!);
            if (exists.Value)
                names.Add(PlainClusterDriver.VolumeName(cluster, node));
        }

        return Result<IReadOnlyList<string>>.Success(
            names.Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList());
    }

    // Том нод-локален, сирота-том (утраченный контейнер) может остаться на
    // любой swarm-ноде или менеджере — существование проверяется на всех
    // движках (объединение инспекций, как в RemoveNodeAsync); имена детерминированы.
    private IEnumerable<(string Node, IDockerEngine Engine)> NodeEnginesByVolume()
    {
        foreach (var engine in _nodeEngines.Values.Append(_engine))
            for (var k = 1; k <= 9; k++)
                yield return ($"node{k}", engine);
    }

    private static readonly IReadOnlyDictionary<string, string> EmptyLabels =
        new Dictionary<string, string>();
}
