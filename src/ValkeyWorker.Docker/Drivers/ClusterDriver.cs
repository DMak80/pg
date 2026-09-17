using Shared.Core.Planning;
using Shared.Core;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Docker.Engine;

namespace ValkeyWorker.Docker.Drivers;

// Хост plain-режима: имя + endpoint Engine API (tcp://… | unix://…).
public sealed record HostEndpoint(string Name, string Endpoint);

/// <summary>
/// Спецификация ноды для docker-драйвера: Args готовит NodeArgsBuilder
/// (детерминирован от декларации/кредов etcd — arch/21 §4.5.1); драйвер только
/// размещает (хост, host-порт публикации 6379, лимиты; без volume, без
/// per-cluster сети — arch/21 §2).
/// </summary>
public sealed record ValkeyNodeSpec(
    string Cluster,
    string NodeName,
    string Host,
    int ClientHostPort,
    string Image,
    IReadOnlyList<string> Args,
    decimal? CpuCores,
    long? MemoryBytes);

// Инспекция размещения ноды (E9-реконструкция portalloc / надзор C): Host —
// хост размещения, ClientHostPort — published host-порт ноды (6379), Running —
// факт docker-инспекта (false = контейнер/таск есть, но остановлен: отказ
// пробы надзора — молчание ноды, не слепота воркера).
public sealed record NodeEndpointInspection(string Host, int ClientHostPort, bool Running = true);

// Унифицированное управление нодой в обоих режимах (порт драйверов kfw с
// упрощениями домена — arch/21 §2): объекты — контейнер/сервис
// vwk-<C>-node<k>; томов нет, per-cluster сетей нет.
// Идемпотентность: существующий объект сверяется по имени и не пересоздаётся
// (решение о сверке/замене — у процессов V3/надзора); 404 на удалении /
// 409 на создании — успех (движок).
public interface IClusterDriver
{
    // Живые хосты для PlacementPlanner: plain — конфиг (UsedSlots по числу
    // контейнеров vwk-*), swarm — ListNodes (running tasks).
    Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct);

    // Занятые host:port (для PortAllocator).
    Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct);

    // Идемпотентно создать ноду (plain: контейнер vwk-<C>-node<k>; swarm:
    // сервис с constraint node.id==<id>, publish mode=host).
    Task<Result> EnsureNodeAsync(ValkeyNodeSpec spec, CancellationToken ct);

    // Остановить и удалить ноду (томов нет — removeVolume не существует; 404 = успех).
    Task<Result> RemoveNodeAsync(string cluster, string nodeName, CancellationToken ct);

    // Фактические лимиты контейнера/сервиса ноды (автоконверге C): null =
    // объекта нет; ошибка инспекта → Failed (надзор не решает вслепую).
    Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct);

    // Инспекция размещения ноды для E9-реконструкции portalloc:
    // null = docker-объекта нет — положительное свидетельство смерти (S7);
    // ошибка инспекта → Failed — надзор не решает вслепую.
    Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
        string cluster, string nodeName, CancellationToken ct);

    // Cmd (args) живой ноды — сверка V3 (image+args+порт+лимиты): plain —
    // перебор хостов InspectContainerCmdAsync(vwk-<C>-node<k>), swarm —
    // InspectServiceCmdAsync; null = объекта нет.
    Task<Result<IReadOnlyList<string>?>> NodeArgsAsync(string cluster, string nodeName, CancellationToken ct);

    // Имена объектов нод кластера (vwk-<C>-*): сверка декларации + сироты (X1).
    Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct);
}

// Plain-режим: контейнеры на перечисленных хостах, per-host Engine API.
public sealed class PlainClusterDriver(
    IReadOnlyList<HostEndpoint> hosts,
    DockerEngineFactory factory) : IClusterDriver
{
    // Контейнерный порт ноды valkey → выделенный host-порт (arch/21 §2).
    public const int ClientContainerPort = 6379;

    private readonly Dictionary<string, IDockerEngine> _engines = hosts.ToDictionary(
        h => h.Name,
        h => factory.Create(h.Endpoint, hostAlias: h.Name));

    public async Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct)
    {
        return await Result<IReadOnlyList<HostInfo>>.FromAsync(async () =>
        {
            var result = new List<HostInfo>();
            foreach (var (name, engine) in _engines)
            {
                var containers = await engine.ListContainersAsync("vwk-", all: true, ct);
                if (!containers.IsSuccess)
                    throw containers.Error!; // один хост недоступен — не тихий список
                result.Add(new HostInfo(name, containers.Value.Count));
            }

            return (IReadOnlyList<HostInfo>)result;
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
                    busy.Add(pair);
            }

            return (IReadOnlySet<(string, int)>)busy;
        });
    }

    public async Task<Result> EnsureNodeAsync(ValkeyNodeSpec spec, CancellationToken ct)
    {
        if (!_engines.TryGetValue(spec.Host, out var engine))
            return Result.Failed(new ApplicationException(
                $"хост {spec.Host} не в таблице Docker:Hosts (кластер {spec.Cluster}/{spec.NodeName})"));

        return await Result.FromAsync(async () =>
        {
            var name = NodeName(spec.Cluster, spec.NodeName);

            // Идемпотентность: существующий контейнер не пересоздаётся (V3);
            // сверка/замена — решение процессов (spec V3/надзор C).
            var existing = await engine.ListContainersAsync(name, all: true, ct);
            if (!existing.IsSuccess)
                throw existing.Error!;
            if (existing.Value.Any(c => c.Names.Contains(name)))
                return;

            var containerSpec = new ContainerSpec(
                spec.Image,
                spec.Args,
                [new PortMap(ClientContainerPort, spec.ClientHostPort)],
                spec.NodeName,
                CpuCores: (double?)spec.CpuCores,
                MemoryBytes: spec.MemoryBytes,
                Label: spec.Cluster);

            var created = await engine.CreateContainerAsync(containerSpec, name, ct);
            if (!created.IsSuccess)
                throw created.Error!;
            var started = await engine.StartContainerAsync(name, ct);
            if (!started.IsSuccess)
                throw started.Error!;
        });
    }

    public async Task<Result> RemoveNodeAsync(string cluster, string nodeName, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            var name = NodeName(cluster, nodeName);
            foreach (var engine in _engines.Values)
            {
                // 404 на каждом шаге — успех (движок); томов у домена нет.
                var stopped = await engine.StopContainerAsync(name, timeoutSec: 10, ct);
                if (!stopped.IsSuccess)
                    throw stopped.Error!;
                var removed = await engine.RemoveContainerAsync(name, force: true, ct);
                if (!removed.IsSuccess)
                    throw removed.Error!;
            }
        });
    }

    public async Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
    {
        return await Result<IReadOnlyList<string>>.FromAsync(async () =>
        {
            var names = new List<string>();
            var prefix = $"vwk-{cluster}-";
            foreach (var engine in _engines.Values)
            {
                var containers = await engine.ListContainersAsync(prefix, all: true, ct);
                if (!containers.IsSuccess)
                    throw containers.Error!;
                names.AddRange(containers.Value.SelectMany(c => c.Names)
                    .Where(n => n.StartsWith(prefix, StringComparison.Ordinal)));
            }

            return (IReadOnlyList<string>)names.Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList();
        });
    }

    // Перебор хостов: первый хост с контейнером отдаёт факт (симметрия args).
    public async Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct)
    {
        var name = NodeName(cluster, nodeName);
        foreach (var engine in _engines.Values)
        {
            var limits = await engine.InspectContainerResourcesAsync(name, ct);
            if (!limits.IsSuccess)
                return limits;
            if (limits.Value is not null)
                return limits;
        }

        return Result<NodeLimits?>.Success(null);
    }

    // Args живой ноды (сверка V3): перебор хостов, первый найденный.
    public async Task<Result<IReadOnlyList<string>?>> NodeArgsAsync(
        string cluster, string nodeName, CancellationToken ct)
    {
        var name = NodeName(cluster, nodeName);
        foreach (var engine in _engines.Values)
        {
            var cmd = await engine.InspectContainerCmdAsync(name, ct);
            if (!cmd.IsSuccess)
                return cmd;
            if (cmd.Value is not null)
                return cmd;
        }

        return Result<IReadOnlyList<string>?>.Success(null);
    }

    // E9-реконструкция: перебор хостов — первый, где контейнер есть,
    // отдаёт host-порт + host-алиас этого движка + флаг running.
    public async Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
        string cluster, string nodeName, CancellationToken ct)
    {
        var name = NodeName(cluster, nodeName);
        foreach (var (host, engine) in _engines)
        {
            var endpoint = await engine.InspectNodeEndpointAsync(name, ct);
            if (!endpoint.IsSuccess)
                return Result<NodeEndpointInspection?>.Failed(endpoint.Error!);
            if (endpoint.Value is { } found)
                return Result<NodeEndpointInspection?>.Success(
                    new NodeEndpointInspection(host, found.ClientHostPort, found.Running));
        }

        return Result<NodeEndpointInspection?>.Success(null);
    }

    internal static string NodeName(string cluster, string nodeName)
        => $"vwk-{cluster}-{nodeName}";
}

// Swarm-режим: сервисы через manager endpoint, replicas=1, constraint node.id==<id>.
public sealed class SwarmClusterDriver(
    string managerEndpoint,
    DockerEngineFactory factory) : IClusterDriver
{
    private readonly IDockerEngine _engine = factory.Create(managerEndpoint, hostAlias: null);

    public async Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct)
    {
        return await Result<IReadOnlyList<HostInfo>>.FromAsync(async () =>
        {
            var nodes = await _engine.ListNodesAsync(ct);
            if (!nodes.IsSuccess)
                throw nodes.Error!;
            return (IReadOnlyList<HostInfo>)nodes.Value
                .Where(n => n.State == "ready") // недоступные swarm-ноды не участвуют в placement
                .Select(n => new HostInfo(n.Hostname, n.RunningTasks))
                .ToList();
        });
    }

    public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct)
        => _engine.BusyPortsAsync(ct);

    public async Task<Result> EnsureNodeAsync(ValkeyNodeSpec spec, CancellationToken ct)
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

            var template = new ContainerSpec(
                spec.Image,
                spec.Args,
                [new PortMap(PlainClusterDriver.ClientContainerPort, spec.ClientHostPort)],
                spec.NodeName,
                CpuCores: (double?)spec.CpuCores,
                MemoryBytes: spec.MemoryBytes,
                Label: spec.Cluster);
            var serviceSpec = new ServiceSpec(
                PlainClusterDriver.NodeName(spec.Cluster, spec.NodeName),
                template,
                target.Id);

            // Идемпотентность: 409 already-exists — успех (движок).
            var created = await _engine.CreateServiceAsync(serviceSpec, ct);
            if (!created.IsSuccess)
                throw created.Error!;
        });
    }

    public Task<Result> RemoveNodeAsync(string cluster, string nodeName, CancellationToken ct)
        => _engine.RemoveServiceAsync(PlainClusterDriver.NodeName(cluster, nodeName), ct);

    public Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct)
        => _engine.InspectServiceResourcesAsync(PlainClusterDriver.NodeName(cluster, nodeName), ct);

    // Args сервиса ноды (сверка V3): Spec.TaskTemplate.ContainerSpec.Cmd.
    public Task<Result<IReadOnlyList<string>?>> NodeArgsAsync(
        string cluster, string nodeName, CancellationToken ct)
        => _engine.InspectServiceCmdAsync(PlainClusterDriver.NodeName(cluster, nodeName), ct);

    // E9-реконструкция: published-порт и хост таска отдаёт одна инспекция
    // движка (swarm-фолбэк по running-таску — один HTTP-раунд ListTasks);
    // running-таск = Running (останавливаться у сервиса — только снятием).
    public async Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
        string cluster, string nodeName, CancellationToken ct)
    {
        var name = PlainClusterDriver.NodeName(cluster, nodeName);
        var endpoint = await _engine.InspectNodeEndpointAsync(name, ct);
        if (!endpoint.IsSuccess)
            return Result<NodeEndpointInspection?>.Failed(endpoint.Error!);
        if (endpoint.Value is not { } found)
            return Result<NodeEndpointInspection?>.Success(null);

        return found.TaskHost is { } host
            ? Result<NodeEndpointInspection?>.Success(
                new NodeEndpointInspection(host, found.ClientHostPort, found.Running))
            : Result<NodeEndpointInspection?>.Success(null); // хоста таска нет — факта нет
    }

    // Объекты нод кластера в swarm — СЕРВИСЫ: GET /services с префиксом
    // vwk-<C>-. Существование сервиса ≠ живой таск: живость — PING-пробы.
    public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
        => _engine.ListServicesAsync($"vwk-{cluster}-", ct);
}
