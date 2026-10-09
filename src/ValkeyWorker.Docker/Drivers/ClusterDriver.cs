using Shared.Core.Planning;
using Shared.Core;
using ValkeyWorker.Core.Model;
// t07: движок возвращает docker-факт Shared.Docker.NodeLimits(long,long);
// доменная запись NodeLimits(decimal? ядра, long?) остаётся в ValkeyWorker.Core
// — конверсия у драйвера (Convert). Алиасы снимают коллизию имён.
using NodeLimits = ValkeyWorker.Core.Model.NodeLimits;
using DockerLimits = Shared.Docker.NodeLimits;

namespace ValkeyWorker.Docker.Drivers;

// Хост plain-режима: имя + endpoint Engine API (tcp://… | unix://…).
public sealed record HostEndpoint(string Name, string Endpoint);

/// <summary>
/// Спецификация ноды для docker-драйвера: Args = cmd-обёртка
/// NodeArgsBuilder.BuildCmd (детерминирована от декларации/кредов etcd —
/// arch/21 §2), Env — TLS-материал VALKEY_TLS_{CERT,KEY,CA} (свежий серт
/// ноды; обёртка раскатывает PEM в /tls при старте); драйвер только
/// размещает (хост, host-порт публикации 6379, лимиты; томов нет,
/// per-cluster сети нет — arch/21 §2).
/// </summary>
public sealed record ValkeyNodeSpec(
    string Cluster,
    string NodeName,
    string Host,
    int ClientHostPort,
    string Image,
    IReadOnlyList<string> Args,
    decimal? CpuCores,
    long? MemoryBytes,
    IReadOnlyDictionary<string, string>? Env = null);

// Инспекция размещения ноды (E9-реконструкция portalloc / надзор C): Host —
// хост размещения, ClientHostPort — published host-порт ноды (6379), Running —
// факт docker-инспекта (false = контейнер/таск есть, но остановлен: отказ
// пробы надзора — молчание ноды, не слепота воркера).
public sealed record NodeEndpointInspection(string Host, int ClientHostPort, bool Running = true);

// Унифицированное управление нодой в обоих режимах (порт драйверов kfw с
// упрощениями домена — arch/21 §2): объекты — контейнер/сервис
// vwk-<C>-node<k>; TLS-материал — env контейнера, томов и per-cluster сетей
// нет (легаси-том vwk-<C>-tls старой volume-модели убирает
// CleanupLegacyVolumeAsync — утилита миграции надзора).
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

    // Остановить и удалить ноду (томов нет; 404 = успех).
    Task<Result> RemoveNodeAsync(string cluster, string nodeName, CancellationToken ct);

    // Фактические лимиты контейнера/сервиса ноды (автоконверге C): null =
    // объекта нет; ошибка инспекта → Failed (надзор не решает вслепую).
    Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct);

    // Инспекция размещения ноды для E9-реконструкции portalloc:
    // null = docker-объекта нет — положительное свидетельство смерти (S7);
    // ошибка инспекта → Failed — надзор не решает вслепую.
    Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
        string cluster, string nodeName, CancellationToken ct);

    // Cmd (обёртка) живой ноды — сверка V3 (image+cmd+порт+лимиты): plain —
    // перебор хостов InspectContainerCmdAsync(vwk-<C>-node<k>), swarm —
    // InspectServiceCmdAsync; null = объекта нет.
    Task<Result<IReadOnlyList<string>?>> NodeArgsAsync(string cluster, string nodeName, CancellationToken ct);

    // Env живой ноды — источник TLS-факта (сверки V3/надзора/фазы R):
    // plain — перебор хостов InspectContainerEnvAsync, swarm —
    // InspectServiceEnvAsync; null = объекта нет.
    Task<Result<IReadOnlyDictionary<string, string>?>> NodeEnvAsync(
        string cluster, string nodeName, CancellationToken ct);

    // Имена объектов нод кластера (vwk-<C>-*): сверка декларации + сироты (X1).
    Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct);

    // Легаси-чистка миграции (надзор C): удаление осиротевшего тома
    // vwk-<C>-tls старой volume-модели на ВСЕХ engines (plain — таблица
    // хостов; swarm — ноды таблицы + manager); 404 = успех (идемпотентность
    // движка); 409 volume-in-use → Failed — ретрай следующим тиком надзора
    // (тик надзора этим НЕ фейлится, warnings не пишет — вызов безусловный).
    Task<Result> CleanupLegacyVolumeAsync(string cluster, CancellationToken ct);
}

// Plain-режим: контейнеры на перечисленных хостах, per-host Engine API.
public sealed class PlainClusterDriver(
    IReadOnlyList<HostEndpoint> hosts,
    DockerEngineFactory factory) : IClusterDriver
{
    // Контейнерный порт ноды valkey → выделенный host-порт (arch/21 §2).
    public const int ClientContainerPort = 6379;

    // Label-ключ контейнеров/сервисов vwk-домена (t07: наследие-литерал
    // "pgworker" заменён доменным; читателей label в коде нет).
    internal const string LabelKey = "valkeyworker";

    // Конверсия docker-факта лимитов в доменную запись (t07 §4.6.2):
    // 0 = без лимита → null — семантика значений прежнего vwk-движка.
    internal static NodeLimits Convert(DockerLimits limits)
        => new(
            limits.NanoCpus > 0 ? limits.NanoCpus / 1_000_000_000m : null,
            limits.MemoryBytes > 0 ? limits.MemoryBytes : null);

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
                [new PortMap(ClientContainerPort, spec.ClientHostPort)],
                spec.NodeName,
                // Cmd — обёртка env-TLS: раскатка PEM из env в /tls + exec
                // valkey-server (аргументы образного docker-entrypoint.sh,
                // ResetEntrypoint=false по умолчанию).
                Cmd: spec.Args,
                CpuCores: (double?)spec.CpuCores,
                MemoryBytes: spec.MemoryBytes,
                LabelKey: LabelKey,
                Label: spec.Cluster,
                // TLS-материал — env контейнера (VALKEY_TLS_{CERT,KEY,CA}).
                Env: spec.Env);

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
    // Движок возвращает docker-факт (NanoCpus/Memory) — конверсия в доменные
    // decimal?-ядра у драйвера (t07 §4.6.2).
    public async Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct)
    {
        var name = NodeName(cluster, nodeName);
        foreach (var engine in _engines.Values)
        {
            var limits = await engine.InspectContainerResourcesAsync(name, ct);
            if (!limits.IsSuccess)
                return Result<NodeLimits?>.Failed(limits.Error!);
            if (limits.Value is not null)
                return Result<NodeLimits?>.Success(Convert(limits.Value));
        }

        return Result<NodeLimits?>.Success(null);
    }

    // Cmd живой ноды (сверка V3): перебор хостов, первый найденный.
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

    // Env живой ноды (сверки V3/надзора/фазы R): перебор хостов, первый найденный.
    public async Task<Result<IReadOnlyDictionary<string, string>?>> NodeEnvAsync(
        string cluster, string nodeName, CancellationToken ct)
    {
        var name = NodeName(cluster, nodeName);
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

    // E9-реконструкция: перебор хостов — первый, где контейнер есть,
    // отдаёт host-порт + host-алиас этого движка + флаг running.
    public async Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
        string cluster, string nodeName, CancellationToken ct)
    {
        var name = NodeName(cluster, nodeName);
        foreach (var (host, engine) in _engines)
        {
            var endpoint = await engine.InspectNodeEndpointAsync(name, ClientContainerPort, ct);
            if (!endpoint.IsSuccess)
                return Result<NodeEndpointInspection?>.Failed(endpoint.Error!);
            if (endpoint.Value is { } found)
                return Result<NodeEndpointInspection?>.Success(
                    new NodeEndpointInspection(host, found.ClientHostPort, found.Running));
        }

        return Result<NodeEndpointInspection?>.Success(null);
    }

    // Легаси-чистка (надзор C): том нод-локален — перебор ВСЕХ хостов;
    // 404 = успех на каждом (идемпотентность движка), 409 → Failed
    // (безусловный ретрай следующим тиком надзора).
    public async Task<Result> CleanupLegacyVolumeAsync(string cluster, CancellationToken ct)
    {
        foreach (var engine in _engines.Values)
        {
            var removed = await engine.DeleteVolumeAsync($"vwk-{cluster}-tls", ct);
            if (!removed.IsSuccess)
                return removed;
        }

        return Result.Success();
    }

    internal static string NodeName(string cluster, string nodeName)
        => $"vwk-{cluster}-{nodeName}";
}

// Swarm-режим: сервисы через manager endpoint, replicas=1, constraint node.id==<id>.
// hosts — endpoint'ы Engine API swarm-нод (по имени ноды, опционально):
// нужен легаси-том старой volume-модели нод-локален — CleanupLegacyVolumeAsync
// обходит и ноды таблицы. Нет ноды в таблице (однонодовый Docker
// Desktop/swarm) — manager engine (поведение не меняется).
public sealed class SwarmClusterDriver(
    string managerEndpoint,
    DockerEngineFactory factory,
    IReadOnlyList<HostEndpoint>? hosts = null) : IClusterDriver
{
    private readonly IDockerEngine _engine = factory.Create(managerEndpoint, hostAlias: null);

    // Engines нод из таблицы (по имени ноды размещения — как EnsureNodeAsync
    // матчит spec.Host по Hostname ноды).
    private readonly Dictionary<string, IDockerEngine> _nodeEngines = (hosts ?? [])
        .ToDictionary(h => h.Name, h => factory.Create(h.Endpoint, hostAlias: h.Name));

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
                [new PortMap(PlainClusterDriver.ClientContainerPort, spec.ClientHostPort)],
                spec.NodeName,
                Cmd: spec.Args, // обёртка env-TLS (args образного entrypoint, ResetEntrypoint=false)
                CpuCores: (double?)spec.CpuCores,
                MemoryBytes: spec.MemoryBytes,
                LabelKey: PlainClusterDriver.LabelKey,
                Label: spec.Cluster,
                Env: spec.Env); // TLS-материал — env сервиса
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

    // Docker-факт движка → доменная запись (конверсия PlainClusterDriver.Convert).
    public async Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct)
    {
        var limits = await _engine.InspectServiceResourcesAsync(PlainClusterDriver.NodeName(cluster, nodeName), ct);
        if (!limits.IsSuccess)
            return Result<NodeLimits?>.Failed(limits.Error!);
        return limits.Value is not null
            ? Result<NodeLimits?>.Success(PlainClusterDriver.Convert(limits.Value))
            : Result<NodeLimits?>.Success(null);
    }

    // Cmd сервиса ноды (сверка V3): Spec.TaskTemplate.ContainerSpec.Cmd.
    public Task<Result<IReadOnlyList<string>?>> NodeArgsAsync(
        string cluster, string nodeName, CancellationToken ct)
        => _engine.InspectServiceCmdAsync(PlainClusterDriver.NodeName(cluster, nodeName), ct);

    // Env сервиса ноды (сверки V3/надзора/фазы R).
    public Task<Result<IReadOnlyDictionary<string, string>?>> NodeEnvAsync(
        string cluster, string nodeName, CancellationToken ct)
        => _engine.InspectServiceEnvAsync(PlainClusterDriver.NodeName(cluster, nodeName), ct);

    // E9-реконструкция: published-порт и хост таска отдаёт одна инспекция
    // движка (swarm-фолбэк по running-таску — один HTTP-раунд ListTasks);
    // running-таск = Running (останавливаться у сервиса — только снятием).
    public async Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
        string cluster, string nodeName, CancellationToken ct)
    {
        var name = PlainClusterDriver.NodeName(cluster, nodeName);
        var endpoint = await _engine.InspectNodeEndpointAsync(name, PlainClusterDriver.ClientContainerPort, ct);
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

    // Легаси-чистка (надзор C): том старой volume-модели нод-локален — DELETE
    // на КАЖДОМ engine таблицы нод + manager (реальный volume мог жить на
    // ноде размещения; manager-копия — при ensure по fallback); 404 = успех
    // на каждом, 409 → Failed (безусловный ретрай тиком надзора).
    public async Task<Result> CleanupLegacyVolumeAsync(string cluster, CancellationToken ct)
    {
        foreach (var engine in _nodeEngines.Values.Append(_engine))
        {
            var removed = await engine.DeleteVolumeAsync($"vwk-{cluster}-tls", ct);
            if (!removed.IsSuccess)
                return removed;
        }

        return Result.Success();
    }
}
