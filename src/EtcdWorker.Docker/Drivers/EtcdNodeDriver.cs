namespace EtcdWorker.Docker.Drivers;

/// <summary>Хост docker-топологии: имя + endpoint Engine API
/// (tcp://host:2375 | unix:///var/run/docker.sock).</summary>
public sealed record HostEndpoint(string Name, string Endpoint);

/// <summary>Спецификация managed-узла etcd (arch/etcdworker/01-placement.md): имя etcdw-node-&lt;k&gt;,
/// host-публикация ТОЛЬКО client-порта; peer — имена сети контура.</summary>
public sealed record EtcdNodeSpec(
    int K,
    string Host,
    int ClientHostPort,
    string Image,
    string[] Cmd,
    IReadOnlyDictionary<string, string> Env,
    string Network);

/// <summary>Факт узла: размещение/живость/фактический client host-порт.</summary>
public sealed record EtcdNodeInspection(string? PlacedOn, bool Running, int? ClientHostPort);

// Управление managed-узлом etcd в обоих режимах (порт драйверов s3w — arch/etcdworker/README.md
// §2): объекты — контейнер/сервис etcdw-node-<k> с томом данных
// etcdw-node-<k>-data (/etcd-data), label-пара etcdworker=member. Наружу
// публикуется ТОЛЬКО client-порт 2379; peer-трафик — имена в сети контура.
// Идемпотентность: существующий объект сверяется по имени и не пересоздаётся
// (решение о сверке/замене — у процессов); 404 на удалении / 409 на создании —
// успех (движок).
public interface IEtcdNodeDriver
{
    // Сеть контура для узлов (Plain — на каждом хосте, Swarm — через manager).
    Task<Result> EnsureNetworkAsync(CancellationToken ct);

    // Занятые host-порты docker-публикаций (для PortAllocator).
    Task<Result<IReadOnlySet<int>>> GetBusyPortsAsync(CancellationToken ct);

    // Имена узлов-кандидатов размещения: plain — конфигурация хостов;
    // swarm — ready-ноды по Hostname.
    Task<Result<IReadOnlyList<string>>> GetSwarmNodesAsync(CancellationToken ct);

    // Идемпотентно создать узел: том данных → контейнер/сервис.
    Task<Result> EnsureNodeAsync(EtcdNodeSpec spec, CancellationToken ct);

    // Остановить и удалить узел (removeVolume=false — данные сохраняются).
    Task<Result> RemoveNodeAsync(int k, bool removeVolume, CancellationToken ct);

    // Инспекция размещения узла: null = docker-объекта нет — положительное
    // свидетельство смерти; ошибка инспекта → Failed — надзор не решает вслепую.
    Task<Result<EtcdNodeInspection?>> InspectNodeAsync(int k, CancellationToken ct);

    // Имена объектов узла (etcdw-node-*: контейнеры/сервисы + тома -data):
    // сверка декларации + сироты (X2).
    Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(CancellationToken ct);
}

// Plain-режим: контейнеры на перечисленных хостах, per-host Engine API.
public sealed class PlainEtcdNodeDriver(
    IReadOnlyList<HostEndpoint> hosts,
    DockerEngineFactory factory,
    string network) : IEtcdNodeDriver
{
    // Контейнерный порт client-грани etcd → выделенный host-порт (arch/etcdworker/01-placement.md).
    public const int ClientPortConst = 2379;

    // Label-пара контейнеров etcdw-домена (arch/etcdworker/01-placement.md: label "etcdworker").
    internal const string LabelKey = "etcdworker";
    internal const string LabelValue = "member";

    private readonly Dictionary<string, IDockerEngine> _engines = hosts.ToDictionary(
        h => h.Name,
        h => factory.Create(h.Endpoint, hostAlias: h.Name));

    public async Task<Result> EnsureNetworkAsync(CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            // Сеть контура нужна на каждом хосте, где может встать узел.
            foreach (var engine in _engines.Values)
            {
                var ensured = await engine.EnsureNetworkAsync(network, ct);
                if (!ensured.IsSuccess)
                    throw ensured.Error!;
            }
        });
    }

    public async Task<Result<IReadOnlySet<int>>> GetBusyPortsAsync(CancellationToken ct)
    {
        return await Result<IReadOnlySet<int>>.FromAsync(async () =>
        {
            var busy = new HashSet<int>();
            foreach (var engine in _engines.Values)
            {
                var ports = await engine.BusyPortsAsync(ct);
                if (!ports.IsSuccess)
                    throw ports.Error!;
                foreach (var pair in ports.Value)
                    busy.Add(pair.Port);
            }

            return (IReadOnlySet<int>)busy;
        });
    }

    public Task<Result<IReadOnlyList<string>>> GetSwarmNodesAsync(CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<string>>.Success((IReadOnlyList<string>)[hosts[0].Name]));

    public async Task<Result> EnsureNodeAsync(EtcdNodeSpec spec, CancellationToken ct)
    {
        if (!_engines.TryGetValue(spec.Host, out var engine))
            return Result.Failed(new ApplicationException(
                $"host {spec.Host} is not in the Docker:Hosts table (etcd node {spec.K})"));

        return await Result.FromAsync(async () =>
        {
            var name = NodeName(spec.K);
            var volumeName = VolumeName(spec.K);

            // Идемпотентность по ФАКТУ: существующий контейнер не пересоздаётся
            // (сверка/замена — у процессов); существующий, но НЕ запущенный —
            // доводится стартом.
            var existing = await engine.ListContainersAsync(name, all: true, ct);
            if (!existing.IsSuccess)
                throw existing.Error!;
            var existingContainer = existing.Value.FirstOrDefault(c => c.Names.Contains(name));
            if (existingContainer is not null)
            {
                if (!string.Equals(existingContainer.State, "running", StringComparison.OrdinalIgnoreCase))
                {
                    var startedExisting = await engine.StartContainerAsync(name, ct);
                    if (!startedExisting.IsSuccess)
                        throw startedExisting.Error!;
                }

                return;
            }

            // Том данных: кластерное состояние etcd переживает пересоздание узла;
            // label etcdworker=member — атрибуция листинга сирот.
            var volume = await engine.EnsureVolumeAsync(volumeName, LabelKey, LabelValue, ct);
            if (!volume.IsSuccess)
                throw volume.Error!;

            var containerSpec = new ContainerSpec(
                spec.Image,
                [new PortMap(ClientPortConst, spec.ClientHostPort)],
                name,
                Env: spec.Env,
                Cmd: spec.Cmd,
                VolumeName: volumeName,
                VolumeDest: "/etcd-data",
                Network: spec.Network,
                RestartPolicy: "unless-stopped",
                LabelKey: LabelKey,
                Label: LabelValue);

            var created = await engine.CreateContainerAsync(containerSpec, name, ct);
            if (!created.IsSuccess)
                throw created.Error!;
            var started = await engine.StartContainerAsync(name, ct);
            if (!started.IsSuccess)
                throw started.Error!;
        });
    }

    public async Task<Result> RemoveNodeAsync(int k, bool removeVolume, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            var name = NodeName(k);
            foreach (var engine in _engines.Values)
            {
                // Узел один (имя уникально): убираем на том хосте, где объект
                // найден; у остальных — 404 = успех.
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
                var volume = await engine.DeleteVolumeAsync(VolumeName(k), ct);
                if (!volume.IsSuccess)
                    throw volume.Error!;
            }
        });
    }

    public async Task<Result<EtcdNodeInspection?>> InspectNodeAsync(int k, CancellationToken ct)
    {
        var name = NodeName(k);
        foreach (var (host, engine) in _engines)
        {
            var endpoint = await engine.InspectNodeEndpointAsync(name, ClientPortConst, ct);
            if (!endpoint.IsSuccess)
                return Result<EtcdNodeInspection?>.Failed(endpoint.Error!);
            if (endpoint.Value is { } found)
                return Result<EtcdNodeInspection?>.Success(
                    new EtcdNodeInspection(host, found.Running, found.ClientHostPort));
        }

        return Result<EtcdNodeInspection?>.Success(null);
    }

    public async Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(CancellationToken ct)
    {
        return await Result<IReadOnlyList<string>>.FromAsync(async () =>
        {
            var names = new List<string>();
            var prefix = "etcdw-node-";
            foreach (var engine in _engines.Values)
            {
                var containers = await engine.ListContainersAsync(prefix, all: true, ct);
                if (!containers.IsSuccess)
                    throw containers.Error!;
                // Точность перечисления: префикс + label etcdworker=member —
                // посторонний объект с подходящим именем не попадает в снос X2.
                names.AddRange(containers.Value
                    .Where(c => HasMemberLabel(c.Labels))
                    .SelectMany(c => c.Names)
                    .Where(n => n.StartsWith(prefix, StringComparison.Ordinal)));
            }

            // Тома данных: листинг по префиксу; том нод-локален, сирота-том
            // может остаться на ЛЮБОМ хосте — объединение листингов всех движков.
            foreach (var engine in _engines.Values)
            {
                var volumes = await engine.ListVolumesAsync(prefix, ct);
                if (!volumes.IsSuccess)
                    throw volumes.Error!;
                names.AddRange(volumes.Value.Where(v => v.EndsWith("-data", StringComparison.Ordinal)));
            }

            return (IReadOnlyList<string>)names.Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList();
        });
    }

    // Label-факт домена: label etcdworker=member присутствует и равен.
    internal static bool HasMemberLabel(IReadOnlyDictionary<string, string>? labels)
        => labels is not null
           && labels.TryGetValue(LabelKey, out var value)
           && string.Equals(value, LabelValue, StringComparison.Ordinal);

    internal static string NodeName(int k)
        => $"etcdw-node-{k}";

    internal static string VolumeName(int k)
        => $"etcdw-node-{k}-data";
}

// Swarm-режим: сервисы через manager endpoint, replicas=1, constraint
// node.id==<id> (порт SwarmClusterDriver s3w). hosts — endpoint'ы Engine API
// swarm-нод (по имени ноды размещения): том данных нод-локален — снос тома
// идёт по таблице + manager.
public sealed class SwarmEtcdNodeDriver(
    string managerEndpoint,
    DockerEngineFactory factory,
    IReadOnlyList<HostEndpoint>? hosts,
    string network) : IEtcdNodeDriver
{
    private readonly IDockerEngine _engine = factory.Create(managerEndpoint, hostAlias: null);

    // Engines нод из таблицы (по имени ноды размещения — как EnsureNodeAsync
    // матчит spec.Host по Hostname ноды).
    private readonly Dictionary<string, IDockerEngine> _nodeEngines = (hosts ?? [])
        .ToDictionary(h => h.Name, h => factory.Create(h.Endpoint, hostAlias: h.Name));

    public Task<Result> EnsureNetworkAsync(CancellationToken ct)
        => _engine.EnsureNetworkAsync(network, ct);

    public async Task<Result<IReadOnlySet<int>>> GetBusyPortsAsync(CancellationToken ct)
    {
        var ports = await _engine.BusyPortsAsync(ct);
        if (!ports.IsSuccess)
            return Result<IReadOnlySet<int>>.Failed(ports.Error!);
        return Result<IReadOnlySet<int>>.Success(
            (IReadOnlySet<int>)ports.Value.Select(p => p.Port).ToHashSet());
    }

    public async Task<Result<IReadOnlyList<string>>> GetSwarmNodesAsync(CancellationToken ct)
    {
        return await Result<IReadOnlyList<string>>.FromAsync(async () =>
        {
            var nodes = await _engine.ListNodesAsync(ct);
            if (!nodes.IsSuccess)
                throw nodes.Error!;
            return (IReadOnlyList<string>)nodes.Value
                .Where(n => n.State == "ready") // недоступные swarm-ноды не участвуют в placement
                .Select(n => n.Hostname)
                .ToList();
        });
    }

    public async Task<Result> EnsureNodeAsync(EtcdNodeSpec spec, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            // constraint: node.id==<id>, id ищем по Hostname==spec.Host.
            var nodes = await _engine.ListNodesAsync(ct);
            if (!nodes.IsSuccess)
                throw nodes.Error!;
            var target = nodes.Value.FirstOrDefault(n => n.Hostname == spec.Host);
            if (target is null)
                throw new ApplicationException($"swarm node with Hostname={spec.Host} not found");

            // Идемпотентность: 409 already-exists — успех (движок); сверка/замена —
            // решение процессов; label etcdworker=member — атрибуция.
            var volume = await _engine.EnsureVolumeAsync(
                PlainEtcdNodeDriver.VolumeName(spec.K),
                PlainEtcdNodeDriver.LabelKey, PlainEtcdNodeDriver.LabelValue, ct);
            if (!volume.IsSuccess)
                throw volume.Error!;

            var template = new ContainerSpec(
                spec.Image,
                [new PortMap(PlainEtcdNodeDriver.ClientPortConst, spec.ClientHostPort)],
                PlainEtcdNodeDriver.NodeName(spec.K),
                Env: spec.Env,
                Cmd: spec.Cmd,
                VolumeName: PlainEtcdNodeDriver.VolumeName(spec.K),
                VolumeDest: "/etcd-data",
                LabelKey: PlainEtcdNodeDriver.LabelKey,
                Label: PlainEtcdNodeDriver.LabelValue);
            var serviceSpec = new ServiceSpec(
                PlainEtcdNodeDriver.NodeName(spec.K),
                template,
                target.Id);

            var created = await _engine.CreateServiceAsync(serviceSpec, ct);
            if (!created.IsSuccess)
                throw created.Error!;
        });
    }

    public async Task<Result> RemoveNodeAsync(int k, bool removeVolume, CancellationToken ct)
    {
        var removed = await _engine.RemoveServiceAsync(PlainEtcdNodeDriver.NodeName(k), ct);
        if (!removed.IsSuccess)
            return removed;

        if (!removeVolume)
            return Result.Success();

        foreach (var engine in _nodeEngines.Values.Append(_engine))
        {
            var volume = await engine.DeleteVolumeAsync(PlainEtcdNodeDriver.VolumeName(k), ct);
            if (!volume.IsSuccess)
                return volume;
        }

        return Result.Success();
    }

    public async Task<Result<EtcdNodeInspection?>> InspectNodeAsync(int k, CancellationToken ct)
    {
        var name = PlainEtcdNodeDriver.NodeName(k);
        var endpoint = await _engine.InspectNodeEndpointAsync(name, PlainEtcdNodeDriver.ClientPortConst, ct);
        if (!endpoint.IsSuccess)
            return Result<EtcdNodeInspection?>.Failed(endpoint.Error!);
        if (endpoint.Value is not { } found)
            return Result<EtcdNodeInspection?>.Success(null);

        return found.TaskHost is { } host
            ? Result<EtcdNodeInspection?>.Success(
                new EtcdNodeInspection(host, found.Running, found.ClientHostPort))
            : Result<EtcdNodeInspection?>.Success(null); // хоста таска нет — факта нет
    }

    // Объекты узлов в swarm — СЕРВИСЫ + тома данных (префикс + label
    // etcdworker=member; имена детерминированы по k).
    public async Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(CancellationToken ct)
    {
        var services = await _engine.ListServiceRecordsAsync("etcdw-node-", ct);
        if (!services.IsSuccess)
            return Result<IReadOnlyList<string>>.Failed(services.Error!);

        var names = new List<string>(services.Value
            .Where(svc => PlainEtcdNodeDriver.HasMemberLabel(svc.Labels))
            .Select(svc => svc.Name));
        // Тома данных: листинг по префиксу на всех движках (сирота-том может
        // остаться на любой ноде).
        foreach (var engine in _nodeEngines.Values.Append(_engine))
        {
            var volumes = await engine.ListVolumesAsync("etcdw-node-", ct);
            if (!volumes.IsSuccess)
                return Result<IReadOnlyList<string>>.Failed(volumes.Error!);
            names.AddRange(volumes.Value.Where(v => v.EndsWith("-data", StringComparison.Ordinal)));
        }

        return Result<IReadOnlyList<string>>.Success(
            names.Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList());
    }
}
