using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Docker.Engine;

namespace PgWorker.Docker.Drivers;

// Хост plain-режима: имя + endpoint Engine API (tcp://… | unix://…).
public sealed record HostEndpoint(string Name, string Endpoint);

// Унифицированное управление нодой кластера в обоих режимах (spec §5.2/§5.3).
// Идемпотентность: существующий объект (контейнер/сервис) сверяется по имени и
// не пересоздаётся; 404/409 на удалении/создании — успех (движок).
public interface IClusterDriver
{
    // Живые хосты для PlacementPlanner: plain — конфиг (UsedSlots по числу
    // контейнеров pgw-*), swarm — ListNodes (running tasks).
    Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct);

    // Занятые host:port (для PortAllocator).
    Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct);

    // Идемпотентно создать ноду (plain: container pgw-<C>-<X>-<n> + volume
    // pgw-<C>-<X>-<n>-data; swarm: service с constraint node.id==<id>, publish
    // mode=host). env/конфиги — из NodeConfigBuilders; существующий объект
    // сверяется по имени и не пересоздаётся.
    Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
        InstallSecrets secrets, EtcdEndpoints etcd, CancellationToken ct);

    // Остановить и удалить ноду + volume (404 = успех). swarm: service rm
    // (volume остаётся на ноде таска — manager не управляет volume нод).
    Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct);

    // Только остановить контейнер ноды (эвакуация E3: карантин вернувшегося
    // шарда — данные на месте, нода не удаляется). 404/304 = успех.
    Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct);

    // Имена объектов нод кластера (pgw-<C>-*): сверка декларации + сироты (D1).
    Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct);
}

// Plain-режим: контейнеры на перечисленных хостах, per-host Engine API.
public sealed class PlainClusterDriver(
    IReadOnlyList<HostEndpoint> hosts,
    DockerEngineFactory factory,
    bool enableDoorman,
    string nodeImage = "pgworker-node:dev") : IClusterDriver
{
    // Общая сеть нод кластера: Patroni-репликация по внутренним адресам
    // (alias = имя ноды); без user-defined сети hostname-резолва нет.
    public const string NodesNetwork = "pgw-net";

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
                var containers = await engine.ListContainersAsync("pgw-", all: true, ct);
                if (!containers.IsSuccess)
                    throw containers.Error!; // один хост недоступен — не тихим список
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

    public async Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
        InstallSecrets secrets, EtcdEndpoints etcd, CancellationToken ct)
    {
        if (!_engines.TryGetValue(addr.Host, out var engine))
            return Result.Failed(new ApplicationException(
                $"хост {addr.Host} не в таблице Docker:Hosts (кластер {topology.Cluster}/{nodeName})"));

        return await Result.FromAsync(async () =>
        {
            var name = NodeName(topology.Cluster, topology.Shard, nodeName);

            // Сеть нод (идемпотентно; 409 already exists = успех).
            var network = await engine.EnsureNetworkAsync(NodesNetwork, ct);
            if (!network.IsSuccess)
                throw network.Error!;

            // Идемпотентность: существующий контейнер не пересоздаётся (P2.1).
            var existing = await engine.ListContainersAsync(name, all: true, ct);
            if (!existing.IsSuccess)
                throw existing.Error!;
            if (existing.Value.Any(c => c.Names.Contains(name)))
                return;

            var spec = BuildSpec(topology, nodeName, addr, secrets, etcd);
            var created = await engine.CreateContainerAsync(spec, name, ct);
            if (!created.IsSuccess)
                throw created.Error!;
            var started = await engine.StartContainerAsync(name, ct);
            if (!started.IsSuccess)
                throw started.Error!;
        });
    }

    public async Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            var name = NodeName(cluster, shard, nodeName);
            foreach (var engine in _engines.Values)
            {
                // 404 на каждом шаге — успех (движок); volume pgw-…-data удаляем следом.
                var stopped = await engine.StopContainerAsync(name, timeoutSec: 10, ct);
                if (!stopped.IsSuccess)
                    throw stopped.Error!;
                var removed = await engine.RemoveContainerAsync(name, force: true, ct);
                if (!removed.IsSuccess)
                    throw removed.Error!;
                var volume = await engine.RemoveVolumeAsync(VolumeName(cluster, shard, nodeName), ct);
                if (!volume.IsSuccess)
                    throw volume.Error!;
            }
        });
    }

    public async Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            var name = NodeName(cluster, shard, nodeName);
            foreach (var engine in _engines.Values)
            {
                var stopped = await engine.StopContainerAsync(name, timeoutSec: 10, ct);
                if (!stopped.IsSuccess)
                    throw stopped.Error!; // карантин E3: только stop, volume/данные на месте
            }
        });
    }

    public async Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
    {
        return await Result<IReadOnlyList<string>>.FromAsync(async () =>
        {
            var names = new List<string>();
            var prefix = $"pgw-{cluster}-";
            foreach (var engine in _engines.Values)
            {
                var containers = await engine.ListContainersAsync(prefix, all: true, ct);
                if (!containers.IsSuccess)
                    throw containers.Error!;
                names.AddRange(containers.Value.SelectMany(c => c.Names).Where(n => n.StartsWith(prefix, StringComparison.Ordinal)));
            }

            return (IReadOnlyList<string>)names.Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList();
        });
    }

    // Сборка ContainerSpec: env Spilo + PGW_NODE_HOST + конфиги doorman/haproxy (Д4).
    internal ContainerSpec BuildSpec(ShardTopology topology, string nodeName, NodeAddress addr,
        InstallSecrets secrets, EtcdEndpoints etcd)
    {
        var env = new Dictionary<string, string>(SpiloEnvBuilder.Build(topology, etcd, secrets))
        {
            // Адрес этой ноды для lease-скрипта мастер-ключа (P11) и сверок.
            ["PGW_NODE_HOST"] = addr.Host,
            ["PGW_NODE_NAME"] = nodeName,
        };
        if (enableDoorman)
        {
            env["DOORMAN_CONFIG"] = DoormanConfigBuilder.Build(topology.Cluster);
            env["PGW_DOORMAN_PORT"] = addr.Ports.Doorman.ToString();
        }

        // HAProxy-фронтенд НЕ поднимаем: PG и HAProxy конфликтуют на :5432 в
        // одном netns (Д4 — один контейнер на ноду). Write-вход MVP — прямой
        // pg-порт master-ноды (portalloc, multi-host DSN); конфиг остаётся в
        // Core (HaproxyConfigBuilder) для отдельного фронтенд-слоя (roadmap).

        var ports = new List<PortMap>
        {
            new(5432, addr.Ports.Pg), // pg: межшард-подписки + HAProxy-вход (P2)
            new(8008, addr.Ports.Patroni), // Patroni REST (пробы/сверка P11)
        };
        if (enableDoorman)
            ports.Add(new PortMap(6432, addr.Ports.Doorman)); // клиентский вход (P13/P14)

        return new ContainerSpec(
            nodeImage,
            env,
            VolumeName(topology.Cluster, topology.Shard, nodeName),
            "/home/postgres/pgdata", // дефолтный PGDATA-корень Spilo (pgroot ломает bootstrap)
            ports,
            nodeName,
            CpuCores: null,
            MemoryBytes: null,
            Label: topology.Cluster,
            Network: NodesNetwork,
            NetworkAliases: [nodeName, NodeName(topology.Cluster, topology.Shard, nodeName)]);
    }

    internal static string NodeName(string cluster, string shard, string nodeName)
        => $"pgw-{cluster}-{shard}-{nodeName}";

    internal static string VolumeName(string cluster, string shard, string nodeName)
        => $"{NodeName(cluster, shard, nodeName)}-data";
}

// Swarm-режим: сервисы через manager endpoint, replicas=1, constraint node.id==<id>.
public sealed class SwarmClusterDriver(
    string managerEndpoint,
    DockerEngineFactory factory,
    bool enableDoorman,
    string nodeImage = "pgworker-node:dev") : IClusterDriver
{
    private readonly IDockerEngine _engine = factory.Create(managerEndpoint, hostAlias: null);

    // Менеджер не управляет volume нод: volume создаётся/живёт на ноде таска;
    // при RemoveNode сервис удаляется, volume остаётся (данные; осознанный MVP).

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

    public async Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
        InstallSecrets secrets, EtcdEndpoints etcd, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            // constraint: node.id==<id>, id ищем по Hostname==addr.Host (spec §5.3).
            var nodes = await _engine.ListNodesAsync(ct);
            if (!nodes.IsSuccess)
                throw nodes.Error!;
            var target = nodes.Value.FirstOrDefault(n => n.Hostname == addr.Host);
            if (target is null)
                throw new ApplicationException($"swarm-нода с Hostname={addr.Host} не найдена");

            var plain = new PlainClusterDriver([], new DockerEngineFactory(), enableDoorman, nodeImage);
            var template = plain.BuildSpec(topology, nodeName, addr, secrets, etcd);
            var spec = new ServiceSpec(
                PlainClusterDriver.NodeName(topology.Cluster, topology.Shard, nodeName),
                template,
                target.Id);

            // Идемпотентность: 409 already-exists — успех (движок).
            var created = await _engine.CreateServiceAsync(spec, ct);
            if (!created.IsSuccess)
                throw created.Error!;
        });
    }

    public async Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct)
    {
        // volume на ноде таска manager'у не виден — удаляем сервис (данные остаются
        // в volume ноды; восстановление/слияние — runbook, spec §6.4 E3).
        var removed = await _engine.RemoveServiceAsync(PlainClusterDriver.NodeName(cluster, shard, nodeName), ct);
        return removed;
    }

    public async Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct)
    {
        // Карантин E3 в swarm: удаляем сервис (контейнер таска погашен, данные в
        // volume ноды на месте); supervisor-перезапуск исключён.
        var stopped = await _engine.RemoveServiceAsync(PlainClusterDriver.NodeName(cluster, shard, nodeName), ct);
        return stopped;
    }

    public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<string>>.Success(
            (IReadOnlyList<string>)[])); // MVP: список сервисов недоступен без ListServices — сироты plain-only
}
