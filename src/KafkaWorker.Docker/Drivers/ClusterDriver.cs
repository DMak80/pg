using KafkaWorker.Core;
using KafkaWorker.Core.Planning;
using KafkaWorker.Docker.Engine;

namespace KafkaWorker.Docker.Drivers;

// Хост plain-режима: имя + endpoint Engine API (tcp://… | unix://…).
public sealed record HostEndpoint(string Name, string Endpoint);

/// <summary>
/// Спецификация брокера для docker-драйвера: env целиком готовит NodeEnvBuilder
/// (Core/Templates — детерминирован от заявки/portalloc/кредов); драйвер только
/// размещает (хост, host-порт CLIENT 9094, лимиты, volume, сеть kfw-net).
/// </summary>
public sealed record KafkaNodeSpec(
    string Cluster,
    string NodeName,
    string Host,
    int ClientHostPort,
    string Image,
    IReadOnlyDictionary<string, string> Env,
    decimal? CpuCores,
    long? MemoryBytes);

// Унифицированное управление брокером в обоих режимах (arch/16 §2.3). Порт
// драйверов PgWorker с заменой pgw-→kfw- и выносом env-генерации в NodeEnvBuilder:
// объекты — контейнер/сервис kfw-<C>-<b>, volume kfw-<C>-<b>-data, сеть kfw-net.
// Идемпотентность: существующий объект сверяется по имени и не пересоздаётся;
// 404 на удалении / 409 на создании — успех (движок).
public interface IClusterDriver
{
    // Живые хосты для PlacementPlanner: plain — конфиг (UsedSlots по числу
    // контейнеров kfw-*), swarm — ListNodes (running tasks).
    Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct);

    // Занятые host:port (для PortAllocator).
    Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct);

    // Идемпотентно создать брокера (plain: контейнер kfw-<C>-<b> + volume
    // kfw-<C>-<b>-data; swarm: сервис с constraint node.id==<id>, publish mode=host).
    Task<Result> EnsureNodeAsync(KafkaNodeSpec spec, CancellationToken ct);

    // Остановить и удалить брокера; removeVolume=true — удалить и том данных
    // (deprovisioning/remove-broker; 404 = успех).
    Task<Result> RemoveNodeAsync(string cluster, string nodeName, bool removeVolume, CancellationToken ct);

    // Существует ли том данных брокера (kfw-<C>-<b>-data). Консервативная
    // семантика надзора (arch/16 §5 C): «неизвестно/не управляем» = true —
    // чистый том допускается только при доказанной утрате (потери данных
    // недопустимы).
    Task<Result<bool>> NodeVolumeExistsAsync(string cluster, string nodeName, CancellationToken ct);

    // Выполнить команду в контейнере живого брокера (arch/16 §2.4; t02:
    // kafka-reassign-partitions CLI): plain — running-контейнер по имени
    // kfw-<C>-<b> перебором хостов; swarm — running-таск сервиса →
    // ContainerId. stdout → строка; exit != 0 → Failed (движок).
    Task<Result<string>> ExecNodeAsync(string cluster, string nodeName, IReadOnlyList<string> cmd, CancellationToken ct);

    // Имена объектов брокеров кластера (kfw-<C>-*): сверка декларации + сироты (X1).
    Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct);
}

// Plain-режим: контейнеры на перечисленных хостах, per-host Engine API.
public sealed class PlainClusterDriver(
    IReadOnlyList<HostEndpoint> hosts,
    DockerEngineFactory factory) : IClusterDriver
{
    // Общая сеть брокеров кластера: KRaft-кворум и межброкерный трафик по
    // внутренним адресам (alias = имя ноды); порт pgw-net.
    public const string NodesNetwork = "kfw-net";

    // Контейнерный порт CLIENT-listener → выделенный host-порт.
    public const int ClientContainerPort = 9094;

    // Точка монтирования тома данных kafka (arch/16 §2.1).
    public const string DataDir = "/var/lib/kafka/data";

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
                var containers = await engine.ListContainersAsync("kfw-", all: true, ct);
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

    public async Task<Result> EnsureNodeAsync(KafkaNodeSpec spec, CancellationToken ct)
    {
        if (!_engines.TryGetValue(spec.Host, out var engine))
            return Result.Failed(new ApplicationException(
                $"хост {spec.Host} не в таблице Docker:Hosts (кластер {spec.Cluster}/{spec.NodeName})"));

        return await Result.FromAsync(async () =>
        {
            var name = NodeName(spec.Cluster, spec.NodeName);

            // Сеть брокеров (идемпотентно; 409 already exists = успех).
            var network = await engine.EnsureNetworkAsync(NodesNetwork, ct);
            if (!network.IsSuccess)
                throw network.Error!;

            // Идемпотентность: существующий контейнер не пересоздаётся (K3).
            var existing = await engine.ListContainersAsync(name, all: true, ct);
            if (!existing.IsSuccess)
                throw existing.Error!;
            if (existing.Value.Any(c => c.Names.Contains(name)))
                return;

            var containerSpec = new ContainerSpec(
                spec.Image,
                spec.Env,
                VolumeName(spec.Cluster, spec.NodeName),
                DataDir,
                [new PortMap(ClientContainerPort, spec.ClientHostPort)],
                spec.NodeName,
                CpuCores: (double?)spec.CpuCores,
                MemoryBytes: spec.MemoryBytes,
                Label: spec.Cluster,
                Network: NodesNetwork,
                NetworkAliases: [spec.NodeName, name]);

            var created = await engine.CreateContainerAsync(containerSpec, name, ct);
            if (!created.IsSuccess)
                throw created.Error!;
            var started = await engine.StartContainerAsync(name, ct);
            if (!started.IsSuccess)
                throw started.Error!;
        });
    }

    public async Task<Result> RemoveNodeAsync(string cluster, string nodeName, bool removeVolume, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            var name = NodeName(cluster, nodeName);
            foreach (var engine in _engines.Values)
            {
                // 404 на каждом шаге — успех (движок); volume kfw-…-data — по флагу.
                var stopped = await engine.StopContainerAsync(name, timeoutSec: 10, ct);
                if (!stopped.IsSuccess)
                    throw stopped.Error!;
                var removed = await engine.RemoveContainerAsync(name, force: true, ct);
                if (!removed.IsSuccess)
                    throw removed.Error!;
                if (removeVolume)
                {
                    var volume = await engine.RemoveVolumeAsync(VolumeName(cluster, nodeName), ct);
                    if (!volume.IsSuccess)
                        throw volume.Error!;
                }
            }
        });
    }

    public async Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
    {
        return await Result<IReadOnlyList<string>>.FromAsync(async () =>
        {
            var names = new List<string>();
            var prefix = $"kfw-{cluster}-";
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

    // Том есть на любом хосте таблицы → жив; недоступность хоста = Failed
    // (надзор не решает судьбу тома вслепую — тик завершится ошибкой).
    public async Task<Result<bool>> NodeVolumeExistsAsync(string cluster, string nodeName, CancellationToken ct)
    {
        foreach (var engine in _engines.Values)
        {
            var exists = await engine.VolumeExistsAsync(VolumeName(cluster, nodeName), ct);
            if (!exists.IsSuccess)
                return exists.Error!;
            if (exists.Value)
                return Result<bool>.Success(true);
        }

        return Result<bool>.Success(false);
    }

    // Контейнер брокера по имени kfw-<C>-<b>: перебор хостов (аналог Remove),
    // на первом, где найден running-контейнер — exec (порт PgWorker t01).
    public async Task<Result<string>> ExecNodeAsync(
        string cluster, string nodeName, IReadOnlyList<string> cmd, CancellationToken ct)
    {
        return await Result<string>.FromAsync(async () =>
        {
            var name = NodeName(cluster, nodeName);
            foreach (var engine in _engines.Values)
            {
                var containers = await engine.ListContainersAsync(name, all: false, ct);
                if (!containers.IsSuccess)
                    throw containers.Error!;

                var running = containers.Value.FirstOrDefault(c =>
                    c.Names.Contains(name) && c.State == "running");
                if (running is null)
                    continue; // контейнера нет на этом хосте — следующий

                var exec = await engine.ExecAsync(running.Id, cmd, ct);
                if (!exec.IsSuccess)
                    throw exec.Error!;
                return exec.Value;
            }

            throw new ApplicationException($"контейнер брокера {name} не найден (нет running-контейнера ни на одном хосте)");
        });
    }

    internal static string NodeName(string cluster, string nodeName)
        => $"kfw-{cluster}-{nodeName}";

    internal static string VolumeName(string cluster, string nodeName)
        => $"{NodeName(cluster, nodeName)}-data";
}

// Swarm-режим: сервисы через manager endpoint, replicas=1, constraint node.id==<id>.
public sealed class SwarmClusterDriver(
    string managerEndpoint,
    DockerEngineFactory factory) : IClusterDriver
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

    public async Task<Result> EnsureNodeAsync(KafkaNodeSpec spec, CancellationToken ct)
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
                spec.Env,
                PlainClusterDriver.VolumeName(spec.Cluster, spec.NodeName),
                PlainClusterDriver.DataDir,
                [new PortMap(PlainClusterDriver.ClientContainerPort, spec.ClientHostPort)],
                spec.NodeName,
                CpuCores: (double?)spec.CpuCores,
                MemoryBytes: spec.MemoryBytes,
                Label: spec.Cluster,
                Network: PlainClusterDriver.NodesNetwork,
                NetworkAliases: [spec.NodeName, PlainClusterDriver.NodeName(spec.Cluster, spec.NodeName)]);
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

    public Task<Result> RemoveNodeAsync(string cluster, string nodeName, bool removeVolume, CancellationToken ct)
    {
        // volume на ноде таска manager'у не виден — удаляем сервис (данные
        // остаются в volume ноды; полный демонтаж — runbook).
        return _engine.RemoveServiceAsync(PlainClusterDriver.NodeName(cluster, nodeName), ct);
    }

    // Менеджер тома ноды таска не видит: консервативно «жив» — надзор не
    // удаляет то, чем не управляет (потери данных недопустимы).
    public Task<Result<bool>> NodeVolumeExistsAsync(string cluster, string nodeName, CancellationToken ct)
        => Task.FromResult(Result<bool>.Success(true));

    // Объекты брокеров кластера в swarm — СЕРВИСЫ: GET /services с префиксом
    // kfw-<C>-. Существование сервиса ≠ живой таск: живость — AdminClient-пробы.
    public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
        => _engine.ListServicesAsync($"kfw-{cluster}-", ct);

    // Контейнер брокера — running-таск сервиса (ContainerID уже в ответе
    // /tasks; порт PgWorker t01).
    public async Task<Result<string>> ExecNodeAsync(
        string cluster, string nodeName, IReadOnlyList<string> cmd, CancellationToken ct)
    {
        return await Result<string>.FromAsync(async () =>
        {
            var tasks = await _engine.ListTasksAsync(PlainClusterDriver.NodeName(cluster, nodeName), ct);
            if (!tasks.IsSuccess)
                throw tasks.Error!;

            var running = tasks.Value.FirstOrDefault(t =>
                t.State == "running" && t.ContainerId is { Length: > 0 });
            if (running is null)
                throw new ApplicationException(
                    $"контейнер брокера {cluster}/{nodeName} не найден (нет running-таска)");

            var exec = await _engine.ExecAsync(running.ContainerId!, cmd, ct);
            if (!exec.IsSuccess)
                throw exec.Error!;
            return exec.Value;
        });
    }
}
