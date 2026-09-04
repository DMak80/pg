using KafkaWorker.Core;
using KafkaWorker.Core.Planning;
using KafkaWorker.Docker.Engine;

namespace KafkaWorker.Docker.Drivers;

// Хост plain-режима: имя + endpoint Engine API (tcp://… | unix://…).
public sealed record HostEndpoint(string Name, string Endpoint);

/// <summary>
/// Спецификация брокера для docker-драйвера: env целиком готовит NodeEnvBuilder
/// (Core/Templates — детерминирован от заявки/portalloc/кредов); драйвер только
/// размещает (хост, host-порт CLIENT 9094, лимиты, volume, сеть kfw-net-<C>).
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

// Инспекция размещения брокера (E9-реконструкция portalloc, t05): Host — хост
// размещения, ClientHostPort — published host-порт CLIENT (9094),
// AdvertisedClient — клиентская пара из env KAFKA_ADVERTISED_LISTENERS
// (контрольная сверка; null — источник недоступен, swarm).
public sealed record NodeEndpointInspection(string Host, int ClientHostPort, string? AdvertisedClient);

// Унифицированное управление брокером в обоих режимах (arch/16 §2.3). Порт
// драйверов PgWorker с заменой pgw-→kfw- и выносом env-генерации в NodeEnvBuilder:
// объекты — контейнер/сервис kfw-<C>-<b>, volume kfw-<C>-<b>-data, сеть
// kfw-net-<C> (per-cluster, t09-фикс).
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

    // Фактические лимиты контейнера/сервиса брокера (t06, spec §5.3): null =
    // объекта нет; ошибка инспекта → Failed (регенератор не решает вслепую).
    Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct);

    // Инспекция размещения брокера для E9-реконструкции portalloc (t05):
    // null = docker-объекта нет — положительное свидетельство смерти, arch/17 S7;
    // ошибка инспекта → Failed — надзор не решает вслепую.
    Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
        string cluster, string nodeName, CancellationToken ct);

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
    // Сеть брокеров кластера: KRaft-кворум и межброкерный трафик по
    // внутренним адресам (alias = имя ноды). Per-cluster (t09-фикс, arch/16
    // §2.1): короткие алиасы broker<N> уникальны в своей сети — единая сеть
    // с общими алиасами ломала Raft-голоса через docker-DNS round-robin
    // (INCONSISTENT_CLUSTER_ID), на docker-хосте собирался максимум один
    // кластер. Существующие контейнеры в старой kfw-net продолжают работать.
    public const string NodesNetworkPrefix = "kfw-net-";

    public static string NetworkName(string cluster) => $"{NodesNetworkPrefix}{cluster}";

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

            // Сеть брокеров кластера — per-cluster (t09-фикс, arch/16 §2.1);
            // идемпотентно; 409 already exists = успех.
            var network = await engine.EnsureNetworkAsync(NetworkName(spec.Cluster), ct);
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
                Network: NetworkName(spec.Cluster),
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

            // Последний брокер кластера демонтирован — удаляем per-cluster
            // сеть (t09-фикс, arch/16 §2.5) на КАЖДОМ хосте таблицы:
            // EnsureNodeAsync создаёт её на каждом хосте размещения нод —
            // чистка не зависит от того, где был последний демонтируемый
            // брокер (multi-host plain, анти-аффинити arch/16 §2.1); 404 =
            // сети уже нет = успех. Пул subnet'ов хоста конечен — сирот не
            // копим. Ретрай per-host: docker API доли секунды держит endpoint
            // после force-remove («has active endpoints» — транзиентно).
            // Старая единая kfw-net не матчится именем (kfw-net-<C>) — не трогаем.
            var objects = await ListNodeObjectsAsync(cluster, ct);
            if (objects.IsSuccess && objects.Value.Count == 0)
            {
                foreach (var engine in _engines.Values)
                {
                    for (var attempt = 0; ; attempt++)
                    {
                        var network = await engine.DeleteNetworkAsync(NetworkName(cluster), ct);
                        if (network.IsSuccess)
                            break;
                        if (attempt >= 3
                            || !network.Error!.Message.Contains("active endpoints", StringComparison.Ordinal))
                            throw network.Error!;
                        await Task.Delay(1000, ct); // endpoint отцепится — повторяем
                    }
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

    // Перебор хостов: первый хост с контейнером отдаёт факт (симметрия Exec).
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

    // E9-реконструкция (t05): перебор хостов — первый, где контейнер есть,
    // отдаёт host-порт + host-алиас этого движка.
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
                    new NodeEndpointInspection(host, found.ClientHostPort, found.AdvertisedClient));
        }

        return Result<NodeEndpointInspection?>.Success(null);
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
                Network: PlainClusterDriver.NetworkName(spec.Cluster),
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

    public Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct)
        => _engine.InspectServiceResourcesAsync(PlainClusterDriver.NodeName(cluster, nodeName), ct);

    // E9-реконструкция (t05): published-порт, advertised и хост таска отдаёт
    // одна инспекция движка (swarm-фолбэк по running-таску; ревью Ф7-3 — один
    // HTTP-раунд ListTasks, второй вызов драйвера удалён).
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
                new NodeEndpointInspection(host, found.ClientHostPort, found.AdvertisedClient))
            : Result<NodeEndpointInspection?>.Success(null); // хоста таска нет — факта нет
    }

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
