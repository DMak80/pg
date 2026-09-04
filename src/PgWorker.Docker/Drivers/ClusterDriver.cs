using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Docker.Engine;

namespace PgWorker.Docker.Drivers;

// Хост plain-режима: имя + endpoint Engine API (tcp://… | unix://…).
public sealed record HostEndpoint(string Name, string Endpoint);

/// <summary>Наличие данных PG у ноды (Д3, arch/14 R11): Present/Absent — доказано
/// exec-пробой PG_VERSION; Unknown — транспорт недоступен (НЕ доказательство утраты).</summary>
public enum DataPresence { Present, Absent, Unknown }

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
    // сверяется по имени и не пересоздаётся. resources — заявка request_*
    // (лимиты NanoCPUs/Memory; request_disk лимита в docker не имеет — игнор).
    Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
        InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, CancellationToken ct);

    // Остановить и удалить ноду + volume (404 = успех). swarm: service rm
    // (volume остаётся на ноде таска — manager не управляет volume нод).
    Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct);

    // Только остановить контейнер ноды (эвакуация E3: карантин вернувшегося
    // шарда — данные на месте, нода не удаляется). 404/304 = успех.
    Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct);

    // Данные ноды (Д3, spec §3.7): docker-exec test -f PG_VERSION; контейнера
    // нет/exec-сбой/нечитаемый stdout → Unknown (утрата не доказана).
    Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct);

    // Выполнить команду в контейнере ноды (t01: pg_dump внутри мастер-контейнера
    // источника), вернуть stdout. Идемпотентности не требует (read-only утилита).
    Task<Result<string>> ExecNodeAsync(
        string cluster, string shard, string node, IReadOnlyList<string> cmd, CancellationToken ct);

    // Docker-инспекция нод усыновления (spec §3.1, arch/14 §5 J AD1; live-Ф7
    // 2026-09-01): по именам нод вернуть DiscoveredNode (host/object/порты).
    // Контейнеры ЧУЖИХ pgw-кластеров исключаются ДО матчинга — имена нод
    // неуникальны между кластерами одного docker-хоста (pgw-canon-*/pgw-canon10-*
    // с одинаковым hostname "shard1a" без фильтра давали неоднозначность →
    // пропуск ВСЕГО); внешние контейнеры усыновления (as-*/hc*) остаются видны.
    // 0 находок — пустой словарь.
    Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
        string cluster,
        IReadOnlyCollection<string> nodeNames, CancellationToken ct);

    // Exec в контейнер по имени (docker-exec fallback для pg_dump усыновлённых
    // нод, spec §3.3): 404/не найден — Failed.
    Task<Result<string>> ExecContainerAsync(string containerName, IReadOnlyList<string> cmd, CancellationToken ct);

    // Имена объектов нод кластера (pgw-<C>-*): сверка декларации + сироты (D1).
    Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct);

    // Честная running-инспекция нод (arch/14 §5 C): InspectNodesAsync отражает
    // ФАКТ running-процесса, пустой результат = ноды нет. Plain — да; Swarm —
    // нет (инспект — заглушка): ветки надзора, трактующие «нет в инспекте» как
    // смерть процесса (ускорение failover, t09), для таких драйверов выключены.
    bool SupportsRunningInspection { get; }
}

// Plain-режим: контейнеры на перечисленных хостах, per-host Engine API.
// advertisedHost (arch/16 advertised-правило): имя, под которым docker-хост
// виден КЛИЕНТАМ записей etcd (portalloc/dsn — панель). Всё, что драйвер
// отдаёт НАРУЖУ (плановые хосты, busy-кортежи, факты инспекции канонических
// нод), несёт advertised-имя — единый namespace адресов с записями portalloc;
// внутренние имена остаются ключами движков. Внешние находки усыновления
// (object) advertised не получают — их адресация операторская (R9-симметрия).
public sealed class PlainClusterDriver(
    IReadOnlyList<HostEndpoint> hosts,
    DockerEngineFactory factory,
    bool enableDoorman,
    string nodeImage = "pgworker-node:dev",
    string? advertisedHost = null) : IClusterDriver
{
    // Plain: инспект контейнера — факт running-процесса (arch/14 §5 C).
    public bool SupportsRunningInspection => true;
    // Общая сеть нод кластера: Patroni-репликация по внутренним адресам
    // (alias = имя ноды); без user-defined сети hostname-резолва нет.
    public const string NodesNetwork = "pgw-net";

    private static string Advertised(string host, string? advertised)
        => advertised is { Length: > 0 } ? advertised : host;

    private readonly Dictionary<string, IDockerEngine> _engines = hosts.ToDictionary(
        h => h.Name,
        h => factory.Create(h.Endpoint, hostAlias: Advertised(h.Name, advertisedHost)));

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
                // Плановое имя хоста — advertised: кандидаты аллокатора живут в одном
                // namespace с записями portalloc и busy-кортежами (advertised-правило).
                result.Add(new HostInfo(Advertised(name, advertisedHost), containers.Value.Count));
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
        InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, CancellationToken ct)
    {
        if (!_engines.TryGetValue(addr.Host, out var engine))
        {
            // advertised-режим: адрес ноды (запись portalloc) несёт advertised-имя,
            // а не ключ движка; валидация старта гарантирует единственный хост.
            if (advertisedHost is not { Length: > 0 } || addr.Host != advertisedHost || _engines.Count != 1)
                return Result.Failed(new ApplicationException(
                    $"хост {addr.Host} не в таблице Docker:Hosts (кластер {topology.Cluster}/{nodeName})"));
            engine = _engines.Values.Single();
        }

        return await Result.FromAsync(async () =>
        {
            var name = NodeName(topology.Cluster, topology.Shard, nodeName);

            // Сеть нод (идемпотентно; 409 already exists = успех).
            var network = await engine.EnsureNetworkAsync(NodesNetwork, ct);
            if (!network.IsSuccess)
                throw network.Error!;

            // Идемпотентность со сверкой портов (spec §3.2): существующий контейнер
            // обязан нести план публичных биндингов; расхождение → пересоздание
            // (фаза PROVISIONING — данных нет, volume сохраняется). Без сверки контейнер
            // навсегда оставался на чужих портах: WaitPatroni бил в мёртвый порт.
            // Усыновлённая нода (object) — чужой контейнер: сверка неприменима (R9).
            var lookupName = addr.Object ?? name;
            var existing = await engine.ListContainersAsync(lookupName, all: true, ct);
            if (!existing.IsSuccess)
                throw existing.Error!;
            if (existing.Value.FirstOrDefault(c => c.Names.Contains(lookupName)) is { } container)
            {
                if (!string.IsNullOrEmpty(addr.Object))
                    return; // усыновлённая (object) — чужой контейнер, не трогаем (R9)

                var inspect = await engine.InspectContainerAsync(container.Id, ct);
                if (!inspect.IsSuccess)
                    throw inspect.Error!;
                if (PortsMatchPlan(inspect.Value.Ports, addr))
                    return; // контейнер на месте с планом — идемпотентность

                var stopped = await engine.StopContainerAsync(name, timeoutSec: 10, ct);
                if (!stopped.IsSuccess)
                    throw stopped.Error!;
                var removed = await engine.RemoveContainerAsync(name, force: true, ct);
                if (!removed.IsSuccess)
                    throw removed.Error!;
            }

            var spec = BuildSpec(topology, nodeName, addr, secrets, etcd, resources);
            var created = await engine.CreateContainerAsync(spec, name, ct);
            if (!created.IsSuccess)
                throw created.Error!;
            var started = await engine.StartContainerAsync(name, ct);
            if (!started.IsSuccess)
                throw started.Error!;
        });
    }

    // Все ожидаемые public-биндинги контейнера совпадают с планом ноды
    // (5432→pg, 8008→patroni, 6432→doorman при enableDoorman).
    private bool PortsMatchPlan(IReadOnlyList<PortMap> actual, NodeAddress addr)
    {
        var expected = new List<PortMap> { new(5432, addr.Ports.Pg), new(8008, addr.Ports.Patroni) };
        if (enableDoorman)
            expected.Add(new PortMap(6432, addr.Ports.Doorman));
        return expected.All(e => actual.Any(p => p.ContainerPort == e.ContainerPort && p.HostPort == e.HostPort));
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

    // Данные ноды (Д3): docker-exec test -f PG_VERSION; контейнера нет/exec-сбой/
    // нечитаемый stdout → Unknown (утрата не доказана — arch/14 R11).
    public async Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct)
    {
        // PGDATA Spilo (arch/14 §2.1): volume-корень /home/postgres/pgdata,
        // данные — pgroot/data/PG_VERSION.
        const string marker = "/home/postgres/pgdata/pgroot/data/PG_VERSION";
        var exec = await ExecNodeAsync(cluster, shard, node,
            ["sh", "-c", $"test -f {marker} && echo present || echo absent"], ct);
        if (!exec.IsSuccess)
            return Result<DataPresence>.Success(DataPresence.Unknown);
        return Result<DataPresence>.Success(exec.Value.Trim() switch
        {
            "present" => DataPresence.Present,
            "absent" => DataPresence.Absent,
            _ => DataPresence.Unknown,
        });
    }

    // Контейнер ноды по имени pgw-<C>-<X>-<n>: перебор хостов (аналог StopNode),
    // на первом, где найден running-контейнер — exec (t01: pg_dump-транспорт).
    public async Task<Result<string>> ExecNodeAsync(
        string cluster, string shard, string node, IReadOnlyList<string> cmd, CancellationToken ct)
    {
        return await Result<string>.FromAsync(async () =>
        {
            var name = NodeName(cluster, shard, node);
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

            throw new ApplicationException($"контейнер ноды {name} не найден (нет running-контейнера ни на одном хосте)");
        });
    }

    // Docker-инспекция нод усыновления (spec §3.1): по каждому хосту собираем
    // пары (контейнер, инспект) КЛАСТЕРА и зовём NodeMatcher.Match один раз на
    // хост — только так работают merge patroni-порта из сайдкара (env NODE_NAME)
    // и skip-on-ambiguity «два контейнера на имя → пропуск» (юнит-тесты NodeMatcher).
    public async Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
        string cluster, IReadOnlyCollection<string> nodeNames, CancellationToken ct)
    {
        return await Result<IReadOnlyDictionary<string, DiscoveredNode>>.FromAsync(async () =>
        {
            var found = new Dictionary<string, DiscoveredNode>();
            foreach (var (host, engine) in _engines)
            {
                var list = await engine.ListContainersAsync("", all: false, ct);
                if (!list.IsSuccess)
                    throw list.Error!; // хост недоступен — не тихий список (паттерн GetHostsAsync)

                // Фильтр кластера (live-Ф7): имена нод неуникальны между кластерами
                // одного docker-хоста — без фильтра NodeMatcher видел чужие pgw-ноды
                // как неоднозначность на КАЖДОЕ имя и пропускал все находки.
                // Чужие pgw-<C'>-* исключаем; Match должен видеть свои ноды, её
                // patroni-сайдкар (env NODE_NAME) и внешние контейнеры усыновления
                // (as-*/hc*, не pgw-*) — один вызов на хост.
                var ownPrefix = $"pgw-{cluster}-";
                var pairs = new List<(DockerContainer, DockerContainerInspect)>();
                foreach (var c in list.Value)
                {
                    if (IsForeignPgw(c, ownPrefix))
                        continue;
                    var inspect = await engine.InspectContainerAsync(c.Id, ct);
                    if (inspect.IsSuccess)
                        pairs.Add((c, inspect.Value)); // контейнер исчез между list и inspect — не наша находка
                }

                // advertised-режим: факт КАНОНИЧЕСКОЙ ноды (pgw-<C>-*) несёт advertised-имя
                // хоста — записи portalloc/dsn резолвимы клиентами (панелью); внешние
                // находки усыновления — docker-имя хоста как есть (операторский контур,
                // R9-симметрия).
                var canonicalPrefix = $"pgw-{cluster}-";
                foreach (var (name, node) in NodeMatcher.Match(host, pairs, nodeNames))
                {
                    var fact = advertisedHost is { Length: > 0 }
                        && node.Object.StartsWith(canonicalPrefix, StringComparison.Ordinal)
                        ? node with { Host = advertisedHost }
                        : node;
                    if (!found.ContainsKey(name))
                        found[name] = fact;
                }
            }

            return (IReadOnlyDictionary<string, DiscoveredNode>)found;
        });
    }

    // Контейнер чужого pgw-кластера: зовётся pgw-*, но не pgw-<наш кластер>-*
    // (Names движок отдаёт без ведущего «/»; внешние as-*/hc* — не pgw-* → false).
    private static bool IsForeignPgw(DockerContainer c, string ownPrefix)
        => c.Names.Any(n => n.StartsWith("pgw-", StringComparison.Ordinal))
            && c.Names.All(n => !n.StartsWith(ownPrefix, StringComparison.Ordinal));

    // Exec в контейнер по имени (docker-exec fallback pg_dump усыновлённых нод,
    // spec §3.3): перебор хостов, первый running-контейнер с точным именем.
    public async Task<Result<string>> ExecContainerAsync(string containerName, IReadOnlyList<string> cmd, CancellationToken ct)
    {
        return await Result<string>.FromAsync(async () =>
        {
            foreach (var engine in _engines.Values)
            {
                var list = await engine.ListContainersAsync(containerName, all: false, ct);
                if (!list.IsSuccess)
                    throw list.Error!;
                if (list.Value.FirstOrDefault(c => c.Names.Contains(containerName)) is not { } hit)
                    continue;

                var exec = await engine.ExecAsync(hit.Id, cmd, ct);
                if (!exec.IsSuccess)
                    throw exec.Error!;
                return exec.Value;
            }

            throw new ApplicationException($"контейнер '{containerName}' не найден на хостах драйвера");
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
        InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources)
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
            CpuCores: resources?.CpuCores,
            MemoryBytes: resources?.MemoryBytes,
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

    // Swarm: InspectNodesAsync — заглушка (пустой результат), «нет в инспекте»
    // НЕ свидетельство смерти → inspect-ускорение failover выключено
    // (arch/14 §5 C, t09-review: иначе любой транспортный флап пробы лидера
    // давал ложный failover живого лидера).
    public bool SupportsRunningInspection => false;

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
        InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            // swarm: сверка портов не реализована (MVP, стенд plain — spec §5):
            // при необходимости — ListTasks(service) → ContainerId running-таска →
            // InspectContainerAsync и тот же PortsMatchPlan-критерий.

            // constraint: node.id==<id>, id ищем по Hostname==addr.Host (spec §5.3).
            var nodes = await _engine.ListNodesAsync(ct);
            if (!nodes.IsSuccess)
                throw nodes.Error!;
            var target = nodes.Value.FirstOrDefault(n => n.Hostname == addr.Host);
            if (target is null)
                throw new ApplicationException($"swarm-нода с Hostname={addr.Host} не найдена");

            var plain = new PlainClusterDriver([], new DockerEngineFactory(), enableDoorman, nodeImage);
            var template = plain.BuildSpec(topology, nodeName, addr, secrets, etcd, resources);
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

    // Данные ноды (Д3): через exec running-таска сервиса (свой ExecNodeAsync);
    // утрата не доказана → Unknown (arch/14 R11).
    public async Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct)
    {
        const string marker = "/home/postgres/pgdata/pgroot/data/PG_VERSION";
        var exec = await ExecNodeAsync(cluster, shard, node,
            ["sh", "-c", $"test -f {marker} && echo present || echo absent"], ct);
        if (!exec.IsSuccess)
            return Result<DataPresence>.Success(DataPresence.Unknown);
        return Result<DataPresence>.Success(exec.Value.Trim() switch
        {
            "present" => DataPresence.Present,
            "absent" => DataPresence.Absent,
            _ => DataPresence.Unknown,
        });
    }

    // Контейнер ноды — running-таск сервиса (ContainerID уже в ответе /tasks).
    public async Task<Result<string>> ExecNodeAsync(
        string cluster, string shard, string node, IReadOnlyList<string> cmd, CancellationToken ct)
    {
        return await Result<string>.FromAsync(async () =>
        {
            var tasks = await _engine.ListTasksAsync(PlainClusterDriver.NodeName(cluster, shard, node), ct);
            if (!tasks.IsSuccess)
                throw tasks.Error!;

            var running = tasks.Value.FirstOrDefault(t =>
                t.State == "running" && t.ContainerId is { Length: > 0 });
            if (running is null)
                throw new ApplicationException(
                    $"контейнер ноды {cluster}/{shard}/{node} не найден (нет running-таска)");

            var exec = await _engine.ExecAsync(running.ContainerId!, cmd, ct);
            if (!exec.IsSuccess)
                throw exec.Error!;
            return exec.Value;
        });
    }

    // Усыновление swarm-кластеров: за пределами текущей задачи (стенд plain,
    // spec §3.1); при необходимости — инспект тасков сервисов.
    public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
        string cluster, IReadOnlyCollection<string> nodeNames, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyDictionary<string, DiscoveredNode>>.Success(
            (IReadOnlyDictionary<string, DiscoveredNode>)new Dictionary<string, DiscoveredNode>()));

    // Exec в контейнент по имени сервиса (образец ExecNodeAsync): running-таск
    // сервиса → ContainerId → engine.ExecAsync.
    public async Task<Result<string>> ExecContainerAsync(string containerName, IReadOnlyList<string> cmd, CancellationToken ct)
    {
        return await Result<string>.FromAsync(async () =>
        {
            var tasks = await _engine.ListTasksAsync(containerName, ct);
            if (!tasks.IsSuccess)
                throw tasks.Error!;

            var running = tasks.Value.FirstOrDefault(t =>
                t.State == "running" && t.ContainerId is { Length: > 0 });
            if (running is null)
                throw new ApplicationException(
                    $"контейнер '{containerName}' не найден (нет running-таска)");

            var exec = await _engine.ExecAsync(running.ContainerId!, cmd, ct);
            if (!exec.IsSuccess)
                throw exec.Error!;
            return exec.Value;
        });
    }

    // Объекты нод кластера в swarm — СЕРВИСЫ (rework №4): GET /services с
    // префиксом pgw-<C>-. Ранее возвращался пустой список → drift-сверка
    // надзора и guard D2 не видели живые сервисы (осцилляция PROVISIONING→
    // RUNNING каждый тик, сироты не чистились). Существование сервиса ≠ живой
    // таск: сверка декларации проверяет объект, живость — Patroni-пробы.
    public async Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
        => await _engine.ListServicesAsync($"pgw-{cluster}-", ct);
}
