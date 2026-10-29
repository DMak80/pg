using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;

namespace PgWorker.Docker.Drivers;

// Хост plain-режима: имя + endpoint Engine API (tcp://… | unix://…).
public sealed record HostEndpoint(string Name, string Endpoint);

/// <summary>Наличие данных PG у ноды (Д3, arch/14 R11): Present/Absent — факт
/// PG_VERSION пробой приёмника ноды (arch/25 №22); Unknown — транспорт
/// недоступен (НЕ доказательство утраты).</summary>
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
    // tuning — рассчитанный per-shard PGTune-вывод (PgtuneInputsFactory, arch/14
    // §2.1: пересчёт на каждый EnsureNode-путь, без фиксации в etcd); несётся в
    // SPILO_CONFIGURATION (merge(PGTune ∪ канон)) и doorman-бюджет; null →
    // прежний хардкод-набор (изолированные пути). syncStrict — per-cluster
    // опция synchronous_mode_strict из config кластера (t06, arch/14 §3).
    // restPassword — эффективная per-cluster REST-пара (t22, arch/14 §2.1/§5 I:
    // pending окна ротации ?? ключ кластера; резолвор RestRotation.EffectivePassword
    // у вызывающих процессов).
    Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
        InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources,
        PgTuneResult? tuning, bool syncStrict, string restPassword, CancellationToken ct);

    // Остановить и удалить ноду + volume (404 = успех). swarm: service rm
    // (volume остаётся на ноде таска — manager не управляет volume нод).
    Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct);

    // Только остановить контейнер ноды (эвакуация E3: карантин вернувшегося
    // шарда — данные на месте, нода не удаляется). 404/304 = успех.
    Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct);

    // Данные ноды (Д3, spec §3.7) — пробой приёмника ноды (arch/25 №22),
    // шов INodeDataProbe: docker-exec в продукте не применяется.
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

    // Имена объектов нод кластера (pgw-<C>-*): сверка декларации + сироты (D1).
    Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct);

    // Инспекция env живого контейнера ноды (t22, arch/14 §5 C — шаг пересоздания:
    // критерий кандидата — факт env: SSL_RESTAPI_CERTIFICATE / PGW_REST_PASSWORD_HASH).
    // Нет контейнера — пустой словарь; Swarm — заглушка (шаг пересоздания — no-op,
    // симметрия заглушки InspectNodesAsync).
    Task<Result<IReadOnlyDictionary<string, string>>> InspectNodeEnvAsync(
        string cluster, string shard, string nodeName, CancellationToken ct);

    // ── WAL-агенты бэкапов (arch/19 §3, t03) ──

    // Санитарный снос legacy wal-контейнеров pgw-backup-wal-<C>-* (+ их
    // -staging-тома) — t25: приёмник живёт в контейнере ноды, отдельных
    // контейнеров больше нет; вызов идемпотентен, 404 = успех.
    Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct);

    // Честная running-инспекция нод (arch/14 §5 C): InspectNodesAsync отражает
    // ФАКТ running-процесса, пустой результат = ноды нет. Plain — да; Swarm —
    // нет (инспект — заглушка): ветки надзора, трактующие «нет в инспекте» как
    // смерть процесса (ускорение failover, t09), для таких драйверов выключены.
    bool SupportsRunningInspection { get; }

    // Docker-движок хоста по имени (plain-таблица/advertised; swarm — manager):
    // джобы бэкапов создаются/супервизируются на docker-хосте источника (t02).
    // null — хост не известен (вызывающий трактует как transient).
    IDockerEngine? EngineFor(string host);

    // Чистка tool-контейнеров бэкапов кластера (Deprovisioning D1/D2,

}

// Plain-режим: контейнеры на перечисленных хостах, per-host Engine API.
// advertisedHost (arch/16 advertised-правило): имя, под которым docker-хост
// виден КЛИЕНТАМ записей etcd (portalloc/dsn — панель). Всё, что драйвер
// отдаёт НАРУЖУ (плановые хосты, busy-кортежи, факты инспекции канонических
// нод), несёт advertised-имя — единый namespace адресов с записями portalloc;
// внутренние имена остаются ключами движков. Внешние находки усыновления
// (object) advertised не получают — их адресация операторская (R9-симметрия).
// scrapeNetwork (t15, arch/14 §2.1, arch/18 §5.4): имя сети контура мониторинга
// поставки — задано → Ensure-attach канонических нод к ней поверх pgw-net-<C>
// (скрейп Prometheus по alias:8008); пусто → поведение идентично базе.
// restCertificates (t22, arch/14 §2.1): кеш серверных сертов REST-эндпоинтов
// нод (DI-синглтон из per-install CA); null — только изолированные юнит-пути
// драйвера без BuildSpec (HTTP-режима не существует — BuildSpec fail-fast).
public sealed class PlainClusterDriver(
    IReadOnlyList<HostEndpoint> hosts,
    DockerEngineFactory factory,
    bool enableDoorman,
    string nodeImage = "pgworker-node:dev",
    string? advertisedHost = null,
    IReadOnlySet<string>? pgtuneExclude = null,
    string? scrapeNetwork = null,
    string? contourNetwork = null,
    RestCertificateCache? restCertificates = null,
    NodeBackupEnv? nodeBackupEnv = null) : IClusterDriver
{
    // Label-ключ контейнеров/сервисов pg-домена (t07: ключ — параметр спеки
    // движка LabelKey; читателей label в коде нет, docker-inspect-косметика).
    internal const string LabelKey = "pgworker";

    // Plain: инспект контейнера — факт running-процесса (arch/14 §5 C).
    public bool SupportsRunningInspection => true;
    // Сеть нод — PER-CLUSTER (arch/14 §2.1, как kfw-net-<C> у KafkaWorker):
    // Patroni-репликация по внутренним адресам (alias = имя ноды), без
    // user-defined сети hostname-резолва нет. Единая сеть делала DNS-зону
    // общей для всех кластеров docker-хоста — перекрёстная видимость чужих
    // алиасов, сетевой изоляции контуров не было; per-cluster сеть даёт её
    // по построению (уникальное имя кластера ⇒ уникальная DNS-зона, t24).
    public const string NodesNetworkPrefix = "pgw-net-";

    // Сеть контура поставки (t41, arch/14 §2.1): задана → ноды/агенты живут в
    // ней (pgw-net-<C> не создаётся и не удаляется движком; сеть — владелец
    // поставка).
    private readonly string? contourNetwork = contourNetwork;

    // Сеть нод/агентов кластера по режиму: контур или per-cluster pgw-net-<C>.
    private string NodesNet(string cluster) => contourNetwork ?? NetworkName(cluster);

    // Ensure сети (идемпотентность, arch/14 §2.1): 409 already exists — успех
    // (сеть контура существует у поставки; реальный движок глотает 409 сам —
    // обёртка страхует контракт для любого движка/фейка).
    private static async Task<Result> EnsureNetAsync(IDockerEngine engine, string name, CancellationToken ct)
    {
        var ensured = await engine.EnsureNetworkAsync(name, ct);
        return !ensured.IsSuccess && ensured.Error!.Message.Contains("409", StringComparison.Ordinal)
            ? Result.Success()
            : ensured;
    }

    public static string NetworkName(string cluster) => $"{NodesNetworkPrefix}{cluster}";

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
                // t25: legacy pgw-backup-wal-* (если остались) попадают в счётчик
                // хоста СОЗНАТЕЛЬНО — потребляют ресурсы до санитарного сноса
                // (RemoveBackupAgentsAsync в тике WalStreamProcess); из
                // ListNodeObjectsAsync фильтруются (там семантика «объекты НОД»).
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
        InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources,
        PgTuneResult? tuning, bool syncStrict, string restPassword, CancellationToken ct)
    {
        if (!_engines.TryGetValue(addr.Host, out var engine))
        {
            // advertised-режим: адрес ноды (запись portalloc) несёт advertised-имя,
            // а не ключ движка; валидация старта гарантирует единственный хост.
            if (advertisedHost is not { Length: > 0 } || addr.Host != advertisedHost || _engines.Count != 1)
                return Result.Failed(new ApplicationException(
                    $"host {addr.Host} not in the Docker:Hosts table (cluster {topology.Cluster}/{nodeName})"));
            engine = _engines.Values.Single();
        }

        return await Result.FromAsync(async () =>
        {
            var name = NodeName(topology.Cluster, topology.Shard, nodeName);

            // Сеть нод кластера (идемпотентно; 409 already exists = успех);
            // в контур-режиме — сеть контура (владелец — поставка).
            var network = await EnsureNetAsync(engine, NodesNet(topology.Cluster), ct);
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
                {
                    // Ensure-инвариант кластерной сети (arch/14 §2.1, t24): нода
                    // обязана жить в сети кластера (в контур-режиме — сеть
                    // контура, подключение идемпотентно) — контейнер из старой
                    // единой сети (наследие миграции) подключается к ней;
                    // пересоздание недопустимо (нода с данными).
                    var clusterNet = NodesNet(topology.Cluster);
                    if ((inspect.Value.Networks ?? []).All(n => n != clusterNet))
                    {
                        var connected = await engine.NetworkConnectAsync(clusterNet, name, ct);
                        if (!connected.IsSuccess)
                            throw connected.Error!;
                    }

                    // Ensure-attach к scrape-сети контура мониторинга (t15,
                    // arch/14 §2.1/§2.4): идемпотентно по inspect; сети нет на
                    // хосте — fail-fast с diagnose (владелец сети — поставка).
                    if (scrapeNetwork is { Length: > 0 } && (inspect.Value.Networks ?? []).All(n => n != scrapeNetwork))
                    {
                        var scraped = await engine.NetworkConnectAsync(scrapeNetwork, name, ct);
                        if (!scraped.IsSuccess)
                            throw scraped.Error!;
                    }

                    return; // контейнер на месте с планом — идемпотентность
                }

                var stopped = await engine.KillContainerAsync(name, ct);
                if (!stopped.IsSuccess)
                    throw stopped.Error!;
                var removed = await engine.RemoveContainerAsync(name, force: true, ct);
                if (!removed.IsSuccess)
                    throw removed.Error!;
            }

            var spec = BuildSpec(topology, nodeName, addr, secrets, etcd, resources, tuning, syncStrict, restPassword);
            var created = await engine.CreateContainerAsync(spec, name, ct);
            if (!created.IsSuccess)
                throw created.Error!;
            var started = await engine.StartContainerAsync(name, ct);
            if (!started.IsSuccess)
                throw started.Error!;

            // Attach новой ноды к scrape-сети контура (t15, arch/14 §2.1):
            // повторный connect docker принимает как no-op (идемпотентность);
            // сети нет (404) — Result.Failed с diagnose, провижининг несёт
            // ошибку в journal/last_error (fail-fast, а не молчаливый пропуск).
            if (scrapeNetwork is { Length: > 0 })
            {
                var scraped = await engine.NetworkConnectAsync(scrapeNetwork, name, ct);
                if (!scraped.IsSuccess)
                    throw scraped.Error!;
            }
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
                var stopped = await engine.KillContainerAsync(name, ct);
                if (!stopped.IsSuccess)
                    throw stopped.Error!;
                var removed = await engine.RemoveContainerAsync(name, force: true, ct);
                if (!removed.IsSuccess)
                    throw removed.Error!;
                var volume = await engine.RemoveVolumeAsync(VolumeName(cluster, shard, nodeName), ct);
                if (!volume.IsSuccess)
                    throw volume.Error!;
            }

            // Демонтаж сети кластера (arch/14 §2.1, kfw-паттерн): когда объектов
            // кластера (ноды) не осталось, per-cluster сеть удаляется — пул
            // subnet'ов хоста конечен, сирот не копим. WAL-агентов в сети больше
            // нет по построению (t25: приёмник — внутри контейнера ноды); legacy
            // pgw-backup-wal-* сносит санитарный вызов тика WalStreamProcess.
            // «Active endpoints» транзиентен (docker API держит endpoint после
            // force-remove) — ретраи; итоговая неудача НЕ роняет демонтаж (сеть
            // могла быть снесена параллельно) — громкий след в stderr воркера,
            // подберут следующий демонтаж/чистка хоста.
            // Контур-режим (t41): сеть контура принадлежит поставке — движок
            // её не удаляет (pgw-net-<C> на стенде не существует вовсе).
            if (contourNetwork is not null)
                return;
            var objects = await ListNodeObjectsAsync(cluster, ct);
            if (!objects.IsSuccess || objects.Value.Count > 0)
                return;
            foreach (var engine in _engines.Values)
            {
                for (var attempt = 0; ; attempt++)
                {
                    var deleted = await engine.DeleteNetworkAsync(NetworkName(cluster), ct);
                    if (deleted.IsSuccess)
                        break;
                    var error = deleted.Error!;
                    if (attempt >= 3
                        || !error.Message.Contains("active endpoints", StringComparison.Ordinal))
                    {
                        Console.Error.WriteLine(
                            $"pgw: cluster network {NetworkName(cluster)} not removed during teardown: {error.Message}");
                        break;
                    }

                    await Task.Delay(1000, ct); // endpoint отцепится — повторяем
                }
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
                var stopped = await engine.KillContainerAsync(name, ct);
                if (!stopped.IsSuccess)
                    throw stopped.Error!; // карантин E3: только kill (spilo, arch/14 §5 D E3), volume/данные на месте
            }
        });
    }

    // ── WAL-агенты бэкапов (t03, arch/19 §3): те же engine-инстансы, сеть нод ──

    // Санитарный снос legacy wal-контейнеров pgw-backup-wal-<C>-* (+ их
    // -staging-тома) — t25: приёмник живёт в контейнере ноды, отдельных
    // контейнеров больше нет; вызов идемпотентен, 404 = успех.
    // Per-engine tolerate (multi-host): отказ list одного docker-хоста НЕ
    // прерывает снос на остальных — недоступный хост снести нельзя всё равно;
    // итог Failed (снос неполный) — вызывающий решает (WalStream — журнал,
    // teardown — ретрай тиком).
    public async Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            Exception? hostFailure = null;
            foreach (var engine in _engines.Values)
            {
                IReadOnlyList<string> agents;
                try
                {
                    agents = await ListAgentsOfEngineAsync(engine, cluster, ct);
                }
                catch (Exception e)
                {
                    hostFailure ??= e; // недоступный хост — остальные обрабатываем
                    continue;
                }

                foreach (var agent in agents)
                {
                    var agentShard = AgentShardOf(cluster, agent);
                    if (shard is not null && agentShard != shard)
                        continue;
                    var stopped = await engine.StopContainerAsync(agent, timeoutSec: 10, ct);
                    if (!stopped.IsSuccess)
                        throw stopped.Error!;
                    var removed = await engine.RemoveContainerAsync(agent, force: true, ct);
                    if (!removed.IsSuccess)
                        throw removed.Error!;
                    var volume = await engine.RemoveVolumeAsync($"{agent}-staging", ct);
                    if (!volume.IsSuccess)
                        throw volume.Error!;
                }
            }

            if (hostFailure is { } failure)
                throw new ApplicationException(
                    $"legacy agent sweep {cluster}: engine host failed (others processed): {failure.Message}", failure);
        });
    }

    // Имена legacy контейнеров-агентов кластера (Names — с ведущим "/"); префикс
    // pgw-backup-wal-<C>- — приватная константа сноса (t25: отдельных агентов
    // больше нет — только санитарный снос).
    private static string AgentPrefix(string cluster) => $"pgw-backup-wal-{cluster}-";

    private static async Task<IReadOnlyList<string>> ListAgentsOfEngineAsync(
        IDockerEngine engine, string cluster, CancellationToken ct)
    {
        var listed = await engine.ListContainersAsync(AgentPrefix(cluster), all: true, ct);
        if (!listed.IsSuccess)
            throw listed.Error!;
        return listed.Value
            .SelectMany(c => c.Names)
            .Where(n => n.TrimStart('/').StartsWith(AgentPrefix(cluster), StringComparison.Ordinal))
            .Select(n => n.TrimStart('/'))
            .Distinct()
            .ToList();
    }

    // pgw-backup-wal-<C>-<ХВОСТ> → <X>: хвост == <X> (старый одноагентный формат,
    // миграция t27 §3.6) ИЛИ начинается с <X>- (per-node имя t27).
    private static string AgentShardOf(string cluster, string containerName)
    {
        var tail = containerName[AgentPrefix(cluster).Length..];
        return tail.StartsWith("-", StringComparison.Ordinal) ? "" : tail.Split('-')[0];
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
                names.AddRange(containers.Value.SelectMany(c => c.Names)
                    .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
                    .Where(n => !n.TrimStart('/').StartsWith("pgw-backup-wal-", StringComparison.Ordinal)));
            }

            return (IReadOnlyList<string>)names.Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList();
        });
    }

    public IDockerEngine? EngineFor(string host)
    {
        if (_engines.TryGetValue(host, out var engine))
            return engine;
        return advertisedHost is { Length: > 0 } && host == advertisedHost && _engines.Count == 1
            ? _engines.Values.Single()
            : null;
    }


    // Сборка ContainerSpec: env Spilo + PGW_NODE_HOST + конфиги doorman/haproxy (Д4).
    // tuning — рассчитанный per-shard PGTune-вывод: env — merge(PGTune ∪ канон,
    // минус pgtuneExclude), doorman-бюджет — от рассчитанного max_connections
    // (P15); tuning == null → прежний хардкод-набор и бюджет 55. syncStrict —
    // per-cluster опция synchronous_mode_strict из config кластера (t06).
    // restPassword — эффективная REST-пара; серт ноды выпускается из кеша
    // (t22): restCertificates == null — fail-fast (HTTP-режима не существует).
    internal ContainerSpec BuildSpec(ShardTopology topology, string nodeName, NodeAddress addr,
        InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, PgTuneResult? tuning,
        bool syncStrict, string restPassword)
    {
        if (restCertificates is null)
            throw new ApplicationException(
                "BuildSpec: RestCertificateCache was not passed to the driver — node REST-TLS material " +
                "is mandatory (HTTP provisioning mode does not exist, arch/14 §2.1)");
        var (certPem, keyPem) = restCertificates.GetOrCreate(topology.Cluster, topology.Shard, nodeName);
        var env = new Dictionary<string, string>(
            SpiloEnvBuilder.Build(topology, etcd, secrets, syncStrict, nodeName,
                new NodeRestTls(certPem, keyPem, restCertificates.CaPem, restPassword),
                tuning, pgtuneExclude))
        {
            // Адрес этой ноды для lease-скрипта мастер-ключа (P11) и сверок.
            ["PGW_NODE_HOST"] = addr.Host,
            ["PGW_NODE_NAME"] = nodeName,
        };

        // Бэкап-цели приёмника WAL (t25, arch/19 §7): per-install константы
        // (bucket/CA https S3), ставятся ДОКЕР-ДРАЙВЕРОМ из Backups-опций
        // воркера (NodeBackupEnv). Bucket пуст — env не ставится: приёмник
        // стоит в waiting_policy с диагностикой до появления env (рестарт
        // ноды/EnsureNode) — старт-отказа живого процесса нет (инстанс
        // чинит валидация старта воркера Enabled=true ⇒ Bucket непустой).
        if (nodeBackupEnv is { } backupEnv)
        {
            if (!string.IsNullOrWhiteSpace(backupEnv.Bucket))
                env["PGW_BACKUP_S3_BUCKET"] = backupEnv.Bucket;
            if (!string.IsNullOrWhiteSpace(backupEnv.CaPem))
                env["PGW_BACKUP_S3_CA_PEM"] = backupEnv.CaPem;
            // Staging полного (t26, arch/19 §2/§9): каталог в data-volume
            // ноды; null/пусто → env не ставится — приёмник возьмёт дефолт.
            if (!string.IsNullOrWhiteSpace(backupEnv.StagingDir))
                env["PGW_BACKUP_STAGING_DIR"] = backupEnv.StagingDir;
        }

        if (enableDoorman)
        {
            // P15 (решение 2026-09-12): бюджет вычисляемый —
            // max(10, max_connections − 5); tuning null (изолированные пути) —
            // прежний литерал 55.
            var serverConnections = tuning is null ? 55 : DoormanConfigBuilder.ServerConnections(tuning);
            env["DOORMAN_CONFIG"] = DoormanConfigBuilder.Build(topology.Cluster, serverConnections);
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
            ports,
            nodeName,
            Env: env,
            VolumeName: VolumeName(topology.Cluster, topology.Shard, nodeName),
            VolumeDest: "/home/postgres/pgdata", // дефолтный PGDATA-корень Spilo (pgroot ломает bootstrap)
            CpuCores: resources?.CpuCores,
            MemoryBytes: resources?.MemoryBytes,
            LabelKey: LabelKey,
            Label: topology.Cluster,
            Network: NodesNet(topology.Cluster),
            NetworkAliases: [nodeName, NodeName(topology.Cluster, topology.Shard, nodeName)]);
    }

    // Env живого контейнера ноды (по имени на всех хостах таблицы — записи
    // portalloc нет у шага; хост с находкой несёт непустой env). Нет контейнера
    // нигде / transport-фейл всех хостов — пустой словарь (не свидетельство).
    public async Task<Result<IReadOnlyDictionary<string, string>>> InspectNodeEnvAsync(
        string cluster, string shard, string nodeName, CancellationToken ct)
    {
        var empty = (IReadOnlyDictionary<string, string>)new Dictionary<string, string>();
        foreach (var engine in _engines.Values)
        {
            var env = await engine.InspectContainerEnvAsync(NodeName(cluster, shard, nodeName), ct);
            if (env is { IsSuccess: true, Value: { Count: > 0 } })
                return Result<IReadOnlyDictionary<string, string>>.Success(env.Value);
        }

        return Result<IReadOnlyDictionary<string, string>>.Success(empty);
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
    string nodeImage = "pgworker-node:dev",
    IReadOnlySet<string>? pgtuneExclude = null,
    RestCertificateCache? restCertificates = null,
    NodeBackupEnv? nodeBackupEnv = null) : IClusterDriver
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
        InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources,
        PgTuneResult? tuning, bool syncStrict, string restPassword, CancellationToken ct)
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
                throw new ApplicationException($"swarm node with Hostname={addr.Host} not found");

            // Внутренний plain для BuildSpec — со СВОИМ кешем сертов (t22: без
            // кеша внутренний BuildSpec упал бы fail-fast'ом REST-TLS).
            var plain = new PlainClusterDriver(
                [], new DockerEngineFactory(), enableDoorman, nodeImage,
                pgtuneExclude: pgtuneExclude, restCertificates: restCertificates,
                nodeBackupEnv: nodeBackupEnv);
            var template = plain.BuildSpec(topology, nodeName, addr, secrets, etcd, resources, tuning, syncStrict, restPassword);
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

    // t25: агентов больше нет (приёмник — в контейнере ноды) — Remove остаётся
    // санитарным сносом legacy pgw-backup-wal-* (swarm-сервисы старого формата).
    public async Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            // Префикс pgw-backup-wal-<C>- — строкой (t25: агентов-контейнеров
            // больше нет — только санитарный снос legacy).
            const string prefixBase = "pgw-backup-wal-";
            var prefix = $"{prefixBase}{cluster}-";
            var services = await _engine.ListServicesAsync(prefix, ct);
            if (!services.IsSuccess)
                throw services.Error!;
            foreach (var service in services.Value)
            {
                if (shard is not null && AgentShardOfSwarm(prefix, service) != shard)
                    continue;
                var removed = await _engine.RemoveServiceAsync(service, ct);
                if (!removed.IsSuccess)
                    throw removed.Error!;
            }
        });
    }

    // Хвост == <X> (старый формат) ИЛИ начинается с <X>- (per-node, t27) — как Plain.
    private static string AgentShardOfSwarm(string prefix, string service)
    {
        var tail = service[prefix.Length..];
        return tail.StartsWith("-", StringComparison.Ordinal) ? "" : tail.Split('-')[0];
    }

    // Swarm: шаг пересоздания нод надзора — no-op (заглушка, симметрия
    // InspectNodesAsync): сервисы не инспектируются по env.
    public Task<Result<IReadOnlyDictionary<string, string>>> InspectNodeEnvAsync(
        string cluster, string shard, string nodeName, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success(
            (IReadOnlyDictionary<string, string>)new Dictionary<string, string>()));

    // Усыновление swarm-кластеров: за пределами текущей задачи (стенд plain,
    // spec §3.1); при необходимости — инспект тасков сервисов.
    public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
        string cluster, IReadOnlyCollection<string> nodeNames, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyDictionary<string, DiscoveredNode>>.Success(
            (IReadOnlyDictionary<string, DiscoveredNode>)new Dictionary<string, DiscoveredNode>()));

    // Объекты нод кластера в swarm — СЕРВИСЫ (rework №4): GET /services с
    // префиксом pgw-<C>-. Ранее возвращался пустой список → drift-сверка
    // надзора и guard D2 не видели живые сервисы (осцилляция PROVISIONING→
    // RUNNING каждый тик, сироты не чистились). Существование сервиса ≠ живой
    // таск: сверка декларации проверяет объект, живость — Patroni-пробы.
    public async Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
        => await _engine.ListServicesAsync($"pgw-{cluster}-", ct);

    // swarm: джобы создаются движком manager'а (constraint-планировщик не
    // используется для ephemeral-джобов — источниковые хосты известны).
    public IDockerEngine? EngineFor(string host) => _engine;


}
