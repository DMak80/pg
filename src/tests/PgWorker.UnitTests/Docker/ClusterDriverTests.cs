using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using PgWorker.Core;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// Драйверы кластера plain/swarm (задача 15): идемпотентный EnsureNode, stop-only
// карантин E3, порядок RemoveNode, constraint+publish у swarm-сервиса.
public class ClusterDriverTests
{
    // Мок движка: записывает вызовы и отвечает заготовками.
    private sealed class FakeEngine : IDockerEngine
    {
        public List<(string Call, object? Arg)> Calls = [];

        public List<DockerContainer> Containers = [];
        public List<DockerSwarmNode> Nodes = [];
        public List<string> Services = [];
        public IReadOnlySet<(string, int)> Busy = new HashSet<(string, int)>();
        public ContainerSpec? CreatedSpec;
        public ServiceSpec? CreatedService;
        public string CreatedName = "";

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public Task<Result> EnsureNetworkAsync(string name, CancellationToken ct)
        {
            Calls.Add(("ensure-network", name));
            return Task.FromResult(Result.Success());
        }

        public Task<Result> PingAsync(CancellationToken ct)
        {
            Calls.Add(("ping", null));
            return Task.FromResult(Result.Success());
        }

        public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(string namePrefix, bool all, CancellationToken ct)
        {
            Calls.Add(("list-containers", namePrefix));
            var matched = Containers
                .Where(c => c.Names.Any(n => n.StartsWith(namePrefix, StringComparison.Ordinal)))
                .ToList();
            return Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success(matched));
        }

        // Инспекты по Id (adopt-repair T2): пустая карта → Failed — движок честно не нашёл.
        public Dictionary<string, DockerContainerInspect> Inspects = [];

        public Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct)
        {
            Calls.Add(("inspect-container", id));
            return Task.FromResult(Inspects.TryGetValue(id, out var inspect)
                ? Result<DockerContainerInspect>.Success(inspect)
                : Result<DockerContainerInspect>.Failed(new ApplicationException($"инспект {id} недоступен (стаб)")));
        }

        public Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct)
        {
            Calls.Add(("create", spec));
            CreatedSpec = spec;
            CreatedName = name;
            return Task.FromResult(Result.Success());
        }

        public Task<Result> StartContainerAsync(string idOrName, CancellationToken ct)
        {
            Calls.Add(("start", idOrName));
            return Task.FromResult(Result.Success());
        }

        public Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct)
        {
            Calls.Add(("stop", idOrName));
            return Task.FromResult(Result.Success());
        }

        public Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct)
        {
            Calls.Add(("rm-container", idOrName));
            return Task.FromResult(Result.Success());
        }

        public Task<Result> RemoveVolumeAsync(string name, CancellationToken ct)
        {
            Calls.Add(("rm-volume", name));
            return Task.FromResult(Result.Success());
        }

        public Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct)
        {
            Calls.Add(("exec", containerId));
            return Task.FromResult(Result<string>.Success(ExecStdout));
        }

        // stdout docker-exec (Д3: проба данных — present/absent по PG_VERSION).
        public string ExecStdout { get; set; } = "";

        public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct)
        {
            Calls.Add(("list-nodes", null));
            return Task.FromResult(Result<IReadOnlyList<DockerSwarmNode>>.Success(Nodes));
        }

        public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct)
        {
            Calls.Add(("create-service", spec));
            CreatedService = spec;
            return Task.FromResult(Result.Success());
        }

        public Task<Result> RemoveServiceAsync(string name, CancellationToken ct)
        {
            Calls.Add(("rm-service", name));
            return Task.FromResult(Result.Success());
        }

        public Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct)
        {
            Calls.Add(("list-services", namePrefix));
            var matched = Services
                .Where(n => n.StartsWith(namePrefix, StringComparison.Ordinal))
                .OrderBy(n => n, StringComparer.Ordinal)
                .ToList();
            return Task.FromResult(Result<IReadOnlyList<string>>.Success(matched));
        }

        public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerTask>>.Success([]));

        public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct)
        {
            Calls.Add(("busy-ports", null));
            return Task.FromResult(Result<IReadOnlySet<(string, int)>>.Success(Busy));
        }
    }

    // Фабрика-заглушка: раздаёт преднастроенные движки (перехват Create
    // с фиксацией hostAlias — namespace busy-портов, advertised-режим).
    private sealed class FakeFactory(IDockerEngine engine) : DockerEngineFactory
    {
        public readonly List<(string Endpoint, string? HostAlias)> Engines = [];

        public override IDockerEngine Create(string endpoint, string? hostAlias = null)
        {
            Engines.Add((endpoint, hostAlias));
            return engine;
        }
    }

    private static ShardTopology Topology(NodeAddress addr) => new(
        "shop", "shard1", "shop-shard1",
        new Dictionary<string, NodeAddress> { ["shard1a"] = addr });

    private static readonly NodeAddress Addr = new("h1", new NodePorts(15432, 18008, 16432));

    private static readonly InstallSecrets Secrets = new("su", "sb", "ba", "mv");

    private static readonly EtcdEndpoints Etcd = new(["http://etcd:2379"]);

    private static PlainClusterDriver NewPlainDriver(FakeEngine engine, string? advertisedHost = null)
        => new([new HostEndpoint("h1", "fake://h1")], new FakeFactory(engine), enableDoorman: true,
            advertisedHost: advertisedHost);

    // AAA: Ф7-live — имена нод НЕуникальны между кластерами одного docker-хоста
    // (pgw-canon-/pgw-canon10-/pgw-smoke- все с hostname/alias "shard1a"):
    // InspectNodesAsync исключает контейнеры ЧУЖИХ pgw-кластеров ДО матчинга —
    // иначе NodeMatcher видел чужие ноды как неоднозначность и пропускал ВСЁ
    // (adoption 0 находок → portalloc не переписан фактом → recreate на битых записях)
    [Fact]
    public async Task InspectNodes_SameNodeNamesAcrossClusters_FindsOnlyOwnCluster()
    {
        // Arrange: три кластера, в каждом контейнер с hostname/alias "shard1a".
        var engine = new FakeEngine
        {
            Containers =
            [
                new DockerContainer("id-canon", ["pgw-canon-shard1-shard1a"], "running", "img"),
                new DockerContainer("id-canon10", ["pgw-canon10-shard1-shard1a"], "running", "img"),
                new DockerContainer("id-smoke", ["pgw-smoke-shard1-shard1a"], "running", "img"),
            ],
            Inspects = new Dictionary<string, DockerContainerInspect>
            {
                ["id-canon"] = new("id-canon", "shard1a", ["shard1a"], [],
                    [new PortMap(5432, 15000), new PortMap(8008, 18000)]),
                ["id-canon10"] = new("id-canon10", "shard1a", ["shard1a"], [],
                    [new PortMap(5432, 15004), new PortMap(8008, 18004)]),
                ["id-smoke"] = new("id-smoke", "shard1a", ["shard1a"], [],
                    [new PortMap(5432, 15002), new PortMap(8008, 18002)]),
            },
        };
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.InspectNodesAsync("canon10", ["shard1a"], CancellationToken.None);

        // Assert: находка — ТОЛЬКО контейнер canon10 (факт 15004), чужие canon/smoke не мешают.
        result.IsSuccess.Should().BeTrue();
        var node = result.Value.Should().ContainSingle().Subject.Value;
        node.NodeName.Should().Be("shard1a");
        node.Object.Should().Be("pgw-canon10-shard1-shard1a");
        node.Pg.Should().Be(15004);
        node.Patroni.Should().Be(18004);
    }

    // AAA: Ф7-live — неоднозначность ВНУТРИ одного кластера по-прежнему → пропуск
    // (фильтр чужих pgw-* не ослабляет guard неоднозначности, spec §3.1)
    [Fact]
    public async Task InspectNodes_AmbiguousWithinSameCluster_StillSkips()
    {
        // Arrange: два живых контейнера ОДНОГО кластера претендуют на имя ноды.
        var engine = new FakeEngine
        {
            Containers =
            [
                new DockerContainer("id-a", ["pgw-canon10-shard1-shard1a"], "running", "img"),
                new DockerContainer("id-b", ["pgw-canon10-shard1-shard1a-alt"], "running", "img"),
            ],
            Inspects = new Dictionary<string, DockerContainerInspect>
            {
                ["id-a"] = new("id-a", "shard1a", ["shard1a"], [], [new PortMap(5432, 15004)]),
                ["id-b"] = new("id-b", "shard1a", ["shard1a"], [], [new PortMap(5432, 15005)]),
            },
        };
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.InspectNodesAsync("canon10", ["shard1a"], CancellationToken.None);

        // Assert: оба контейнера свои (pgw-canon10-*) → неоднозначность → безопасный пропуск.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEmpty();
    }

    // AAA: Ф7-live — фильтр кластера НЕ прячет внешние контейнеры усыновления
    // (AD1 ищет as-*/hc*, не pgw-*): исключаются только чужие pgw-<C'>-*
    [Fact]
    public async Task InspectNodes_ForeignNonPgwContainersOfOthers_RemainVisible()
    {
        // Arrange: внешний контейнер as-s2a (hostname s2a — усыновление AD1) +
        // чужой pgw-кластер с той же нодой s2a.
        var engine = new FakeEngine
        {
            Containers =
            [
                new DockerContainer("id-as", ["as-s2a"], "running", "img"),
                new DockerContainer("id-foreign", ["pgw-other-s2-s2a"], "running", "img"),
            ],
            Inspects = new Dictionary<string, DockerContainerInspect>
            {
                ["id-as"] = new("id-as", "s2a", ["s2a"], [], [new PortMap(5432, 15432)]),
                ["id-foreign"] = new("id-foreign", "s2a", ["s2a"], [], [new PortMap(5432, 15099)]),
            },
        };
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.InspectNodesAsync("demo", ["s2a"], CancellationToken.None);

        // Assert: внешний as-s2a виден (усыпление кластера demo), чужой pgw-other-* исключён.
        result.IsSuccess.Should().BeTrue();
        var node = result.Value.Should().ContainSingle().Subject.Value;
        node.Object.Should().Be("as-s2a");
        node.Pg.Should().Be(15432);
    }

    // AAA: advertised-режим (arch/16 advertised-правило, прецедент KafkaWorker):
    // факт КАНОНИЧЕСКОЙ ноды (pgw-<C>-*) несёт advertised-имя docker-хоста —
    // записи portalloc/dsn резолвимы КЛИЕНТАМИ (панель), внутреннее имя хоста
    // резолвится только контейнерами воркеров (extra_hosts) — пробы панели
    // по внутреннему имени уходили в DNS-таймаут
    [Fact]
    public async Task InspectNodes_CanonicalNode_AdvertisedHostInFact()
    {
        // Arrange: живой канонический контейнер; драйвер с advertised-хостом.
        var engine = new FakeEngine
        {
            Containers = [new DockerContainer("id1", ["pgw-canon10-shard1-shard1a"], "running", "img")],
            Inspects = new Dictionary<string, DockerContainerInspect>
            {
                ["id1"] = new("id1", "shard1a", ["shard1a"], [],
                    [new PortMap(5432, 15004), new PortMap(8008, 18004)]),
            },
        };
        var driver = NewPlainDriver(engine, advertisedHost: "host.docker.internal");

        // Act
        var result = await driver.InspectNodesAsync("canon10", ["shard1a"], CancellationToken.None);

        // Assert: находка несёт advertised-хост (факт пойдёт в portalloc).
        result.IsSuccess.Should().BeTrue();
        var node = result.Value.Should().ContainSingle().Subject.Value;
        node.Host.Should().Be("host.docker.internal");
        node.Pg.Should().Be(15004);
        node.Patroni.Should().Be(18004);
    }

    // AAA: внешние находки усыновления (object) advertised-имя НЕ получают —
    // их адресация операторская (R9-симметрия: HostMap/композ-имена внешнего контура)
    [Fact]
    public async Task InspectNodes_ExternalAdoptionNode_KeepsDockerHostName()
    {
        // Arrange: внешний контейнер as-s2a (усыпление demo) при advertised-драйвере.
        var engine = new FakeEngine
        {
            Containers = [new DockerContainer("id-as", ["as-s2a"], "running", "img")],
            Inspects = new Dictionary<string, DockerContainerInspect>
            {
                ["id-as"] = new("id-as", "s2a", ["s2a"], [], [new PortMap(5432, 15432)]),
            },
        };
        var driver = NewPlainDriver(engine, advertisedHost: "host.docker.internal");

        // Act
        var result = await driver.InspectNodesAsync("demo", ["s2a"], CancellationToken.None);

        // Assert: host находки — docker-имя хоста, advertised не подменяется.
        var node = result.Value.Should().ContainSingle().Subject.Value;
        node.Host.Should().Be("h1");
        node.Object.Should().Be("as-s2a");
    }

    // AAA: advertised-адрес ноды (запись portalloc) резолвится в единственный
    // движок таблицы Hosts; env НОВОГО контейнера несёт advertised-хост —
    // lease-демон мастер-ключа согласован с portalloc по хост-части
    [Fact]
    public async Task EnsureNode_AdvertisedHostAddress_ResolvesEngineAndStampsEnv()
    {
        // Arrange: адрес ноды с advertised-хостом (как в portalloc advertised-режима).
        var engine = new FakeEngine();
        var driver = NewPlainDriver(engine, advertisedHost: "host.docker.internal");
        var addr = new NodeAddress("host.docker.internal", new NodePorts(15432, 18008, 16432));

        // Act
        var result = await driver.EnsureNodeAsync(
            Topology(addr), "shard1a", addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);

        // Assert: движок найден (create прошёл), PGW_NODE_HOST = advertised.
        result.IsSuccess.Should().BeTrue();
        engine.CreatedSpec.Should().NotBeNull();
        engine.CreatedSpec!.Env["PGW_NODE_HOST"].Should().Be("host.docker.internal");
    }

    // AAA: advertised-режим — планировщик видит advertised-имя хоста (кандидаты
    // аллокатора и busy-множество в одном namespace), hostAlias движка
    // (BusyPorts-кортежи) — тоже advertised
    [Fact]
    public async Task GetHosts_AdvertisedHost_PlannerAndBusyNamespaceAdvertised()
    {
        // Arrange: драйвер с advertised-хостом; фабрика фиксирует hostAlias движков.
        var engine = new FakeEngine();
        var factory = new FakeFactory(engine);
        var driver = new PlainClusterDriver(
            [new HostEndpoint("h1", "fake://h1")], factory, enableDoorman: true,
            advertisedHost: "host.docker.internal");

        // Act
        var hosts = await driver.GetHostsAsync(CancellationToken.None);

        // Assert: HostInfo — advertised-имя; движок создан с hostAlias=advertised.
        hosts.IsSuccess.Should().BeTrue();
        hosts.Value.Should().ContainSingle().Which.Name.Should().Be("host.docker.internal");
        factory.Engines.Should().ContainSingle().Which.HostAlias.Should().Be("host.docker.internal");
    }

    // AAA: Д3 — проба данных ноды: docker-exec test -f PG_VERSION → Present/Absent;
    // контейнера нет → Unknown (транспорт ≠ доказательство утраты, arch/14 R11)
    [Fact]
    public async Task NodeDataPresence_StdoutPresent_Present()
    {
        // Arrange: running-контейнер ноды; exec вернул "present" (PG_VERSION есть).
        var engine = new FakeEngine
        {
            Containers = [new DockerContainer("id1", ["pgw-shop-shard1-shard1a"], "running", "img")],
            ExecStdout = "present",
        };
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.NodeDataPresenceAsync("shop", "shard1", "shard1a", CancellationToken.None);

        // Assert: данные доказанно есть.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(DataPresence.Present);
    }

    [Fact]
    public async Task NodeDataPresence_StdoutAbsent_Absent()
    {
        // Arrange: контейнер жив, PG_VERSION нет (volume пуст — доказанная утрата).
        var engine = new FakeEngine
        {
            Containers = [new DockerContainer("id1", ["pgw-shop-shard1-shard1a"], "running", "img")],
            ExecStdout = "absent",
        };
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.NodeDataPresenceAsync("shop", "shard1", "shard1a", CancellationToken.None);

        // Assert: данных доказанно нет.
        result.Value.Should().Be(DataPresence.Absent);
    }

    [Fact]
    public async Task NodeDataPresence_NoRunningContainer_Unknown()
    {
        // Arrange: контейнера нет — утрата НЕ доказана.
        var engine = new FakeEngine { Containers = [] };
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.NodeDataPresenceAsync("shop", "shard1", "shard1a", CancellationToken.None);

        // Assert: Unknown — чистка scope запрещена.
        result.Value.Should().Be(DataPresence.Unknown);
    }

    [Fact]
    public async Task EnsureNode_ExistingContainer_DoesNotRecreate()
    {
        // Arrange — контейнер ноды уже есть с ПЛАНОМ портов (повторный тик/пересоздание после сбоя)
        var engine = new FakeEngine
        {
            Containers = [new DockerContainer("id1", ["pgw-shop-shard1-shard1a"], "running", "img")],
            Inspects = new Dictionary<string, DockerContainerInspect>
            {
                ["id1"] = new("id1", "shard1a", [], [],
                    [new PortMap(5432, 15432), new PortMap(8008, 18008), new PortMap(6432, 16432)]),
            },
        };
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.EnsureNodeAsync(Topology(Addr), "shard1a", Addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);

        // Assert: ни create, ни start — только сверка списком и инспектом
        result.IsSuccess.Should().BeTrue();
        engine.Calls.Should().NotContain(c => c.Call == "create");
        engine.Calls.Should().NotContain(c => c.Call == "start");
    }

    // AAA: B — контейнер на ЧУЖИХ портах (portalloc потерян и выделен заново):
    // расхождение биндингов → stop+rm+create+start с планом (volume жив)
    [Fact]
    public async Task EnsureNode_PortDrift_RecreatesContainer()
    {
        // Arrange: контейнер на ЧУЖИХ портах (сценарий: portalloc потерян и выделен заново).
        var engine = new FakeEngine
        {
            Containers = [new DockerContainer("id1", ["pgw-shop-shard1-shard1a"], "running", "img")],
            Inspects = new Dictionary<string, DockerContainerInspect>
            {
                ["id1"] = new("id1", "shard1a", [], [],
                    [new PortMap(5432, 15111), new PortMap(8008, 18111), new PortMap(6432, 16611)]),
            },
        };
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.EnsureNodeAsync(Topology(Addr), "shard1a", Addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);

        // Assert: stop → create → start с планом портов (PROVISIONING-фаза, volume жив).
        result.IsSuccess.Should().BeTrue();
        var calls = engine.Calls.Select(c => c.Call).ToList();
        calls.Should().ContainInOrder("stop", "create", "start");
        engine.CreatedSpec!.Ports.Should().Contain(new PortMap(5432, 15432));
    }

    // AAA: B — отсутствие ожидаемого биндинга = расхождение → пересоздание
    [Fact]
    public async Task EnsureNode_MissingBinding_RecreatesContainer()
    {
        // Arrange: контейнер без 5432-биндинга — «бесполезный» контейнер.
        var engine = new FakeEngine
        {
            Containers = [new DockerContainer("id1", ["pgw-shop-shard1-shard1a"], "running", "img")],
            Inspects = new Dictionary<string, DockerContainerInspect>
            {
                ["id1"] = new("id1", "shard1a", [], [], [new PortMap(8008, 18008)]),
            },
        };
        var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new FakeFactory(engine), enableDoorman: false);

        // Act
        await driver.EnsureNodeAsync(Topology(Addr), "shard1a", Addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);

        // Assert: пересоздание (отсутствие ожидаемого биндинга = расхождение).
        engine.Calls.Select(c => c.Call).Should().Contain("create");
    }

    // AAA: B/R9 — усыновлённая нода (object) — чужой контейнер: сверка и
    // пересоздание неприменимы
    [Fact]
    public async Task EnsureNode_AdoptedObjectNode_NeverTouched()
    {
        // Arrange: усыновлённая нода (object) — чужой контейнер, сверка неприменима (R9).
        var engine = new FakeEngine
        {
            Containers = [new DockerContainer("id1", ["foreign-1"], "running", "img")],
            Inspects = new Dictionary<string, DockerContainerInspect>
            {
                ["id1"] = new("id1", "shard1a", [], [], [new PortMap(5432, 15999)]),
            },
        };
        var driver = NewPlainDriver(engine);
        var addr = new NodeAddress("h1", new NodePorts(15432, 18008, 16432), Object: "foreign-1");

        // Act
        var result = await driver.EnsureNodeAsync(Topology(addr), "shard1a", addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);

        // Assert: никаких stop/remove/create.
        result.IsSuccess.Should().BeTrue();
        engine.Calls.Select(c => c.Call).Should().NotContain("stop");
        engine.Calls.Select(c => c.Call).Should().NotContain("create");
    }

    [Fact]
    public async Task EnsureNode_NewContainer_EnvFromBuildersAndVolumeAndPorts()
    {
        // Arrange
        var engine = new FakeEngine();
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.EnsureNodeAsync(Topology(Addr), "shard1a", Addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);

        // Assert: имя pgw-<C>-<X>-<n>; env из SpiloEnvBuilder + PGW_NODE_HOST;
        // volume -data; publish тройка портов; затем start
        result.IsSuccess.Should().BeTrue();
        engine.CreatedName.Should().Be("pgw-shop-shard1-shard1a");
        engine.CreatedSpec.Should().NotBeNull();
        var spec = engine.CreatedSpec!;
        spec.VolumeName.Should().Be("pgw-shop-shard1-shard1a-data");
        spec.VolumeDest.Should().Be("/home/postgres/pgdata"); // дефолтный PGDATA-корень Spilo
        spec.Hostname.Should().Be("shard1a");
        spec.Env["SCOPE"].Should().Be("shop-shard1");
        spec.Env["ETCD3_HOSTS"].Should().Be("etcd:2379"); // Patroni: host:port без scheme
        spec.Env["PGW_NODE_HOST"].Should().Be("h1");
        spec.Env["DOORMAN_CONFIG"].Should().Contain("pool_mode = \"transaction\"");
        // HAPROXY_CONFIG не передаётся: PG и HAProxy конфликтуют на :5432 (Д4).
        spec.Env.Should().NotContainKey("HAPROXY_CONFIG");
        spec.Ports.Should().BeEquivalentTo(
        [
            new PortMap(5432, 15432),
            new PortMap(8008, 18008),
            new PortMap(6432, 16432),
        ]);
        engine.Calls.Select(c => c.Call).Should().Equal("ensure-network", "list-containers", "create", "start");
    }

    [Fact]
    public async Task EnsureNode_DoormanDisabled_NoDoormanPortAndConfig()
    {
        // Arrange — флаг EnableDoorman=false (R1: узел без пулера, компромисс стенда)
        var engine = new FakeEngine();
        var driver = new PlainClusterDriver(
            [new HostEndpoint("h1", "fake://h1")], new FakeFactory(engine), enableDoorman: false);

        // Act
        var result = await driver.EnsureNodeAsync(Topology(Addr), "shard1a", Addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);

        // Assert: порт 6432 не публикуется, DOORMAN_CONFIG не генерируется
        result.IsSuccess.Should().BeTrue();
        engine.CreatedSpec.Should().NotBeNull();
        var spec = engine.CreatedSpec!;
        spec.Ports.Should().NotContain(p => p.ContainerPort == 6432);
        spec.Env.Should().NotContainKey("DOORMAN_CONFIG");
        spec.Env.Should().NotContainKey("PGW_DOORMAN_PORT");
    }

    [Fact]
    public async Task RemoveNode_StopsRemovesContainerAndVolume()
    {
        // Arrange
        var engine = new FakeEngine();
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.RemoveNodeAsync("shop", "shard1", "shard1a", CancellationToken.None);

        // Assert: stop → rm(force) → volume rm; никаких других вызовов
        result.IsSuccess.Should().BeTrue();
        engine.Calls.Select(c => c.Call).Should().Equal("stop", "rm-container", "rm-volume");
        engine.Calls[1].Arg.Should().Be("pgw-shop-shard1-shard1a");
        engine.Calls[2].Arg.Should().Be("pgw-shop-shard1-shard1a-data");
    }

    [Fact]
    public async Task StopNode_StopsOnly_NoRemoveNoVolume()
    {
        // Arrange — карантин вернувшегося шарда (E3): данные на месте
        var engine = new FakeEngine();
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.StopNodeAsync("shop", "shard1", "shard1a", CancellationToken.None);

        // Assert: только stop
        result.IsSuccess.Should().BeTrue();
        engine.Calls.Select(c => c.Call).Should().Equal("stop");
        engine.Calls[0].Arg.Should().Be("pgw-shop-shard1-shard1a");
    }

    [Fact]
    public async Task GetHosts_UsedSlotsByContainerCount()
    {
        // Arrange — на хосте два pgw-контейнера
        var engine = new FakeEngine
        {
            Containers =
            [
                new DockerContainer("a", ["pgw-shop-shard1-shard1a"], "running", "i"),
                new DockerContainer("b", ["pgw-shop-shard1-shard1b"], "exited", "i"),
                new DockerContainer("c", ["foreign"], "running", "i"),
            ],
        };
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.GetHostsAsync(CancellationToken.None);

        // Assert: UsedSlots = число контейнеров префикса (foreign не считается)
        result.IsSuccess.Should().BeTrue();
        var host = result.Value.Should().ContainSingle().Subject;
        host.Name.Should().Be("h1");
        host.UsedSlots.Should().Be(2);
    }

    [Fact]
    public async Task Swarm_EnsureNode_ServiceConstraintAndPublishPorts()
    {
        // Arrange — swarm-нода h1 существует; драйвер строит сервис с constraint
        var engine = new FakeEngine
        {
            Nodes = [new DockerSwarmNode("node-id-1", "h1", "ready", 0)],
        };
        var driver = new SwarmClusterDriver("fake://manager", new FakeFactory(engine), enableDoorman: true);

        // Act
        var result = await driver.EnsureNodeAsync(Topology(Addr), "shard1a", Addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);

        // Assert: ServiceSpec с constraint по id найденной ноды; шаблон — как у plain
        result.IsSuccess.Should().BeTrue();
        engine.CreatedService.Should().NotBeNull();
        var spec = engine.CreatedService!;
        spec.Name.Should().Be("pgw-shop-shard1-shard1a");
        spec.NodeConstraint.Should().Be("node-id-1");
        spec.Template.Ports.Should().Contain(p => p.ContainerPort == 5432 && p.HostPort == 15432);
        spec.Template.VolumeName.Should().Be("pgw-shop-shard1-shard1a-data");
    }

    [Fact]
    public async Task EnsureNode_WithResources_LimitsReachContainerSpec()
    {
        // Arrange — заявка request_* (rework №5): 2 ядра/8GiB должны дойти до
        // ContainerSpec (драйвер транспортирует; упаковку в тело create
        // проверяют DockerEngineTests)
        var engine = new FakeEngine();
        var driver = NewPlainDriver(engine);
        var resources = new NodeResources(CpuCores: 2, MemoryBytes: 8L * 1024 * 1024 * 1024);

        // Act
        var result = await driver.EnsureNodeAsync(Topology(Addr), "shard1a", Addr, Secrets, Etcd, resources, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        engine.CreatedSpec!.CpuCores.Should().Be(2);
        engine.CreatedSpec!.MemoryBytes.Should().Be(8_589_934_592);
    }

    [Fact]
    public async Task Swarm_GetHosts_FromReadyNodes()
    {
        // Arrange
        var engine = new FakeEngine
        {
            Nodes =
            [
                new DockerSwarmNode("n1", "h1", "ready", 3),
                new DockerSwarmNode("n2", "h2", "down", 1),
            ],
        };
        var driver = new SwarmClusterDriver("fake://manager", new FakeFactory(engine), enableDoorman: true);

        // Act
        var result = await driver.GetHostsAsync(CancellationToken.None);

        // Assert: только ready-ноды, UsedSlots = running tasks
        result.IsSuccess.Should().BeTrue();
        var host = result.Value.Should().ContainSingle().Subject;
        host.Name.Should().Be("h1");
        host.UsedSlots.Should().Be(3);
    }

    [Fact]
    public async Task ListNodeObjects_ReturnsPrefixedNames()
    {
        // Arrange
        var engine = new FakeEngine
        {
            Containers =
            [
                new DockerContainer("a", ["pgw-shop-shard1-shard1a"], "running", "i"),
                new DockerContainer("b", ["pgw-shop-shard2-shard2a"], "running", "i"),
                new DockerContainer("c", ["pgw-other-shard1-shard1a"], "running", "i"),
            ],
        };
        var driver = NewPlainDriver(engine);

        // Act
        var result = await driver.ListNodeObjectsAsync("shop", CancellationToken.None);

        // Assert: имена pgw-<C>-*, детерминированный порядок
        result.Value.Should().Equal("pgw-shop-shard1-shard1a", "pgw-shop-shard2-shard2a");
    }

    [Fact]
    public async Task Swarm_ListNodeObjects_ReturnsPrefixedServiceNames()
    {
        // Arrange — объекты нод кластера в swarm = сервисы (rework №4): drift-
        // сверка надзора и guard D2 видят живые сервисы, осцилляция
        // PROVISIONING→RUNNING исключена
        var engine = new FakeEngine
        {
            Services =
            [
                "pgw-shop-shard1-shard1a",
                "pgw-shop-shard2-shard2a",
                "pgw-other-shard1-shard1a", // чужой кластер
            ],
        };
        var driver = new SwarmClusterDriver("fake://manager", new FakeFactory(engine), enableDoorman: true);

        // Act
        var result = await driver.ListNodeObjectsAsync("shop", CancellationToken.None);

        // Assert: только сервисы кластера pgw-shop-*
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Equal("pgw-shop-shard1-shard1a", "pgw-shop-shard2-shard2a");
        engine.Calls.Should().Contain(c => c.Call == "list-services" && c.Arg!.Equals("pgw-shop-"));
    }
}
