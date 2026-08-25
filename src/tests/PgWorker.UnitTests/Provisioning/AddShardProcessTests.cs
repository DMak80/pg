using System.Net;
using System.Text;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;

namespace PgWorker.UnitTests.Provisioning;

// AddShardProcess A0–A6 (t06 spec §5.2; arch/14 §5 G): подъём ОТДЕЛЬНОГО
// пустого шарда в Active-кластере. Главный ассерт границы §2.1 — SQL не
// содержит CREATE SCHEMA bucket_*, routing/status не пишутся.
public class AddShardProcessTests
{
    private static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "app-pw", "adm-pw", "mov-pw");
    private static readonly EtcdEndpoints EtcdEndp = new(["http://etcd:2379"]);
    private const string Ep = "http://etcd:2379";

    // Сид Active-кластера: config без state, shard1/shard2 подняты (dsn +
    // service initialize/leader), routing 0..5, portalloc живых нод.
    private static void SeedActiveCluster(Fakes.FakeEtcd etcd)
    {
        etcd.Seed("/clusters/shop/config",
            """{"buckets":6,"dbname":"shop","created_unix":1755900000}""");
        etcd.Seed("/clusters/shop/shards/shard1/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard1/dsn", "host=h1,h2 port=15000,15000 dbname=shop user=bucket_admin");
        etcd.Seed("/clusters/shop/shards/shard2/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard2/nodes/shard2a/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard2/nodes/shard2b/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard2/dsn", "host=h1,h2 port=15001,15001 dbname=shop user=bucket_admin");
        for (var i = 0; i < 6; i++)
            etcd.Seed($"/clusters/shop/buckets/routing/bucket_{i}", i % 2 == 0 ? "shard1" : "shard2");
        etcd.Seed("/service/shop-shard1/initialize", "7403705125687833961");
        etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a","poll_queued_commands":0}""");
        etcd.Seed("/service/shop-shard2/initialize", "7403705125687833962");
        etcd.Seed("/service/shop-shard2/leader", """{"name":"shard2a","poll_queued_commands":0}""");

        var existing = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
            ["shard1/shard1b"] = new("h2", new NodePorts(15000, 18000, 16500)),
            ["shard2/shard2a"] = new("h1", new NodePorts(15001, 18001, 16501)),
            ["shard2/shard2b"] = new("h2", new NodePorts(15001, 18001, 16501)),
        };
        etcd.Seed("/pgworker/portalloc/shop", Portalloc.Serialize(existing));
    }

    // Add-декларация панели (§4.1): replicas + nodes NOT_INITIALIZED + request_*.
    private static void SeedAddDeclaration(Fakes.FakeEtcd etcd)
    {
        etcd.Seed("/clusters/shop/shards/shard3/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard3/nodes/shard3a/state", "NOT_INITIALIZED");
        etcd.Seed("/clusters/shop/shards/shard3/nodes/shard3b/state", "NOT_INITIALIZED");
        etcd.Seed("/service/shop-shard3/request_cpu", "2");
        etcd.Seed("/service/shop-shard3/request_mem", "4Gi");
        etcd.Seed("/service/shop-shard3/request_disk", "10Gi");
    }

    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == "shop");
    }

    private static ShardProbe Probe(Func<int, HttpResponseMessage> respondByPort)
        => new(new HttpClient(new FakeHandler(r => respondByPort(r.RequestUri!.Port))));

    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(responder(request));
    }

    private static HttpResponseMessage Patroni(string masterName) => new()
    {
        StatusCode = HttpStatusCode.OK,
        Content = new StringContent(
            $$"""{"members":[{"name":"{{masterName}}","role":"master","state":"running"},{"name":"other","role":"replica","state":"streaming"}]}""",
            Encoding.UTF8,
            "application/json"),
    };

    private static HttpResponseMessage DeadPatroni() => new(HttpStatusCode.InternalServerError);

    private sealed record Rig(Fakes.FakeEtcd Etcd, Fakes.FakeDriver Driver, Fakes.FakeSql Sql,
        ClaimStore Claims, WorkJournal Journal, AddShardProcess Process);

    private static async Task<Rig> NewRig(
        Func<int, HttpResponseMessage> patroniResponse,
        PlacementOptions? opts = null,
        IReadOnlySet<(string Host, int Port)>? busyPorts = null)
    {
        var etcd = new Fakes.FakeEtcd();
        SeedActiveCluster(etcd);
        SeedAddDeclaration(etcd);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var driver = new Fakes.FakeDriver();
        if (busyPorts is not null)
            driver.BusyPorts = busyPorts;
        var sql = new Fakes.FakeSql();
        var process = new AddShardProcess(
            etcd, [Ep], driver, sql, Probe(patroniResponse), claims, journal,
            opts ?? new PlacementOptions(15000, 15100, PatroniBootSec: 600),
            Secrets, EtcdEndp, snapshot: null);
        return new Rig(etcd, driver, sql, claims, journal, process);
    }

    // Порты живых нод (docker видит публикации контейнеров) — новые ноды уходят выше.
    private static IReadOnlySet<(string Host, int Port)> LiveNodePorts() => new HashSet<(string, int)>
    {
        ("h1", 15000), ("h2", 15000), ("h1", 15001), ("h2", 15001),
    };

    [Fact]
    public async Task Tick_IncompleteDeclaration_WaitingKeys_NoMutations()
    {
        // Arrange — панель дописала только replicas, nodes-ключей нет
        var etcd = new Fakes.FakeEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed("/clusters/shop/shards/shard3/replicas", "2");
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var driver = new Fakes.FakeDriver();
        var process = new AddShardProcess(
            etcd, [Ep], driver, new Fakes.FakeSql(), Probe(_ => DeadPatroni()),
            claims, journal, new PlacementOptions(15000, 15100, 600), Secrets, EtcdEndp, snapshot: null);

        // Act
        var outcome = await process.TickAsync(await Snapshot(etcd), "shard3", CancellationToken.None);

        // Assert — ждём доустойчивости ключей; мутаций нет (docker/portalloc/nodes)
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        (await journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("waiting-keys");
        driver.EnsuredNodes.Should().BeEmpty();
        etcd.Store.Keys.Should().NotContain(k =>
            k.StartsWith("/clusters/shop/shards/shard3/", StringComparison.Ordinal)
            && k != "/clusters/shop/shards/shard3/replicas");
        etcd.Store["/pgworker/portalloc/shop"].Value.Should().NotContain("shard3/");
    }

    [Fact]
    public async Task Tick_ScopeTaken_PermanentError_DeclarationStays()
    {
        // Arrange — scope /service/shop-shard3 занят живым чужим Patroni-кластером
        var rig = await NewRig(_ => DeadPatroni());
        rig.Etcd.Seed("/service/shop-shard3/initialize", "7403705125687833999");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);

        // Assert — перманентная ошибка (коллизия имён); декларация жива, docker не трогаем
        outcome.IsSuccess.Should().BeFalse();
        outcome.Error!.Message.Should().Contain("shop-shard3");
        var work = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        work.LastError.Should().Contain("shop-shard3");
        rig.Driver.EnsuredNodes.Should().BeEmpty();
        rig.Etcd.Store.ContainsKey("/clusters/shop/shards/shard3/replicas").Should().BeTrue();
        rig.Etcd.Store.ContainsKey("/clusters/shop/shards/shard3/nodes/shard3a/state").Should().BeTrue();
    }

    [Fact]
    public async Task Tick_AlreadyMarkedToRemove_BlockedRemoving_NoMutations()
    {
        // Arrange — add-декларация + маркер демонтажа (шард в обоих списках §5.1)
        var rig = await NewRig(_ => DeadPatroni());
        rig.Etcd.Seed("/clusters/shop/shards/shard3/state", "TO_REMOVE");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);

        // Assert — домен RemoveShardProcess; add ничего не делает
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("blocked-removing");
        rig.Driver.EnsuredNodes.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_ClusterToRemoveMidFlight_Aborted_NoMutations()
    {
        // Arrange — R6: кластер переведён в TO_REMOVE до тика add'а
        var rig = await NewRig(_ => DeadPatroni());
        rig.Etcd.Seed("/clusters/shop/config",
            """{"buckets":6,"dbname":"shop","created_unix":1755900000,"state":"TO_REMOVE"}""");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);

        // Assert — безопасный abort: кластер снесёт deprovisioning
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("aborted");
        rig.Driver.EnsuredNodes.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_FullDeclaration_EnsureNodesThenInProgress()
    {
        // Arrange — полный сид; Patroni глухой (первый тик только поднимает ноды)
        var rig = await NewRig(_ => DeadPatroni(), busyPorts: LiveNodePorts());

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);

        // Assert — ноды созданы в порядке имени, PROVISIONING; portalloc merge
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Driver.EnsuredNodes.Should().Equal("shard3/shard3a", "shard3/shard3b");
        rig.Etcd.Store["/clusters/shop/shards/shard3/nodes/shard3a/state"].Value.Should().Be("PROVISIONING");
        rig.Etcd.Store["/clusters/shop/shards/shard3/nodes/shard3b/state"].Value.Should().Be("PROVISIONING");
        var portalloc = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
        portalloc.Should().Contain("shard3/shard3a");
        portalloc.Should().Contain("shard1/shard1a");
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("waiting-patroni");
    }

    [Fact]
    public async Task Tick_PatroniAlive_BootStrapsEmptyShardAndRegistersDsn()
    {
        // Arrange — Patroni шарда поднялся (initialize + leader + REST по портам нод)
        var rig = await NewRig(port => port == 18002 ? Patroni("shard3a") : DeadPatroni(),
            busyPorts: LiveNodePorts());
        rig.Etcd.Seed("/service/shop-shard3/initialize", "7403705125687833998");
        rig.Etcd.Seed("/service/shop-shard3/leader", """{"name":"shard3a","poll_queued_commands":0}""");
        var routingBefore = rig.Etcd.Store
            .Where(p => p.Key.StartsWith("/clusters/shop/buckets/routing/", StringComparison.Ordinal))
            .OrderBy(p => p.Key).Select(p => p.Value.Value).ToList();

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);

        // Assert — шард поднят ПУСТЫМ: БД/роли есть, схем бакетов НЕТ, routing жив
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Sql.EnsuredDatabases.Should().Contain(
            ("Host=h1;Port=15002;Database=postgres;Username=postgres;Password=su-pw;SSL Mode=Require;Trust Server Certificate=true", "shop"));
        rig.Sql.Scalars.Should().Contain(s => s.Sql.Contains("CREATE ROLE \"app\""));
        rig.Sql.Scalars.Should().Contain(s => s.Sql.Contains("CREATE ROLE \"bucket_admin\""));
        rig.Sql.Executed.Should().NotContain(e => e.Sql.Contains("CREATE SCHEMA bucket_"));
        rig.Etcd.Store["/clusters/shop/shards/shard3/dsn"].Value.Should()
            .Be("host=h1,h2 port=15002,15002 dbname=shop user=bucket_admin password=adm-pw");
        rig.Etcd.Store["/clusters/shop/shards/shard3/nodes/shard3a/state"].Value.Should().Be("RUNNING");
        rig.Etcd.Store["/clusters/shop/shards/shard3/nodes/shard3b/state"].Value.Should().Be("RUNNING");
        var routingAfter = rig.Etcd.Store
            .Where(p => p.Key.StartsWith("/clusters/shop/buckets/routing/", StringComparison.Ordinal))
            .OrderBy(p => p.Key).Select(p => p.Value.Value).ToList();
        routingAfter.Should().Equal(routingBefore);
        rig.Etcd.Store.Keys.Should().NotContain(k => k.Contains("/buckets/status/"));
    }

    [Fact]
    public async Task Tick_DsnAlreadyWritten_DoneIdempotent()
    {
        // Arrange — шард уже зарегистрирован (dsn записан ранее)
        var rig = await NewRig(_ => DeadPatroni());
        rig.Etcd.Seed("/clusters/shop/shards/shard3/dsn", "host=h1,h2 port=15002,15002 dbname=shop user=bucket_admin password=adm-pw");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);

        // Assert — Done без мутаций (надзор видит шард по dsn)
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Driver.EnsuredNodes.Should().BeEmpty();
        rig.Sql.Executed.Should().BeEmpty();
        rig.Sql.EnsuredDatabases.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_RerunAfterPartial_ConvergesToSameState()
    {
        // Arrange — первый тик с глухим Patroni (ноды PROVISIONING, portalloc записан)
        var rig = await NewRig(_ => DeadPatroni(), busyPorts: LiveNodePorts());
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);
        var firstDsn = "host=h1,h2 port=15002,15002 dbname=shop user=bucket_admin password=adm-pw";

        // Act — Patroni ожил; второй тик по СВЕЖЕМУ снапшоту доводит до dsn
        rig.Etcd.Seed("/service/shop-shard3/initialize", "7403705125687833998");
        rig.Etcd.Seed("/service/shop-shard3/leader", """{"name":"shard3a","poll_queued_commands":0}""");
        var alive = new AddShardProcess(
            rig.Etcd, [Ep], rig.Driver, rig.Sql,
            Probe(port => port == 18002 ? Patroni("shard3a") : DeadPatroni()),
            rig.Claims, rig.Journal, new PlacementOptions(15000, 15100, 600), Secrets, EtcdEndp, snapshot: null);
        var outcome = await alive.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);

        // Assert — детерминизм multi-host: dsn тот же; схем/routing-мутаций нет
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Store["/clusters/shop/shards/shard3/dsn"].Value.Should().Be(firstDsn);
        rig.Etcd.Store["/clusters/shop/shards/shard3/nodes/shard3a/state"].Value.Should().Be("RUNNING");
        rig.Sql.Executed.Should().NotContain(e => e.Sql.Contains("CREATE SCHEMA bucket_"));
        rig.Etcd.Store.Keys.Should().NotContain(k => k.Contains("/buckets/status/"));
    }

    [Fact]
    public async Task Tick_PortRangeExhausted_LastErrorMentionsPortRange()
    {
        // Arrange — узкий диапазон (один base) занят живой нодой: тройку не взять
        var rig = await NewRig(_ => DeadPatroni(),
            opts: new PlacementOptions(15000, 15001, 600),
            busyPorts: new HashSet<(string, int)> { ("h1", 15000) });

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);

        // Assert — перманентная подсказка оператору; декларация жива, docker не трогаем
        outcome.IsSuccess.Should().BeFalse();
        outcome.Error!.Message.Should().Contain("расширьте PortRange");
        var work = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        work.Phase.Should().Be("planning");
        work.LastError.Should().Contain("расширьте PortRange");
        rig.Driver.EnsuredNodes.Should().BeEmpty();
        rig.Etcd.Store.ContainsKey("/clusters/shop/shards/shard3/replicas").Should().BeTrue();

        // Act 2 — повторный тик с теми же порогами: тот же отказ (ретраи тиками)
        var again = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);
        again.IsSuccess.Should().BeFalse();
        again.Error!.Message.Should().Contain("расширьте PortRange");
    }
}
