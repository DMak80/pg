using PgWorker.Core.Model;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;

namespace PgWorker.UnitTests.Provisioning;

// RemoveShardProcess S0–S4 (t06 spec §5.3; arch/14 §5 H): таблица guard'ов
// G1–G7, «сначала docker, потом etcd», чистка ключей/portalloc/журнала эвакуации.
public class RemoveShardProcessTests
{
    private const string Ep = "http://etcd:2379";

    // Базовый сид: Active-кластер shop, shard1 (2 ноды RUNNING, dsn, ПОМЕЧЕН,
    // все бакеты ушли на shard2), portalloc обоих шардов, журнал эвакуации.
    private static Fakes.FakeEtcd SeedBase()
    {
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/config",
            """{"buckets":3,"dbname":"shop","created_unix":1755900000}""");
        etcd.Seed("/clusters/shop/shards/shard1/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard1/dsn", "host=h1,h2 port=15000,15000 dbname=shop user=bucket_admin");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard1/state", "TO_REMOVE");
        etcd.Seed("/clusters/shop/shards/shard2/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard2/dsn", "host=h1,h2 port=15001,15001 dbname=shop user=bucket_admin");
        etcd.Seed("/clusters/shop/shards/shard2/nodes/shard2a/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard2/nodes/shard2b/state", "RUNNING");
        for (var i = 0; i < 3; i++)
            etcd.Seed($"/clusters/shop/buckets/routing/bucket_{i}", "shard2"); // shard1 ПУСТ
        etcd.Seed("/service/shop-shard1/initialize", "7403705125687833961");
        etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a"}""");
        etcd.Seed("/service/shop-shard1/request_cpu", "2");
        etcd.Seed("/service/shop-shard2/initialize", "7403705125687833962");
        etcd.Seed("/service/shop-shard2/leader", """{"name":"shard2a"}""");
        etcd.Seed("/pgworker/portalloc/shop", """{"shard1/shard1a":{"host":"h1","pg":15000,"patroni":18000,"doorman":16500},"shard1/shard1b":{"host":"h2","pg":15000,"patroni":18000,"doorman":16500},"shard2/shard2a":{"host":"h1","pg":15001,"patroni":18001,"doorman":16501},"shard2/shard2b":{"host":"h2","pg":15001,"patroni":18001,"doorman":16501}}""");
        etcd.Seed("/pgworker/evacuations/shop/shard1",
            """{"buckets":{"0":"shard2"},"reason":"shard-dead","evacuated_unix":1,"state":"DONE","returned_unix":null}""");
        return etcd;
    }

    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == "shop");
    }

    private sealed record Rig(Fakes.FakeEtcd Etcd, Fakes.FakeDriver Driver, ClaimStore Claims,
        WorkJournal Journal, RemoveShardProcess Process, List<string> Puts);

    private static async Task<Rig> NewRig(Fakes.FakeEtcd? etcd = null, Fakes.FakeDriver? driver = null)
    {
        var usedEtcd = etcd ?? SeedBase();
        var puts = new List<string>();
        usedEtcd.OnPut = puts.Add;
        var claims = new ClaimStore([Ep], usedEtcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        var journal = new WorkJournal(usedEtcd, [Ep]);
        var usedDriver = driver ?? new Fakes.FakeDriver
        {
            NodeObjects = ["pgw-shop-shard1-shard1a", "pgw-shop-shard1-shard1b"],
        };
        var process = new RemoveShardProcess(usedEtcd, [Ep], usedDriver, claims, journal, snapshot: null);
        return new Rig(usedEtcd, usedDriver, claims, journal, process, puts);
    }

    [Theory]
    [InlineData("G2")] [InlineData("G3")] [InlineData("G5")]
    [InlineData("G6")] [InlineData("G7")]
    public async Task Tick_GuardBlocked_MarkerStays_NoDockerMutations(string guardId)
    {
        // Arrange — базовый сид + мутация под конкретный guard
        var etcd = SeedBase();
        switch (guardId)
        {
            case "G2": // шард не заявлен: ключей replicas/dsn/nodes нет, маркер жив
                foreach (var key in etcd.Store.Keys.Where(k =>
                             k.StartsWith("/clusters/shop/shards/shard1/", StringComparison.Ordinal)
                             && k != "/clusters/shop/shards/shard1/state").ToList())
                    etcd.Store.Remove(key);
                break;
            case "G3": // на шарде есть бакет по routing
                etcd.Seed("/clusters/shop/buckets/routing/bucket_0", "shard1");
                break;
            case "G5": // живая заявка переезда с to=shard1
                etcd.Seed("/pgworker/moves/shop/bucket_0",
                    """{"op":"move","to":"shard1","requested_unix":1770000000}""");
                break;
            case "G6": // нода в карантине после эвакуации
                etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "QUARANTINED");
                break;
            case "G7": // shard1 — единственный шард кластера
                foreach (var key in etcd.Store.Keys.Where(k =>
                             k.StartsWith("/clusters/shop/shards/shard2/", StringComparison.Ordinal)
                             || k.StartsWith("/service/shop-shard2/", StringComparison.Ordinal)).ToList())
                    etcd.Store.Remove(key);
                break;
        }

        var rig = await NewRig(etcd);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard1", CancellationToken.None);

        // Assert — блокировка НЕ ошибка тика: маркер жив, мутаций docker/etcd нет
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        var work = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        work.Op.Should().Be("remove-shard");
        work.Phase.Should().Be($"blocked-{guardId}");
        work.LastError.Should().NotBeNullOrWhiteSpace();
        rig.Etcd.Store.ContainsKey("/clusters/shop/shards/shard1/state").Should().BeTrue();
        rig.Driver.RemovedNodes.Should().BeEmpty();
    }

    [Theory]
    [InlineData("owner")] [InlineData("target")]
    public async Task Tick_G4_StatusKeyReferencesShard_Blocked(string side)
    {
        // Arrange — ОБА плеча G4 (§4.4 «owner ИЛИ target»): routing НЕ указывает
        // на shard1 (G3 проходит), блокирует именно зависший статус-ключ
        var etcd = SeedBase();
        etcd.Seed("/clusters/shop/buckets/status/bucket_0", side == "owner"
            ? """{"state":"FROZEN","owner":"shard1","target":"shard2","phase":"flip"}"""
            : """{"state":"SYNCING","owner":"shard2","target":"shard1","phase":"ddl"}""");
        var rig = await NewRig(etcd);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard1", CancellationToken.None);

        // Assert — незавершённый переезд держит демонтаж; маркер жив
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        var work = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        work.Phase.Should().Be("blocked-G4");
        work.LastError.Should().Contain("незавершённый переезд");
        rig.Etcd.Store.ContainsKey("/clusters/shop/shards/shard1/state").Should().BeTrue();
        rig.Driver.RemovedNodes.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_G1_ClusterToRemoveMidFlight_Aborted()
    {
        // Arrange — кластер переведён в TO_REMOVE: демонтажем займётся deprovisioning
        var etcd = SeedBase();
        etcd.Seed("/clusters/shop/config",
            """{"buckets":3,"dbname":"shop","created_unix":1755900000,"state":"TO_REMOVE"}""");
        var rig = await NewRig(etcd);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard1", CancellationToken.None);

        // Assert — безопасный abort без docker-мутаций
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("aborted");
        rig.Driver.RemovedNodes.Should().BeEmpty();
        rig.Etcd.Store.ContainsKey("/clusters/shop/shards/shard1/state").Should().BeTrue();
    }

    [Fact]
    public async Task Tick_HappyPath_RemovesDockerThenEtcdKeys()
    {
        // Arrange — помеченный ПУСТОЙ шард, guard'ы проходят
        var rig = await NewRig();

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard1", CancellationToken.None);

        // Assert — демонтаж: docker-объекты сняты, ключи/порталы/журнал вычищены
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Driver.RemovedNodes.Should().Equal("shard1/shard1a", "shard1/shard1b");
        // state=REMOVING записан ДО удаления контейнеров (S2)
        rig.Puts.Should().Contain("/clusters/shop/shards/shard1/nodes/shard1a/state");
        rig.Puts.Should().Contain("/clusters/shop/shards/shard1/nodes/shard1b/state");
        rig.Etcd.Store.Keys.Should().NotContain(k =>
            k.StartsWith("/clusters/shop/shards/shard1/", StringComparison.Ordinal));
        rig.Etcd.Store.Keys.Should().NotContain(k =>
            k.StartsWith("/service/shop-shard1/", StringComparison.Ordinal));
        var portalloc = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
        portalloc.Should().NotContain("shard1/");
        portalloc.Should().Contain("shard2/");
        rig.Etcd.Store.Keys.Should().NotContain("/pgworker/evacuations/shop/shard1");
        // сосед не тронут
        rig.Etcd.Store.ContainsKey("/clusters/shop/shards/shard2/dsn").Should().BeTrue();
        rig.Etcd.Store.ContainsKey("/service/shop-shard2/initialize").Should().BeTrue();
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("done");
    }

    [Fact]
    public async Task Tick_HappyPath_RemovesShardScopedOrphans()
    {
        // Arrange — docker вернул сироту шарда (ключей nodes на неё нет)
        var etcd = SeedBase();
        var driver = new Fakes.FakeDriver
        {
            NodeObjects =
            [
                "pgw-shop-shard1-shard1a", "pgw-shop-shard1-shard1b", "pgw-shop-shard1-x",
            ],
        };
        var rig = await NewRig(etcd, driver);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard1", CancellationToken.None);

        // Assert — сирота разобрана по префиксу шарда
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Driver.RemovedNodes.Should().Contain("shard1/x");
        rig.Driver.NodeObjects.Should().NotContain(n => n.Contains("shard1-"));
    }

    [Fact]
    public async Task Tick_DockerObjectAlive_RepeatsNextTick()
    {
        // Arrange — первый RemoveNode падает (docker-хост недоступен)
        var etcd = SeedBase();
        var driver = new Fakes.FakeDriver
        {
            NodeObjects = ["pgw-shop-shard1-shard1a", "pgw-shop-shard1-shard1b"],
            RemoveFailsOnce = true,
        };
        var rig = await NewRig(etcd, driver);

        // Act — тик с отказом (маркер жив), затем повтор
        var first = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard1", CancellationToken.None);
        var markerAliveAfterFirst = rig.Etcd.Store.ContainsKey("/clusters/shop/shards/shard1/state");
        var second = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard1", CancellationToken.None);

        // Assert — отказ не снёс маркер; повторный тик доводит (идемпотентность)
        first.IsSuccess.Should().BeFalse();
        markerAliveAfterFirst.Should().BeTrue();
        second.IsSuccess.Should().BeTrue();
        second.Value.Should().Be(ProcessOutcome.Done);
    }

    [Fact]
    public async Task Tick_AlreadyRemoved_Done()
    {
        // Arrange — ключей шарда нет (чистка уже прошла)
        var rig = await NewRig();
        var snap = new ClusterSnapshot(
            new ClusterConfig("shop", 3, "shop", null, ClusterState.Active),
            [new ShardSpec("shard2", 2, "dsn", null,
                [new NodeSpec("shard2", "shard2a", NodeState.Running)])],
            []);

        // Act
        var outcome = await rig.Process.TickAsync(snap, "shard1", CancellationToken.None);

        // Assert — идемпотентность: Done без мутаций
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Driver.RemovedNodes.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_MarkedUndeclaredShard_DismantlesDeclaration()
    {
        // Arrange — недоднятый add (declared без dsn) + маркер: способ отменить add (Д5)
        var etcd = SeedBase();
        etcd.Seed("/clusters/shop/shards/shard3/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard3/nodes/shard3a/state", "NOT_INITIALIZED");
        etcd.Seed("/clusters/shop/shards/shard3/nodes/shard3b/state", "NOT_INITIALIZED");
        etcd.Seed("/clusters/shop/shards/shard3/state", "TO_REMOVE");
        etcd.Seed("/service/shop-shard3/request_cpu", "2");
        etcd.Seed("/service/shop-shard3/request_mem", "4Gi");
        var rig = await NewRig(etcd);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);

        // Assert — декларация вычищена; контейнеров не было (RemoveNode 404 = ок)
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Store.Keys.Should().NotContain(k =>
            k.StartsWith("/clusters/shop/shards/shard3/", StringComparison.Ordinal));
        rig.Etcd.Store.Keys.Should().NotContain(k =>
            k.StartsWith("/service/shop-shard3/", StringComparison.Ordinal));
    }
}
