using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;

namespace PgWorker.UnitTests.Provisioning;

// DeprovisioningProcess D0–D3 (задача 20; arch/14 §5 B): удаление нод и сирот,
// очистка префикса кластера + service-скопов, снятие клэйма сразу (не по TTL).
public class DeprovisioningProcessTests
{
    private const string Ep = "http://etcd:2379";

    private static void SeedRemovableCluster(Fakes.FakeEtcd etcd)
    {
        etcd.Seed("/clusters/shop/config",
            """{"buckets":2,"dbname":"shop","created_unix":1755900000,"state":"TO_REMOVE"}""");
        etcd.Seed("/clusters/shop/shards/shard1/replicas", "2");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_0", "shard1");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_1", "shard1");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/state", "RUNNING");
        etcd.Seed("/service/shop-shard1/initialize", "7403705125687833961");
        etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a"}""");
        etcd.Seed("/service/shop-shard1/request_cpu", "2");
        etcd.Seed("/service/shop-shard1/request_mem", "4G");
        etcd.Seed("/pgworker/portalloc/shop",
            """{"shard1/shard1a":{"Host":"h1","Ports":{"Pg":15000,"Patroni":18000,"Doorman":16500}}}""");
    }

    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == "shop");
    }

    private sealed record Rig(Fakes.FakeEtcd Etcd, Fakes.FakeDriver Driver, ClaimStore Claims,
        WorkJournal Journal, DeprovisioningProcess Process, List<string> Snapshots);

    private static async Task<Rig> NewRig(Fakes.FakeDriver? driver = null)
    {
        var etcd = new Fakes.FakeEtcd();
        SeedRemovableCluster(etcd);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var usedDriver = driver ?? new Fakes.FakeDriver();
        var snapshots = new List<string>();
        var process = new DeprovisioningProcess(
            etcd, [Ep], usedDriver, claims, journal,
            snapshot: ct =>
            {
                snapshots.Add("shot");
                return Task.FromResult(Core.Result.Success());
            });
        return new Rig(etcd, usedDriver, claims, journal, process, snapshots);
    }

    [Fact]
    public async Task Tick_FullRemoval_RemovesNodesKeysAndScope()
    {
        // Arrange — кластер в TO_REMOVE, клэйм наш
        var rig = await NewRig();

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: Done; все ноды удалены, префикс кластера пуст, service-скоп
        // очищен (заявки + Patroni-ключи), portalloc/work удалены, снапшот снят
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Driver.RemovedNodes.Should().BeEquivalentTo(["shard1/shard1a", "shard1/shard1b"]);
        rig.Etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/clusters/shop/", StringComparison.Ordinal));
        rig.Etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/service/shop-shard1/", StringComparison.Ordinal));
        rig.Etcd.Store.Keys.Should().NotContain(k =>
            k == "/pgworker/portalloc/shop" || k == "/pgworker/work/shop");
        rig.Snapshots.Should().ContainSingle();
    }

    [Fact]
    public async Task Tick_OrphanContainers_RemovedToo()
    {
        // Arrange — docker вернул имя, которого нет в nodes-ключах (сирота)
        var driver = new Fakes.FakeDriver
        {
            NodeObjects = ["pgw-shop-shard1-shard1a", "pgw-shop-shard1-shard1b", "pgw-shop-oldshard-oldnode1"],
        };
        var rig = await NewRig(driver);

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: сирота тоже удалён (имя разбирается на шард/ноду)
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Driver.RemovedNodes.Should().BeEquivalentTo(
        [
            "shard1/shard1a", "shard1/shard1b", // заявленные
            "oldshard/oldnode1", // сирота (nodes-ключей нет, docker вернул имя)
        ]);
    }

    [Fact]
    public async Task Tick_DockerFail_JournalRemovingNodes_NextTickContinues()
    {
        // Arrange — первый RemoveNode падает (docker-хост недоступен)
        var driver = new Fakes.FakeDriver { RemoveFailsOnce = true };
        var rig = await NewRig(driver);

        // Act — тик с отказом, затем тик со здоровым docker
        var first = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        var firstPhase = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase;
        var second = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: отказ зафиксирован в журнале (фаза removing-nodes), тик вернул
        // ошибку → ретрай следующим тиком доезжает до Done (§7: last_error + продолжение)
        first.IsSuccess.Should().BeFalse();
        firstPhase.Should().Be("removing-nodes");
        second.Value.Should().Be(ProcessOutcome.Done);
        rig.Driver.RemovedNodes.Should().BeEquivalentTo(["shard1/shard1a", "shard1/shard1b"]);
    }

    [Fact]
    public async Task Tick_AfterDone_ClaimReleasedImmediately()
    {
        // Arrange — кластер демонтирован первым тиком
        var rig = await NewRig();

        // Act
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: клэйм снят ЯВНО (ключ удалён, IsMine=false) — не ждём TTL 15с
        rig.Etcd.Store.Keys.Should().NotContain("/pgworker/claims/shop");
        rig.Claims.IsMine("shop").Should().BeFalse();
    }

    [Fact]
    public async Task Tick_ConfigKeyMissing_AlreadyClean_Done()
    {
        // Arrange — etcd уже пуст: снапшот вырожден (парсер дал бы пустой кластер)
        var rig = await NewRig();
        rig.Etcd.Store.Clear();
        var emptySnap = new ClusterSnapshot(
            new ClusterConfig("shop", 0, string.Empty, null, ClusterState.ToRemove), [], []);

        // Act
        var outcome = await rig.Process.TickAsync(emptySnap, CancellationToken.None);

        // Assert: идемпотентность — пустой кластер сразу Done
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
    }
}
