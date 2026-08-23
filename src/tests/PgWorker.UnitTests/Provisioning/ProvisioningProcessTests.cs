using System.Net;
using System.Text;
using System.Text.Json;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;

namespace PgWorker.UnitTests.Provisioning;

// ProvisioningProcess P0–P5 (задача 19; arch/14 §5 A): guard входа, план
// placement+portalloc, EnsureNode×ноды, ожидание Patroni, БД/роли/схемы, dsn,
// снятие status-ключей, config без state, снапшот, journal-фазы.
public class ProvisioningProcessTests
{
    private static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "app-pw", "adm-pw", "mov-pw");
    private static readonly EtcdEndpoints EtcdEndp = new(["http://etcd:2379"]);
    private static readonly PlacementOptions Opts = new(15000, 15100, PatroniBootSec: 600);
    private const string Ep = "http://etcd:2379";

    private static void SeedCluster(Fakes.FakeEtcd etcd)
    {
        etcd.Seed("/clusters/shop/config",
            """{"buckets":4,"dbname":"shop","created_unix":1755900000,"state":"NOT_INITIALIZED"}""");
        etcd.Seed("/clusters/shop/shards/shard1/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard2/replicas", "2");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_0", "shard1");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_1", "shard2");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_2", "shard1");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_3", "shard2");
        etcd.Seed("/clusters/shop/buckets/status/bucket_0", """{"state":"NOT_INITIALIZED"}""");
        etcd.Seed("/clusters/shop/buckets/status/bucket_1", """{"state":"NOT_INITIALIZED"}""");
        etcd.Seed("/clusters/shop/buckets/status/bucket_2", """{"state":"NOT_INITIALIZED"}""");
        etcd.Seed("/clusters/shop/buckets/status/bucket_3", """{"state":"NOT_INITIALIZED"}""");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "NOT_INITIALIZED");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/state", "NOT_INITIALIZED");
        etcd.Seed("/clusters/shop/shards/shard2/nodes/shard2a/state", "NOT_INITIALIZED");
        etcd.Seed("/clusters/shop/shards/shard2/nodes/shard2b/state", "NOT_INITIALIZED");
        // Patroni DCS поднявшихся шардов (P2.2: initialize + leader)
        etcd.Seed("/service/shop-shard1/initialize", "7403705125687833961");
        etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a","poll_queued_commands":0}""");
        etcd.Seed("/service/shop-shard2/initialize", "7403705125687833962");
        etcd.Seed("/service/shop-shard2/leader", """{"name":"shard2a","poll_queued_commands":0}""");
    }

    // Снапшот кластера из имитации etcd (как это сделает цикл задачи 23).
    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == "shop");
    }

    // Patroni-проба с управляемым ответом (живой шард или глухой); ответ по порту ноды.
    private static ShardProbe Probe(Func<int, HttpResponseMessage> respondByPort, List<int>? trace = null)
        => new(new HttpClient(new FakeHandler(r =>
        {
            var port = r.RequestUri!.Port;
            lock (trace ?? new object())
            {
                trace?.Add(port);
            }

            return respondByPort(port);
        })));

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
        ClaimStore Claims, WorkJournal Journal, ProvisioningProcess Process);

    private static async Task<Rig> NewRig(Func<int, HttpResponseMessage> patroniResponse, List<int>? trace = null)
    {
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var driver = new Fakes.FakeDriver();
        var sql = new Fakes.FakeSql();
        var process = new ProvisioningProcess(
            etcd, [Ep], driver, sql, Probe(patroniResponse, trace), claims, journal, Opts, Secrets, EtcdEndp, snapshot: null);
        return new Rig(etcd, driver, sql, claims, journal, process);
    }

    [Fact]
    public async Task Tick_FreshCluster_EnsureNodesThenInProgressWaitingPatroni()
    {
        // Arrange — чистый NOT_INITIALIZED; Patroni молчит (500 на все пробы)
        var rig = await NewRig(_ => DeadPatroni());

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: тик дошёл до P2.2 и ждёт Patroni; все ноды созданы и PROVISIONING
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Driver.EnsuredNodes.Should().BeEquivalentTo(
        [
            "shard1/shard1a", "shard1/shard1b", "shard2/shard2a", "shard2/shard2b",
        ]);
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("PROVISIONING");
        rig.Etcd.Store["/clusters/shop/shards/shard2/nodes/shard2b/state"].Value.Should().Be("PROVISIONING");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("waiting-patroni");
        work.Value.Op.Should().Be("provision");
    }

    [Fact]
    public async Task Tick_PatroniAlive_DoesEverythingToDone()
    {
        // Arrange — Patroni жив: master шарда1 = shard1a, шарда2 = shard2a
        var rig = await NewRig(port => Patroni(port == 18000 ? "shard1a" : "shard2a"));
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        rig.Driver.EnsuredNodes.Should().HaveCount(4); // первый тик создал ноды

        // Act — тик при живом Patroni (master по имени ноды через probe)
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);


        // Assert: DONE — dsn записан, БД/роли/схемы исполнены, статус-ключи
        // сняты, config перезаписан без state (txn compare mod_revision)
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);

        // placement: анти-аффинити → ноды шарда на h1,h2; на каждом хосте свой base
        // (шард1: 15000, шард2: 15001 — тройки портов не пересекаются на хосте)
        rig.Etcd.Store["/clusters/shop/shards/shard1/dsn"].Value.Should()
            .Be("host=h1,h2 port=15000,15000 dbname=shop user=bucket_admin");
        rig.Etcd.Store["/clusters/shop/shards/shard2/dsn"].Value.Should()
            .Be("host=h1,h2 port=15001,15001 dbname=shop user=bucket_admin");

        // portalloc закреплён (ключ создан txn-ом NotExists)
        rig.Etcd.Txns.Should().Contain(t => t.Compare.Any(c =>
            c.Key == "/pgworker/portalloc/shop" && c.Target == TxnTarget.Version && c.Num == 0));

        // БД создаётся на мастере каждого шарда (shard1a → h1:15000, shard2a → h1:15001);
        // оба тика повторяют вызовы — SQL идемпотентен (повтор безопасен, §7)
        rig.Sql.EnsuredDatabases.Should().Contain(
            ("Host=h1;Port=15000;Database=postgres;Username=postgres;Password=su-pw", "shop"));
        rig.Sql.EnsuredDatabases.Should().Contain(
            ("Host=h1;Port=15001;Database=postgres;Username=postgres;Password=su-pw", "shop"));
        rig.Sql.Executed.Should().Contain(e => e.Sql.Contains("CREATE SCHEMA IF NOT EXISTS bucket_0"));
        rig.Sql.Executed.Should().Contain(e => e.Sql.Contains("CREATE SCHEMA IF NOT EXISTS bucket_1"));

        // ноды RUNNING, статус-ключи сняты
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("RUNNING");
        rig.Etcd.Store.Keys.Should().NotContain(k => k.Contains("/status/bucket_"));

        // config перезаписан без state через txn compare mod_revision
        var configTxn = rig.Etcd.Txns.Should().Contain(t =>
            t.Success.OfType<TxnOp.Put>().Any(put => put.Key == "/clusters/shop/config")).Subject;
        configTxn.Compare.Should().ContainSingle(c =>
            c.Target == TxnTarget.ModRevision && c.Key == "/clusters/shop/config");
        var config = rig.Etcd.Store["/clusters/shop/config"].Value;
        config.Should().NotContain("state");
        JsonDocument.Parse(config).RootElement.GetProperty("buckets").GetInt32().Should().Be(4);

        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("done");
    }

    [Fact]
    public async Task Tick_AfterDone_NoNewEnsureNodes()
    {
        // Arrange — кластер доведён до DONE, снапшот перечитан (ноды RUNNING)
        var rig = await NewRig(port => Patroni(port == 18000 ? "shard1a" : "shard2a"));
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        var ensuredTotal = rig.Driver.EnsuredNodes.Count;
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("done");

        // Act — повторный тик по свежему снапшоту
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: идемпотентность — новых EnsureNode нет, всё ещё DONE
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Driver.EnsuredNodes.Should().HaveCount(ensuredTotal);
    }

    [Fact]
    public async Task Tick_NoRoutingKeys_WaitingKeys_NoDocker()
    {
        // Arrange — полуфабрикат панели: config есть, routing-ключей нет
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/config",
            """{"buckets":4,"dbname":"shop","created_unix":1755900000,"state":"NOT_INITIALIZED"}""");
        etcd.Seed("/clusters/shop/shards/shard1/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "NOT_INITIALIZED");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/state", "NOT_INITIALIZED");
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var driver = new Fakes.FakeDriver();
        var process = new ProvisioningProcess(
            etcd, [Ep], driver, new Fakes.FakeSql(), Probe(_ => Patroni("shard1a")),
            claims, journal, Opts, Secrets, EtcdEndp, snapshot: null);

        // Act
        var outcome = await process.TickAsync(await Snapshot(etcd), CancellationToken.None);

        // Assert: guard входа — docker не трогаем, ждём доустойчивости ключей
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        driver.EnsuredNodes.Should().BeEmpty();
        etcd.Store.Should().NotContainKey("/pgworker/portalloc/shop");
        (await journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("waiting-keys");
    }

    [Fact]
    public async Task Tick_ConfigSwitchedToRemove_SafelyAborts()
    {
        // Arrange — R6: панель перевела кластер в TO_REMOVE до тика
        var rig = await NewRig(port => Patroni(port == 18000 ? "shard1a" : "shard2a"));
        rig.Etcd.Seed("/clusters/shop/config",
            """{"buckets":4,"dbname":"shop","created_unix":1755900000,"state":"TO_REMOVE"}""");

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: provisioning прекращается без мутаций — кластер подхватит deprovisioning
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Driver.EnsuredNodes.Should().BeEmpty();
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase.Should().Be("aborted");
    }
}
