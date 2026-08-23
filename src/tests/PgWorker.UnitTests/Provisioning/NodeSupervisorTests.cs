using System.Net;
using System.Text;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;

namespace PgWorker.UnitTests.Provisioning;

// NodeSupervisor + MasterKeyReconciler (задача 21; arch/14 §5 C, P11):
// самовосстановление декларации, rebuild мёртвой не-лидерской ноды при
// кворуме, лидер не трогается (failover — Patroni), детект мёртвого шарда,
// сверка мастер-ключа только при рассинхроне.
public class NodeSupervisorTests
{
    private const string Ep = "http://etcd:2379";
    private static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "app-pw", "adm-pw", "mov-pw");
    private static readonly ThresholdsOptions Thresholds = new(NodeDeadSec: 90, ShardDeadSec: 300);

    // Patroni-проба: 200/500 по порту ноды (два Patroni-порта на хостах h1/h2).
    private static ShardProbe Probe(Func<int, HttpResponseMessage> respondByPort)
        => new(new HttpClient(new FakeHandler(r => respondByPort(r.RequestUri!.Port))));

    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(responder(request));
    }

    // Полноценные ответы: /cluster парсит тело (members), /primary — только статус.
    private static HttpResponseMessage Ok() => new(HttpStatusCode.OK)
    {
        Content = new StringContent("""{"members":[]}""", Encoding.UTF8, "application/json"),
    };

    private static HttpResponseMessage Down() => new(HttpStatusCode.ServiceUnavailable);

    private static void SeedCluster(Fakes.FakeEtcd etcd, int nodes = 3)
    {
        etcd.Seed("/clusters/shop/config", """{"buckets":2,"dbname":"shop","created_unix":1755900000}""");
        etcd.Seed("/clusters/shop/shards/shard1/replicas", nodes.ToString());
        for (var i = 0; i < nodes; i++)
            etcd.Seed($"/clusters/shop/shards/shard1/nodes/shard1{(char)('a' + i)}/state", "RUNNING");
        etcd.Seed("/clusters/shop/shards/shard1/dsn", "host=h1,h2 port=15000,15000 dbname=shop user=bucket_admin");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_0", "shard1");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_1", "shard1");
        // portalloc: ноды h1/h2 чередуются, порты уникальны per-нода (18000/18001/18002)
        var alloc = new Dictionary<string, NodeAddress>();
        for (var i = 0; i < nodes; i++)
            alloc[$"shard1/shard1{(char)('a' + i)}"] = new NodeAddress(
                i % 2 == 0 ? "h1" : "h2",
                new NodePorts(15000 + i, 18000 + i, 16500 + i));
        etcd.Seed("/pgworker/portalloc/shop", PgWorker.Core.Model.Portalloc.Serialize(alloc));
    }

    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == "shop");
    }

    private sealed record Rig(Fakes.FakeEtcd Etcd, Fakes.FakeDriver Driver, ClaimStore Claims,
        WorkJournal Journal, NodeSupervisor Supervisor);

    private static async Task<Rig> NewRig(
        Func<int, HttpResponseMessage> respond,
        IReadOnlyList<string>? nodeObjects = null,
        long? staleUnreachableForShard1A = null,
        long? staleUnreachableAll = null)
    {
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        if (staleUnreachableForShard1A.HasValue || staleUnreachableAll.HasValue)
        {
            var track = new Dictionary<string, long>();
            if (staleUnreachableForShard1A is { } staleA)
                track["shard1/shard1a"] = staleA;
            if (staleUnreachableAll is { } staleAll)
                for (var i = 0; i < 3; i++)
                    track[$"shard1/shard1{(char)('a' + i)}"] = staleAll;
            await journal.WriteSupervisionAsync("shop", "seed", track, CancellationToken.None);
        }

        var driver = new Fakes.FakeDriver
        {
            NodeObjects = (nodeObjects ?? new List<string>
            {
                "pgw-shop-shard1-shard1a", "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c",
            }).ToList(),
        };
        var probe = Probe(respond);
        var supervisor = new NodeSupervisor(
            etcd, [Ep], driver, probe, claims, journal, Thresholds, TimeProvider.System, Secrets,
            new MasterKeyReconciler(etcd, [Ep], probe));
        return new Rig(etcd, driver, claims, journal, supervisor);
    }

    [Fact]
    public async Task Tick_ManuallyRemovedContainer_EnsureNodeRestores()
    {
        // Arrange — контейнер shard1a снесён руками (docker его не видит), Patroni жив
        var rig = await NewRig(_ => Ok(), nodeObjects:
        [
            "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c",
        ]);

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: декларативное самовосстановление — нода пересоздана, state PROVISIONING
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Driver.EnsuredNodes.Should().ContainSingle().Which.Should().Be("shard1/shard1a");
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("PROVISIONING");
    }

    [Fact]
    public async Task Tick_DeadNonLeaderNodeWithQuorum_Rebuild()
    {
        // Arrange — shard1a мертва дольше NodeDeadSec (трек устарел), лидер shard1b,
        // кворум жив (b, c отвечают); /primary: b — primary
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var rig = await NewRig(
            port => port == 18000 ? Down() : Ok(), // shard1a (18000) мертва, b/c живы
            staleUnreachableForShard1A: now - 200);
        rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1b"}""");
        rig.Etcd.Seed("/clusters/shop/shards/shard1/master", "h1:16500"); // устаревший (shard1a была)

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: rebuild — RemoveNode + EnsureNode того же addr, state REBUILDING
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Driver.RemovedNodes.Should().ContainSingle().Which.Should().Be("shard1/shard1a");
        rig.Driver.EnsuredNodes.Should().Contain("shard1/shard1a");
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("REBUILDING");
    }

    [Fact]
    public async Task Tick_DeadLeaderNode_NoRebuild()
    {
        // Arrange — мертва ЛИДЕР-нода shard1a: failover делает Patroni (P11), не мы
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var rig = await NewRig(
            port => port == 18000 ? Down() : Ok(),
            staleUnreachableForShard1A: now - 200);
        rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a"}""");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: никаких docker-мутаций, нода отмечена UNREACHABLE
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Driver.RemovedNodes.Should().BeEmpty();
        rig.Driver.EnsuredNodes.Should().BeEmpty();
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("UNREACHABLE");
    }

    [Fact]
    public async Task Tick_WholeShardDead_MasterExpired_DeadShards()
    {
        // Arrange — весь шард молчит дольше ShardDeadSec, master-ключа нет
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var rig = await NewRig(_ => Down(), staleUnreachableAll: now - 400);

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: шард попал в DeadShards (триггер эвакуации для цикла, задачи 22/23)
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Supervisor.DeadShards.Should().BeEquivalentTo(["shard1"]);
    }

    [Fact]
    public async Task Tick_WholeShardDeadButMasterAlive_NotDead()
    {
        // Arrange — ноды молчат, но master-ключ жив (Patroni lease) — надежда есть
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var rig = await NewRig(_ => Down(), staleUnreachableAll: now - 400);
        rig.Etcd.Seed("/clusters/shop/shards/shard1/master", "h1:16500");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: эвакуация не запускается (arch/14 §5 C: master протух — обязательное условие)
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Supervisor.DeadShards.Should().BeEmpty();
    }

    [Fact]
    public async Task MasterKeyReconciler_KeyPointsToReplica_RewrittenToPrimary()
    {
        // Arrange — ключ указывает на реплику (h2); фактический primary — h1
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/shards/shard1/master", "h2:16500");
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
            ["shard1/shard1b"] = new("h2", new NodePorts(15000, 18001, 16501)),
        };
        var snap = new ClusterSnapshot(
            new ClusterConfig("shop", 2, "shop", null, ClusterState.Active),
            [new ShardSpec("shard1", 2, null, "h2:16500",
            [
                new NodeSpec("shard1", "shard1a", NodeState.Running),
                new NodeSpec("shard1", "shard1b", NodeState.Running),
            ])],
            []);
        // /primary: только shard1a (порт 18000) отвечает 200
        var probe = Probe(port => port == 18000 ? Ok() : Down());
        var reconciler = new MasterKeyReconciler(etcd, [Ep], probe);

        // Act
        var result = await reconciler.ReconcileAsync(snap, addresses, CancellationToken.None);

        // Assert: ключ переписан по факту primary (h1:doorman-порт) под lease TTL 5
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/shards/shard1/master"].Value.Should().Be("h1:16500");
        etcd.Txns.Should().BeEmpty(); // коррекция — прямой put (не txn)
    }

    [Fact]
    public async Task MasterKeyReconciler_KeyCorrect_NoMutation()
    {
        // Arrange — ключ уже указывает на фактический primary (h1)
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/shards/shard1/master", "h1:16500");
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
            ["shard1/shard1b"] = new("h2", new NodePorts(15000, 18001, 16501)),
        };
        var snap = new ClusterSnapshot(
            new ClusterConfig("shop", 2, "shop", null, ClusterState.Active),
            [new ShardSpec("shard1", 2, null, "h1:16500",
            [
                new NodeSpec("shard1", "shard1a", NodeState.Running),
                new NodeSpec("shard1", "shard1b", NodeState.Running),
            ])],
            []);
        var probe = Probe(port => port == 18000 ? Ok() : Down());
        var reconciler = new MasterKeyReconciler(etcd, [Ep], probe);
        var before = etcd.Store["/clusters/shop/shards/shard1/master"].ModRevision;

        // Act
        var result = await reconciler.ReconcileAsync(snap, addresses, CancellationToken.None);

        // Assert: синхрон — ноль мутаций (не второй регулярный писатель, P11)
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/shards/shard1/master"].ModRevision.Should().Be(before);
    }
}
