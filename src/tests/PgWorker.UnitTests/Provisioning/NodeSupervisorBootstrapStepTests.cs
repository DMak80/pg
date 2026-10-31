using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;
using PgWorker.Provisioning.Processes;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Probes;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.UnitTests.Provisioning;

// Шаг хендшейка/конвергенции надзора (arch/14 §5 C, spec §3.7.3): реакции по
// статусному ключу /pgworker/nodes/<C>/… — not_initialized → доставка
// (delivery.EnsureAsync ровно один раз/тик); ready на ревизии → мутаций нет;
// ready+расхождение → доставка (apply); start_failed → RemoveNodeAsync +
// EnsureNodeAsync (НОВЫЙ ДИСК) + доставка; ≤1 мутация/тик; restore-шард/
// TO_REMOVE-шард/object-нода — без реакций даже при start_failed.
public class NodeSupervisorBootstrapStepTests
{
    private const string Ep = "http://etcd:2379";
    private static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "adm-pw", "mov-pw");

    // Фейк-delivery: журнал вызовов + настраиваемый исход (transient-инъекция).
    private sealed class FakeDelivery : INodeBootstrapDelivery
    {
        public readonly List<(string Shard, string Node)> Calls = [];

        public Task<DeliveryOutcome> EnsureAsync(
            ShardTopology topology, string nodeName, NodeAddress addr, InstallSecrets secrets,
            PgTuneResult? tuning, bool syncStrict, string restPassword, CancellationToken ct)
        {
            Calls.Add((topology.Shard, nodeName));
            return Task.FromResult(new DeliveryOutcome(
                Delivered: true, Wait: false, Transient: false, null));
        }

        public Task<bool> IsOnRevisionAsync(
            ShardTopology topology, string nodeName, NodeAddress addr,
            PgTuneResult? tuning, bool syncStrict, string restPassword, CancellationToken ct)
            => Task.FromResult(false);
    }

    private static void SeedCluster(Fakes.FakeEtcd etcd, string shardState = "RUNNING")
    {
        etcd.Seed("/clusters/shop/config",
            """{"buckets":2,"dbname":"shop","created_unix":1755900000,"state":"ACTIVE"}""");
        etcd.Seed("/clusters/shop/rest_password", "Rest0Pass0000000000000000000000A");
        etcd.Seed("/clusters/shop/shards/shard1/replicas", "2");
        etcd.Seed("/clusters/shop/shards/shard1/dsn",
            "host=h1 port=15000 dbname=shop user=bucket_admin password=x");
        etcd.Seed("/clusters/shop/shards/shard1/master", "h1:16500");
        foreach (var node in new[] { "shard1a", "shard1b", "shard1c" })
        {
            etcd.Seed($"/clusters/shop/shards/shard1/nodes/{node}/state", shardState);
            etcd.Seed($"/clusters/shop/shards/shard1/nodes/{node}/app_params", "sslmode=require");
        }
        etcd.Seed("/pgworker/portalloc/shop", Portalloc.Serialize(new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
            ["shard1/shard1b"] = new("h2", new NodePorts(15002, 18002, 16502)),
            ["shard1/shard1c"] = new("h1", new NodePorts(15003, 18003, 16503)),
        }));
        etcd.Seed("/service/shop-shard1/request_cpu", "2");
        etcd.Seed("/service/shop-shard1/request_mem", "4Gi");
    }

    private sealed record Rig(
        Fakes.FakeEtcd Etcd, Fakes.FakeDriver Driver, FakeDelivery Delivery,
        NodeSupervisor Supervisor);

    private static async Task<Rig> NewRig()
    {
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd);
        var claims = new ClaimStore("/pgworker", [Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        var journal = new WorkJournal("/pgworker", etcd, [Ep]);
        var delivery = new FakeDelivery();
        var driver = new Fakes.FakeDriver
        {
            NodeObjects =
            [
                "pgw-shop-shard1-shard1a", "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c",
            ],
        };
        // Patroni /cluster — members без кворум-факта (пробы шарда не владеют
        // сценариями этого теста; шаг конвергенции их не зовёт).
        var probe = new ShardProbe(new HttpClient(new FakeHandler(_ =>
            new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    """{"members":[{"name":"shard1a","role":"leader","state":"running"}]}"""),
            })));
        var supervisor = new NodeSupervisor(
            etcd, [Ep], driver, probe, new Fakes.FakeSql(), claims, journal,
            new ThresholdsOptions(90, 300, 300), TimeProvider.System, Secrets,
            new Fakes.FakeSecretEnsurer(),
            new AppParamsEnsurer(etcd, [Ep], "sslmode=require"),
            Fakes.PgtuneFactory(), Fakes.PgtuneSettings(),
            delivery,
            NullLogger<NodeSupervisor>.Instance,
            new MasterKeyReconciler(etcd, [Ep], probe));
        return new Rig(etcd, driver, delivery, supervisor);
    }

    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(respond(request));
    }

    private static void SeedStatus(Fakes.FakeEtcd etcd, string node, string state,
        long? applied = null, long mod = 3)
    {
        etcd.Store[$"/pgworker/nodes/shop/shard1/{node}/status"] = new Fakes.FakeEtcd.Entry(
            System.Text.Json.JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["state"] = state,
                ["applied_revision"] = applied,
                ["components"] = new Dictionary<string, string> { ["pg"] = "ready", ["doorman"] = "ready" },
                ["updated_unix"] = 1,
            }), mod, 1);
    }

    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == "shop");
    }

    // not_initialized → delivery.EnsureAsync ровно один раз, тик успешен.
    [Fact]
    public async Task Tick_NotInitialized_DeliveryCalledOnce()
    {
        // Arrange: статус not_initialized одной ноды.
        var rig = await NewRig();
        SeedStatus(rig.Etcd, "shard1a", "not_initialized");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

        // Assert
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        rig.Delivery.Calls.Should().HaveCount(1);
        rig.Delivery.Calls[0].Should().Be(("shard1", "shard1a"));
    }

    // Все ноды ready на ревизии (bootstrap-ключей нет — узел не готовится;
    // нет статуса доставки → мутаций нет)... точный контракт: ready-статус БЕЗ
    // ключа → ключ «ещё не доставлен» → delivery. Здесь проверяем чистый
    // no-op-путь: статусов нет вовсе — мутаций нет.
    [Fact]
    public async Task Tick_NoStatuses_NoMutations()
    {
        // Arrange: статусных ключей нет (агенты стартуют).
        var rig = await NewRig();

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

        // Assert: доставок нет (ключа нет — delivery ставит Wait... delivery
        // вызывается ТОЛЬКО при статусе not_initialized/ready-расхождении —
        // без статусов шаг молчит, arch/14 §5 C).
        outcome.IsSuccess.Should().BeTrue();
        rig.Delivery.Calls.Should().BeEmpty();
    }

    // ready + расхождение applied != mod bootstrap-ключа → доставка (apply).
    [Fact]
    public async Task Tick_ReadyStaleRevision_DeliveryCalled()
    {
        // Arrange: ready с applied=1; bootstrap-ключ с mod=9 (расхождение).
        var rig = await NewRig();
        rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/bootstrap"] =
            new Fakes.FakeEtcd.Entry("""{"spilo_configuration":"x","cert_pem":"c","key_pem":"k"}""", 9, 1);
        SeedStatus(rig.Etcd, "shard1a", "ready", applied: 1, mod: 3);

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

        // Assert: доставка (apply) — ровно один вызов.
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        rig.Delivery.Calls.Should().HaveCount(1);
        rig.Delivery.Calls[0].Should().Be(("shard1", "shard1a"));
    }

    // start_failed → RemoveNodeAsync + EnsureNodeAsync (новый диск), delivery после.
    [Fact]
    public async Task Tick_StartFailed_RecreatesNodeWithNewDisk()
    {
        // Arrange: статус start_failed ноды shard1a.
        var rig = await NewRig();
        SeedStatus(rig.Etcd, "shard1a", "start_failed");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

        // Assert: RemoveNode (kill+rm контейнера+volume) + EnsureNode (новый
        // volume) + доставка — ровно одна мутация на тик.
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        rig.Driver.RemovedNodes.Should().Contain("shard1/shard1a");
        rig.Driver.EnsuredNodes.Should().Contain("shard1/shard1a");
        rig.Delivery.Calls.Should().HaveCount(1);
        rig.Delivery.Calls[0].Should().Be(("shard1", "shard1a"));
    }

    // TO_REMOVE-шард → никаких реакций даже при start_failed нод (спека
    // §3.7.3/§8.19: шаг пропускает TO_REMOVE-шарды ЦЕЛИКОМ).
    [Fact]
    public async Task Tick_ToRemoveShard_NoMutations()
    {
        // Arrange: статусы start_failed на всех нодах; шард помечен TO_REMOVE.
        var rig = await NewRig();
        foreach (var node in new[] { "shard1a", "shard1b", "shard1c" })
            SeedStatus(rig.Etcd, node, "start_failed");
        rig.Etcd.Store["/clusters/shop/shards/shard1/state"] =
            new Fakes.FakeEtcd.Entry("TO_REMOVE", 5, 1);

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

        // Assert: доставок и пересозданий нет (шард — вне шага).
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        rig.Delivery.Calls.Should().BeEmpty();
        rig.Driver.RemovedNodes.Should().BeEmpty();
    }

    // Два кандидата за тик (две not_initialized ноды) → обработан ровно один.
    [Fact]
    public async Task Tick_TwoCandidates_OnlyOneMutationPerTick()
    {
        // Arrange: статусы not_initialized у двух нод.
        var rig = await NewRig();
        SeedStatus(rig.Etcd, "shard1a", "not_initialized");
        SeedStatus(rig.Etcd, "shard1b", "not_initialized");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

        // Assert: ровно одна мутация на тик кластера.
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        rig.Delivery.Calls.Should().ContainSingle();
    }
}
