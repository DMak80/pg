using FluentAssertions;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.UnitTests.Provisioning;

// NodeSupervisor (arch/16 §5 C): снесённый контейнер пересоздаётся, молчание
// дольше NodeDeadSec → UNREACHABLE + пересоздание с чистым томом (RF=1 —
// warning), чужие процессы (TO_REMOVE/REMOVING/PROVISIONING) не трогаются.

public class NodeSupervisorTests
{
    private const string Ep = "http://etcd:2379";

    private sealed record Rig(
        Fakes.FakeEtcd Etcd,
        Fakes.FakeKafkaDriver Driver,
        FakeKafkaAdminClient Admin,
        ClaimStore Claims,
        WorkJournal Journal,
        NodeSupervisor Supervisor);

    private static async Task<Rig> NewRig(
        int nodeDeadSec = 90,
        int rf = 3,
        Action<Fakes.FakeEtcd, Fakes.FakeKafkaDriver, FakeKafkaAdminClient>? setup = null)
    {
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafka/clusters/events/config",
            $$"""{"brokers":3,"replication_factor":{{rf}},"min_insync_replicas":1,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}""");
        for (var k = 1; k <= 3; k++)
        {
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/state", "RUNNING");
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/role", k <= 3 ? "controller" : "broker");
        }
        etcd.Seed("/kafka/clusters/events/endpoints", "h1:16000,h1:16001,h1:16002");
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.Seed("/kafka/clusters/events/app_password", "AbCdEf0123456789AbCdEf0123456789");
        etcd.Seed("/kafkaworker/portalloc/events",
            """{"broker1":{"host":"h1","client":16000},"broker2":{"host":"h1","client":16001},"broker3":{"host":"h1","client":16002}}""");

        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("events", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var driver = new Fakes.FakeKafkaDriver
        {
            NodeObjects = ["kfw-events-broker1", "kfw-events-broker2", "kfw-events-broker3"],
        };
        var admin = new FakeKafkaAdminClient
        {
            ClusterView = new KafkaClusterView(
                [new KafkaBrokerView(1, "b1"), new KafkaBrokerView(2, "b2"), new KafkaBrokerView(3, "b3")],
                ControllerId: 1),
        };
        var supervisor = new NodeSupervisor(
            etcd, [Ep], driver, claims, journal, new FakeAdminFactory(admin),
            new ProvisioningOptions(16000, 16999, 600, nodeDeadSec, null, "apache/kafka:4.0.0"));
        setup?.Invoke(etcd, driver, admin);
        return new Rig(etcd, driver, admin, claims, journal, supervisor);
    }

    private static async Task<KafkaClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/kafka/clusters/", CancellationToken.None);
        return KafkaSnapshotParser.Parse(range.Value).Value.Single(c => c.Cluster == "events");
    }

    private sealed class FakeAdminFactory(FakeKafkaAdminClient client) : IKafkaAdminClientFactory
    {
        public IKafkaAdminClient Create(string bootstrap, string user, string password) => client;
    }

    [Fact]
    public async Task Run_MissingContainer_RecreatedWithSameEnv()
    {
        // Arrange: broker2 снесён руками (docker-объекта нет), кластер жив.
        var rig = await NewRig();
        rig.Driver.NodeObjects.Remove("kfw-events-broker2");

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: EnsureNode вызван для broker2 с тем же env/портом из portalloc
        // (advertised стабилен); контейнер снова в списке объектов.
        result.IsSuccess.Should().BeTrue();
        var spec = rig.Driver.Ensured.Single(s => s.NodeName == "broker2");
        spec.ClientHostPort.Should().Be(16001);
        spec.Env["KAFKA_ADVERTISED_LISTENERS"].Should().Contain("CLIENT://h1:16001");
        rig.Driver.NodeObjects.Should().Contain("kfw-events-broker2");
        // Успешные ноды не тронуты; пересозданный — PROVISIONING (в RUNNING
        // переведёт следующий цикл по факту готовности).
        rig.Driver.Ensured.Should().HaveCount(1);
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker2/state"].Value.Should().Be("PROVISIONING");
    }

    // Сид journal-трека молчания: unreachable {broker: first_seen_unix}.
    private static void SeedSilent(Fakes.FakeEtcd etcd, string broker, long sinceUnix)
        => etcd.Seed("/kafkaworker/work/events",
            "{\"op\":\"supervise\",\"phase\":\"supervising\",\"instance\":\"x\",\"updated_unix\":1," +
            "\"unreachable\":{\"" + broker + "\":" + sinceUnix + "}}");

    [Fact]
    public async Task Run_BrokerSilentBeyondBudget_UnreachableAndCleanVolumeRecreate()
    {
        // Arrange: broker3 не отвечает в DescribeCluster (нет id=3) и молчит
        // уже дольше NodeDeadSec (трек в journal.unreachable засеян в прошлом).
        var rig = await NewRig(nodeDeadSec: 90);
        rig.Admin.ClusterView = new KafkaClusterView(
            [new KafkaBrokerView(1, "b1"), new KafkaBrokerView(2, "b2")], ControllerId: 1);
        SeedSilent(rig.Etcd, "broker3", DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 200);

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: broker3 → UNREACHABLE, пересоздан с чистым томом
        // (RemoveNode removeVolume=true + EnsureNode), RF=3 — без warning.
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker3/state"].Value.Should().Be("PROVISIONING");
        rig.Driver.Removed.Should().Contain(("broker3", true));
        rig.Driver.Ensured.Should().Contain(s => s.NodeName == "broker3");
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.LastError.Should().BeNull("RF>1 — потеря данных не грозит");
    }

    [Fact]
    public async Task Run_SilentBrokerRfOne_WarningJournaled()
    {
        // Arrange: RF=1-кластер, broker1 молчит дольше бюджета.
        var rig = await NewRig(nodeDeadSec: 90, rf: 1);
        rig.Admin.ClusterView = new KafkaClusterView([], ControllerId: null);
        SeedSilent(rig.Etcd, "broker1", DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 200);

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: пересоздание с чистым томом + journal-warning о потере данных.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().Contain(("broker1", true));
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.LastError.Should().Contain("RF=1");
    }

    [Fact]
    public async Task Run_SilentBrokerWithinBudget_Waited()
    {
        // Arrange: broker3 молчит, но меньше NodeDeadSec (трек свежий).
        var rig = await NewRig(nodeDeadSec: 90);
        rig.Admin.ClusterView = new KafkaClusterView(
            [new KafkaBrokerView(1, "b1"), new KafkaBrokerView(2, "b2")], ControllerId: 1);
        SeedSilent(rig.Etcd, "broker3", DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 10);

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: ждём — контейнер жив, state не менялся, пересоздания нет.
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker3/state"].Value.Should().Be("RUNNING");
        rig.Driver.Removed.Should().BeEmpty();
    }

    [Fact]
    public async Task Run_ForeignStates_Untouched()
    {
        // Arrange: broker2 — TO_REMOVE (панель), broker3 — PROVISIONING
        // (provisioning-процесс), оба контейнера отсутствуют.
        var rig = await NewRig();
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker2/state", "TO_REMOVE");
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker3/state", "PROVISIONING");
        rig.Driver.NodeObjects.Remove("kfw-events-broker2");
        rig.Driver.NodeObjects.Remove("kfw-events-broker3");
        rig.Admin.ClusterView = new KafkaClusterView([new KafkaBrokerView(1, "b1")], ControllerId: 1);

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: только broker1 в зоне надзора (жив) — никаких пересозданий
        // и перезаписей state у чужих процессов.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker2/state"].Value.Should().Be("TO_REMOVE");
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker3/state"].Value.Should().Be("PROVISIONING");
    }

    [Fact]
    public async Task Run_NoEndpoints_NoProbeNoCrash()
    {
        // Arrange: Active-кластер без endpoints/app-кредов (ещё поднимается).
        var rig = await NewRig();
        rig.Etcd.Store.Remove("/kafka/clusters/events/endpoints");
        rig.Etcd.Store.Remove("/kafka/clusters/events/app_password");

        // Act: тик надзора.
        var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: проба невозможна — не ошибка; пересоздание снесённых живо.
        result.IsSuccess.Should().BeTrue();
    }
}
