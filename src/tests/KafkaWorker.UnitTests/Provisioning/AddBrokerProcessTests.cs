using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;

namespace KafkaWorker.UnitTests.Provisioning;

// AddBrokerProcess (arch/16 §5 F): заявка NOT_INITIALIZED у Active-кластера →
// broker-only контейнер с неизменным кворумом → DescribeCluster → RMW endpoints →
// RUNNING; идемпотентность.

public class AddBrokerProcessTests
{
    private const string Ep = "http://etcd:2379";

    private sealed record Rig(
        Fakes.FakeEtcd Etcd,
        Fakes.FakeKafkaDriver Driver,
        FakeKafkaAdminClient Admin,
        ClaimStore Claims,
        WorkJournal Journal,
        AddBrokerProcess Process);

    // Active-кластер events: 3 RUNNING controller-брокера + endpoints + креды.
    private static void SeedActive(Fakes.FakeEtcd etcd)
    {
        etcd.Seed("/kafka/clusters/events/config",
            """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}""");
        for (var k = 1; k <= 3; k++)
        {
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/state", "RUNNING");
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/role", "controller");
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/resources",
                """{"cpu":"2","mem":"4Gi","disk":"40Gi"}""");
        }

        etcd.Seed("/kafka/clusters/events/endpoints", "h1:16000,h1:16001,h1:16002");
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.Seed("/kafka/clusters/events/app_password", "OldPassword0123456789abcdef");
        etcd.Seed("/kafkaworker/portalloc/events",
            """{"broker1":{"host":"h1","client":16000},"broker2":{"host":"h1","client":16001},"broker3":{"host":"h1","client":16002}}""");
    }

    private static async Task<KafkaClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/kafka/clusters/", CancellationToken.None);
        return KafkaSnapshotParser.Parse(range.Value).Value.Single(c => c.Cluster == "events");
    }

    private static async Task<Rig> NewRig()
    {
        var etcd = new Fakes.FakeEtcd();
        SeedActive(etcd);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("events", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        var portLock = new PortAllocLock([Ep], etcd, TimeProvider.System, claims.InstanceId);
        var portAllocIndex = new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);
        var driver = new Fakes.FakeKafkaDriver();
        var admin = new FakeKafkaAdminClient();
        var process = new AddBrokerProcess(
            etcd, [Ep], driver, claims, journal, portLock, portAllocIndex,
            new FakeAdminFactory(admin),
            new ProvisioningOptions(16000, 16999, 600, 90, null, "apache/kafka:4.0.0"));
        return new Rig(etcd, driver, admin, claims, journal, process);
    }

    private sealed class FakeAdminFactory(FakeKafkaAdminClient client) : IKafkaAdminClientFactory
    {
        public IKafkaAdminClient Create(string bootstrap, string user, string password) => client;
    }

    private static void ReadyCluster(FakeKafkaAdminClient admin, int brokers)
        => admin.ClusterView = new KafkaClusterView(
            Enumerable.Range(1, brokers).Select(i => new KafkaBrokerView(i, $"broker{i}")).ToList(),
            ControllerId: 1);

    private static void SeedPendingBroker(Fakes.FakeEtcd etcd)
    {
        etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "NOT_INITIALIZED");
        etcd.Seed("/kafka/clusters/events/brokers/broker4/resources",
            """{"cpu":"1","mem":"2Gi","disk":"20Gi"}""");
    }

    [Fact]
    public async Task Run_PendingBroker_ContainerRoleEndpointsRunning()
    {
        // Arrange: Active-кластер + заявка broker4; DescribeCluster уже видит 4 брокеров.
        var rig = await NewRig();
        SeedPendingBroker(rig.Etcd);
        ReadyCluster(rig.Admin, 4);

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: контейнер broker-only (кворум НЕ включает 4@), порт закреплён,
        // endpoints пополнен адресом, state=RUNNING, роль зафиксирована.
        result.IsSuccess.Should().BeTrue();
        var spec = rig.Driver.Ensured.Single(s => s.NodeName == "broker4");
        spec.Env["KAFKA_PROCESS_ROLES"].Should().Be("broker");
        spec.Env["KAFKA_CONTROLLER_QUORUM_VOTERS"].Should().Be("1@broker1:9093,2@broker2:9093,3@broker3:9093");
        spec.Env["KAFKA_LISTENERS"].Should().NotContain("CONTROLLER");
        spec.ClientHostPort.Should().BeGreaterThan(16002);
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker4/role"].Value.Should().Be("broker");
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker4/state"].Value.Should().Be("RUNNING");
        var endpoints = rig.Etcd.Store["/kafka/clusters/events/endpoints"].Value;
        endpoints.Should().Contain("h1:16000"); // прежние адреса не потеряны
        endpoints.Split(',').Should().HaveCount(4); // адрес broker4 добавлен к трём
        rig.Etcd.Store["/kafkaworker/portalloc/events"].Value.Should().Contain("broker4");
    }

    [Fact]
    public async Task Run_ClusterNotReadyYet_InProgressNextTick()
    {
        // Arrange: broker4 заявлен, но DescribeCluster ещё видит 3 брокеров.
        var rig = await NewRig();
        SeedPendingBroker(rig.Etcd);
        ReadyCluster(rig.Admin, 3);

        // Act
        var first = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: контейнер создан, state=PROVISIONING, endpoints ещё не тронут
        // (RMW только по факту появления в кластере); вызов успешен (InProgress).
        first.IsSuccess.Should().BeTrue();
        rig.Driver.Ensured.Should().ContainSingle(s => s.NodeName == "broker4");
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker4/state"].Value.Should().Be("PROVISIONING");
        rig.Etcd.Store["/kafka/clusters/events/endpoints"].Value
            .Should().Be("h1:16000,h1:16001,h1:16002");

        // Act-2: брокер поднялся — тик доводит до RUNNING + endpoints.
        ReadyCluster(rig.Admin, 4);
        var second = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert
        second.IsSuccess.Should().BeTrue();
        rig.Etcd.Store["/kafka/clusters/events/brokers/broker4/state"].Value.Should().Be("RUNNING");
        rig.Etcd.Store["/kafka/clusters/events/endpoints"].Value.Split(',').Should().HaveCount(4);
    }

    [Fact]
    public async Task Run_PinnedBrokerPortBusyByOwnContainer_NotReallocated()
    {
        // Arrange: broker1 закреплён на 16000 и его контейнер публикует порт —
        // busy содержит (h1,16000); заявка broker4 (регрессия: полный план
        // перевыделял broker1 на 16001 и рассинхронил portalloc/endpoints).
        var rig = await NewRig();
        rig.Driver.BusyPorts = new HashSet<(string, int)> { ("h1", 16000), ("h1", 16001), ("h1", 16002) };
        SeedPendingBroker(rig.Etcd);
        ReadyCluster(rig.Admin, 4);

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: broker1 остался на 16000; broker4 — следующий свободный.
        result.IsSuccess.Should().BeTrue();
        var portAlloc = rig.Etcd.Store["/kafkaworker/portalloc/events"].Value;
        portAlloc.Should().Contain("\"broker1\":{\"host\":\"h1\",\"client\":16000}");
        portAlloc.Should().Contain("broker4");
        rig.Etcd.Store["/kafka/clusters/events/endpoints"].Value
            .Should().StartWith("h1:16000");
    }

    [Fact]
    public async Task Run_NoPendingBrokers_NoOp()
    {
        // Arrange: Active-кластер без заявок.
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 3);

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: никаких docker-операций, endpoints не тронут.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Etcd.Store["/kafka/clusters/events/endpoints"].Value
            .Should().Be("h1:16000,h1:16001,h1:16002");
    }

    [Fact]
    public async Task Run_NotClaimed_Refuses()
    {
        // Arrange: клэйм не захвачен.
        var etcd = new Fakes.FakeEtcd();
        SeedActive(etcd);
        SeedPendingBroker(etcd);
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        var portLock = new PortAllocLock([Ep], etcd, TimeProvider.System, claims.InstanceId);
        var portAllocIndex = new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);
        var process = new AddBrokerProcess(
            etcd, [Ep], new Fakes.FakeKafkaDriver(), claims, new WorkJournal(etcd, [Ep]),
            portLock, portAllocIndex,
            new FakeAdminFactory(new FakeKafkaAdminClient()), ProvisioningOptions.Default);

        // Act
        var result = await process.RunAsync(await Snapshot(etcd), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("клэйм не наш");
    }

    // AAA (t91): клэйм занят чужим инстансом при недоборе портов broker4 →
    // журнальная фаза waiting-portalloc-lock, успех тика, без мутаций.
    [Fact]
    public async Task Run_PortLockBusy_WaitingPhase_NoMutations()
    {
        // Arrange: Active-кластер + заявка broker4; клэйм держит «другой инстанс».
        var rig = await NewRig();
        SeedPendingBroker(rig.Etcd);
        ReadyCluster(rig.Admin, 4);
        rig.Etcd.Seed("/kafkaworker/locks/portalloc", """{"instance":"inst-2","since_unix":1}""");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: InProgress — успех без мутаций portalloc/docker/endpoints.
        result.IsSuccess.Should().BeTrue();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-portalloc-lock");
        rig.Etcd.Store["/kafkaworker/portalloc/events"].Value.Should().NotContain("broker4");
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Etcd.Store["/kafka/clusters/events/endpoints"].Value
            .Should().Be("h1:16000,h1:16001,h1:16002");
    }

    // AAA (t91): чужая portalloc-запись — занятость добора: broker4 не получает
    // порт, закреплённый соседом (окно «сосед записал, контейнеров нет»).
    [Fact]
    public async Task Run_ForeignPortAllocRecord_PortIsNotReused()
    {
        // Arrange: сосед shop1 закрепил h1:16003; docker-busy пуст.
        var rig = await NewRig();
        SeedPendingBroker(rig.Etcd);
        ReadyCluster(rig.Admin, 4);
        rig.Etcd.Seed("/kafkaworker/portalloc/shop1",
            """{"broker1":{"host":"h1","client":16003}}""");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: broker4 — следующий свободный после своих и соседа (16004).
        result.IsSuccess.Should().BeTrue();
        var spec = rig.Driver.Ensured.Single(s => s.NodeName == "broker4");
        spec.ClientHostPort.Should().Be(16004);
        rig.Etcd.Store["/kafkaworker/portalloc/events"].Value
            .Should().Contain("\"broker4\":{\"host\":\"h1\",\"client\":16004}");
    }
}
