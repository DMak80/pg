using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.UnitTests.Provisioning;

// RemoveBrokerProcess (arch/16 §5 G): guards (controller/последний/партиции),
// демонтаж контейнера+тома, очистка ключей brokers/<b>/, RMW endpoints,
// portalloc-фильтрация, идемпотентность повтора.

public class RemoveBrokerProcessTests
{
    private const string Ep = "http://etcd:2379";

    private sealed record Rig(
        Fakes.FakeEtcd Etcd,
        Fakes.FakeKafkaDriver Driver,
        FakeKafkaAdminClient Admin,
        ClaimStore Claims,
        WorkJournal Journal,
        RemoveBrokerProcess Process);

    // Active-кластер events: broker1..3 controller + broker4 broker-only (демонтаж — broker4).
    private static void SeedActive(Fakes.FakeEtcd etcd)
    {
        etcd.Seed("/kafka/clusters/events/config",
            """{"brokers":4,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}""");
        for (var k = 1; k <= 4; k++)
        {
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/state", "RUNNING");
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/role", k <= 3 ? "controller" : "broker");
        }

        etcd.Seed("/kafka/clusters/events/endpoints", "h1:16000,h1:16001,h1:16002,h1:16003");
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.SeedSecurity("events");
        etcd.Seed("/kafka/clusters/events/app_password", "OldPassword0123456789abcdef");
        etcd.Seed("/kafkaworker/portalloc/events",
            """{"broker1":{"host":"h1","client":16000},"broker2":{"host":"h1","client":16001},"broker3":{"host":"h1","client":16002},"broker4":{"host":"h1","client":16003}}""");
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
        var driver = new Fakes.FakeKafkaDriver();
        driver.NodeObjects.AddRange(Enumerable.Range(1, 4).Select(k => $"kfw-events-broker{k}"));
        var admin = new FakeKafkaAdminClient();
        var process = new RemoveBrokerProcess(
            etcd, [Ep], driver, claims, journal, new FakeAdminFactory(admin), ProvisioningOptions.Default);
        return new Rig(etcd, driver, admin, claims, journal, process);
    }

    private sealed class FakeAdminFactory(FakeKafkaAdminClient client) : IKafkaAdminClientFactory
    {
        public IKafkaAdminClient Create(string bootstrap, string user, string password, string? caPem) => client;
    }

    private static void ReadyCluster(FakeKafkaAdminClient admin, int brokers)
        => admin.ClusterView = new KafkaClusterView(
            Enumerable.Range(1, brokers).Select(i => new KafkaBrokerView(i, $"broker{i}")).ToList(),
            ControllerId: 1);

    [Fact]
    public async Task Run_BrokerOnlyEmpty_ContainerVolumeKeysRemoved()
    {
        // Arrange: маркер TO_REMOVE на пустом broker-only broker4; топиков с
        // репликами на нём нет.
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 4);
        rig.Admin.Topics =
        [
            new KafkaTopicView("orders", 2, [[1, 2], [2, 3]]), // реплики только на 1..3
        ];
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: контейнер+том удалены, ключи brokers/broker4/ исчезли, endpoints
        // сократился, portalloc отфильтрован, journal done.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().Contain(("broker4", true));
        rig.Etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/kafka/clusters/events/brokers/broker4/"));
        rig.Etcd.Store["/kafka/clusters/events/endpoints"].Value
            .Should().Be("h1:16000,h1:16001,h1:16002");
        rig.Etcd.Store["/kafkaworker/portalloc/events"].Value
            .Should().NotContain("broker4").And.Contain("broker1");
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("done");
    }

    [Fact]
    public async Task Run_ControllerBroker_Refused()
    {
        // Arrange: TO_REMOVE на controller-ноде (роль фиксируется навсегда).
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 4);
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker1/state", "TO_REMOVE");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: отказ до docker-мутаций; журнал несёт причину.
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("controller");
        rig.Driver.Removed.Should().BeEmpty();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.LastError.Should().Contain("controller");
    }

    [Fact]
    public async Task Run_LastRemainingBroker_Refused()
    {
        // Arrange: TO_REMOVE на ВСЕХ брокерах — демонтаж опустошит кластер.
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 4);
        for (var k = 1; k <= 4; k++)
            rig.Etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/state", "TO_REMOVE");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: отказ «последний брокер» без docker-мутаций.
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("последний");
        rig.Driver.Removed.Should().BeEmpty();
    }

    [Fact]
    public async Task Run_BrokerHasPartitions_WaitsInJournal()
    {
        // Arrange: на broker4 есть реплики партиций (roadmap t02 ещё не умеет
        // reassignment) — процесс ждёт, не ломая кластер.
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 4);
        rig.Admin.Topics =
        [
            new KafkaTopicView("orders", 2, [[1, 2], [2, 4]]), // партиция 1 держит реплику на 4
        ];
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: успех (InProgress-семантика: guard не ошибка, а ожидание);
        // ничего не демонтировано; журнал отражает ожидание.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().BeEmpty();
        rig.Etcd.Store.Keys.Should().Contain("/kafka/clusters/events/brokers/broker4/state");
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-partitions");
    }

    [Fact]
    public async Task HasPartitions_учитывает_internal_топики()
    {
        // Arrange: broker3 broker-only TO_REMOVE, реплики ТОЛЬКО в
        // __consumer_offsets — guard обязан видеть internal-реплики и не
        // отпускать брокер мимо drain (регресс t02 §1, фикс describe-all).
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 4);
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker3/role", "broker");
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker3/state", "TO_REMOVE");
        rig.Admin.Topics = [new KafkaTopicView("__consumer_offsets", 1, [[1, 2, 3]])];

        // Act
        var result = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: демонтажа нет, journal waiting-partitions. Фейк не фильтрует
        // __-топики (фильтрация живёт в реальном адаптере, Task 1.2) — тест
        // фиксирует контракт guard'а; реальный describe-all подтверждает T7.1.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().BeEmpty();
        var state = await rig.Journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-partitions");
    }

    [Fact]
    public async Task Run_RepeatAfterRemoval_IdempotentDone()
    {
        // Arrange: первый прогон демонтировал broker4; маркер исчез вместе с ключами.
        var rig = await NewRig();
        ReadyCluster(rig.Admin, 4);
        rig.Etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "TO_REMOVE");
        (await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None))
            .IsSuccess.Should().BeTrue();

        // Act: повторный тик — кандидатов нет.
        var second = await rig.Process.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: no-op, docker не дёргается повторно.
        second.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().ContainSingle().Which.Node.Should().Be("broker4");
        rig.Etcd.Store["/kafka/clusters/events/endpoints"].Value
            .Should().Be("h1:16000,h1:16001,h1:16002");
    }
}
