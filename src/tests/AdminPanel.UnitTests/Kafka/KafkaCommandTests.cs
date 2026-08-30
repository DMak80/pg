using AdminPanel.Api.Operations.Kafka;
using AdminPanel.Api.Operations;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Xunit;
using static AdminPanel.UnitTests.Kafka.KafkaCommandHarness;

namespace AdminPanel.UnitTests.Kafka;

// Хендлеры kafka-мутаций (arch/02 §10.2): клэймы/RMW/guard'ы/идемпотентность.
public class CreateKafkaClusterCommandTests
{
    private static CreateKafkaClusterCommandHandler Handler(FakeKafkaEtcd etcd)
        => new(StoreWithEndpoint(), etcd, TimeProvider.System);

    [Fact]
    public async Task Handle_Valid_ClaimThenBrokerPuts()
    {
        // Arrange
        var etcd = new FakeKafkaEtcd();

        // Act
        var result = await Handler(etcd).Handle(
            new CreateKafkaClusterCommand(new CreateKafkaClusterRequest("events")), CancellationToken.None);

        // Assert: config через txn-клэйм (version==0); state/resources на каждого брокера.
        result.IsSuccess.Should().BeTrue();
        result.Value.Name.Should().Be("events");
        result.Value.State.Should().Be("NOT_INITIALIZED");
        result.Value.Brokers.Should().Be(3);
        etcd.Store["/kafka/clusters/events/config"].Value.Should().Contain("\"state\":\"NOT_INITIALIZED\"");
        etcd.Store["/kafka/clusters/events/brokers/broker1/state"].Value.Should().Be("NOT_INITIALIZED");
        etcd.Store["/kafka/clusters/events/brokers/broker3/resources"].Value.Should().Contain("\"cpu\":\"2\"");
        etcd.Txns.Should().ContainSingle(t => t.Compares.Any(c => c.Key == "/kafka/clusters/events/config"));
    }

    [Fact]
    public async Task Handle_NameTaken_409()
    {
        // Arrange: config уже существует — клэйм проигрывает.
        var etcd = new FakeKafkaEtcd();
        etcd.Seed("/kafka/clusters/events/config", "{}");

        // Act
        var result = await Handler(etcd).Handle(
            new CreateKafkaClusterCommand(new CreateKafkaClusterRequest("events")), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<KafkaClusterAlreadyExistsException>();
        etcd.Store.Keys.Should().NotContain(k => k.EndsWith("brokers/broker1/state"));
    }

    [Fact]
    public async Task Handle_InvalidFields_400()
    {
        // Arrange: RF > brokers.
        var etcd = new FakeKafkaEtcd();

        // Act
        var result = await Handler(etcd).Handle(
            new CreateKafkaClusterCommand(new CreateKafkaClusterRequest("events", Brokers: 1, ReplicationFactor: 3)),
            CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaValidationException>()
            .Which.Errors.Should().NotBeEmpty();
    }

    [Fact]
    public async Task Handle_NoEndpoint_503()
    {
        // Arrange: снапшот пуст — ActiveEndpoint нет.
        var store = new SnapshotStore();
        var handler = new CreateKafkaClusterCommandHandler(store, new FakeKafkaEtcd(), TimeProvider.System);

        // Act
        var result = await handler.Handle(
            new CreateKafkaClusterCommand(new CreateKafkaClusterRequest("events")), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<EtcdWriteUnavailableException>();
    }
}

public class DeleteKafkaClusterCommandTests
{
    private static DeleteKafkaClusterCommandHandler Handler(FakeKafkaEtcd etcd)
        => new(StoreWithEndpoint(), etcd);

    [Fact]
    public async Task Handle_ActiveCluster_MarksToRemoveKeepsFields()
    {
        // Arrange
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(new DeleteKafkaClusterCommand("events"), CancellationToken.None);

        // Assert: state=TO_REMOVE, прочие поля сохранены.
        result.IsSuccess.Should().BeTrue();
        var config = etcd.Store["/kafka/clusters/events/config"].Value;
        config.Should().Contain("\"state\":\"TO_REMOVE\"").And.Contain("\"brokers\":3")
            .And.Contain("\"created_unix\":1756500000");
    }

    [Fact]
    public async Task Handle_AlreadyToRemove_IdempotentNoWrite()
    {
        // Arrange
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        var config = etcd.Store["/kafka/clusters/events/config"];
        etcd.Seed("/kafka/clusters/events/config",
            config.Value.Replace("}", ",\"state\":\"TO_REMOVE\"}"));

        // Act
        var result = await Handler(etcd).Handle(new DeleteKafkaClusterCommand("events"), CancellationToken.None);

        // Assert: 204 без записи (revision не изменился бы — значение то же).
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/kafka/clusters/events/config"].Value.Should().Contain("TO_REMOVE");
    }

    [Fact]
    public async Task Handle_MissingCluster_404()
    {
        // Arrange: пустой etcd.
        var etcd = new FakeKafkaEtcd();

        // Act
        var result = await Handler(etcd).Handle(new DeleteKafkaClusterCommand("ghost"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaClusterNotFoundException>();
    }
}

public class UpdateKafkaConfigCommandTests
{
    private static UpdateKafkaConfigCommandHandler Handler(FakeKafkaEtcd etcd)
        => new(StoreWithEndpoint(), etcd);

    [Fact]
    public async Task Handle_Valid_RmwByModRevision()
    {
        // Arrange
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new UpdateKafkaConfigCommand("events",
                new KafkaConfigUpdateRequest(DefaultRetentionMs: 86400000)), CancellationToken.None);

        // Assert: только retention изменился; txn-compare по mod_revision.
        result.IsSuccess.Should().BeTrue();
        result.Value.DefaultRetentionMs.Should().Be(86400000);
        result.Value.ReplicationFactor.Should().Be(3); // не тронуто
        etcd.Txns.Should().Contain(t => t.Compares.Any(c => c.ModRevision != null));
        etcd.Store["/kafka/clusters/events/config"].Value.Should().Contain("\"default_retention_ms\":86400000");
    }

    [Fact]
    public async Task Handle_ConcurrentModification_503Retry()
    {
        // Arrange: config переписан после чтения (мод-ревизия ушла).
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed("/kafka/clusters/events/config",
            etcd.Store["/kafka/clusters/events/config"].Value); // +1 revision мимо команды

        // Act: txn compare по УСТАРЕВШЕЙ revision — но хендлер читает свежую...
        // Симулируем гонку: портим compare после чтения нельзя — вместо этого
        // проверяем конкурентную запись МЕЖДУ чтением и txn: перезапишем ключ
        // в txn-ветке fake (инкремент ревизии до compare).
        etcd.OnTxn = () => etcd.Seed("/kafka/clusters/events/config",
            etcd.Store["/kafka/clusters/events/config"].Value);
        var result = await Handler(etcd).Handle(
            new UpdateKafkaConfigCommand("events",
                new KafkaConfigUpdateRequest(DefaultPartitions: 6)), CancellationToken.None);

        // Assert: compare не сошёлся — конкурентная запись, повтор клиентом.
        result.Error.Should().BeOfType<KafkaConcurrentWriteException>();
    }

    [Fact]
    public async Task Handle_NotActive_409()
    {
        // Arrange: кластер в NOT_INITIALIZED.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed("/kafka/clusters/events/config",
            etcd.Store["/kafka/clusters/events/config"].Value.Replace("}", ",\"state\":\"NOT_INITIALIZED\"}"));

        // Act
        var result = await Handler(etcd).Handle(
            new UpdateKafkaConfigCommand("events",
                new KafkaConfigUpdateRequest(DefaultPartitions: 6)), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaClusterNotActiveException>();
    }

    [Fact]
    public async Task Handle_EmptyUpdate_400()
    {
        // Arrange: ни одного поля.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new UpdateKafkaConfigCommand("events", new KafkaConfigUpdateRequest()), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaValidationException>();
    }

    [Fact]
    public async Task Handle_MinIsrAboveNewRf_400()
    {
        // Arrange: minISR текущий 2; RF снижаем до 1 — межполевая валидация.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new UpdateKafkaConfigCommand("events",
                new KafkaConfigUpdateRequest(ReplicationFactor: 1)), CancellationToken.None);

        // Assert: эффективный minISR (2) > эффективного RF (1).
        result.Error.Should().BeOfType<KafkaValidationException>()
            .Which.Errors.Should().Contain(e => e.Field == "minInSyncReplicas");
    }
}

public class AddKafkaBrokerCommandTests
{
    private static AddKafkaBrokerCommandHandler Handler(FakeKafkaEtcd etcd)
        => new(StoreWithEndpoint(), etcd);

    [Fact]
    public async Task Handle_ActiveCluster_GeneratesBroker4AndClaims()
    {
        // Arrange
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new AddKafkaBrokerCommand("events", new AddKafkaBrokerRequest(Cpu: 1m, MemGi: 2, DiskGi: 20)),
            CancellationToken.None);

        // Assert: имя broker<max+1>; клэйм-txn version(state)==0 + resources.
        result.IsSuccess.Should().BeTrue();
        result.Value.Name.Should().Be("broker4");
        result.Value.State.Should().Be("NOT_INITIALIZED");
        etcd.Txns.Should().Contain(t => t.Compares.Any(c =>
            c.Key == "/kafka/clusters/events/brokers/broker4/state" && c.Version == 0));
        etcd.Store["/kafka/clusters/events/brokers/broker4/state"].Value.Should().Be("NOT_INITIALIZED");
        etcd.Store["/kafka/clusters/events/brokers/broker4/resources"].Value
            .Should().Contain("\"cpu\":\"1\"");
    }

    [Fact]
    public async Task Handle_LimitNine_409()
    {
        // Arrange: уже 9 брокеров.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd, brokers: 9);

        // Act
        var result = await Handler(etcd).Handle(
            new AddKafkaBrokerCommand("events", new AddKafkaBrokerRequest()), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaBrokerLimitException>();
    }

    [Fact]
    public async Task Handle_ClaimLost_409()
    {
        // Arrange: конкурентный POST занимает broker4 МЕЖДУ чтением списка
        // брокеров и клэйм-txn (гонка TOCTOU ловится compare version==0).
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.OnTxn = () =>
        {
            etcd.Seed("/kafka/clusters/events/brokers/broker4/state", "NOT_INITIALIZED");
            etcd.OnTxn = null;
        };

        // Act
        var result = await Handler(etcd).Handle(
            new AddKafkaBrokerCommand("events", new AddKafkaBrokerRequest()), CancellationToken.None);

        // Assert: клэйм проигран — 409, ключи брокера не дописаны.
        result.Error.Should().BeOfType<KafkaBrokerNameTakenException>();
        etcd.Store.Should().NotContainKey("/kafka/clusters/events/brokers/broker4/resources");
    }
}

public class RemoveKafkaBrokerCommandTests
{
    private static RemoveKafkaBrokerCommandHandler Handler(FakeKafkaEtcd etcd)
        => new(StoreWithEndpoint(), etcd);

    [Fact]
    public async Task Handle_BrokerOnly_MarksToRemove()
    {
        // Arrange: 4 брокера, broker4 — broker-only.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd, brokers: 4);

        // Act
        var result = await Handler(etcd).Handle(
            new RemoveKafkaBrokerCommand("events", "broker4"), CancellationToken.None);

        // Assert: маркер TO_REMOVE (one-way).
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/kafka/clusters/events/brokers/broker4/state"].Value.Should().Be("TO_REMOVE");
    }

    [Fact]
    public async Task Handle_Controller_409()
    {
        // Arrange: broker1 — controller (роль фиксируется навсегда).
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd, brokers: 4);

        // Act
        var result = await Handler(etcd).Handle(
            new RemoveKafkaBrokerCommand("events", "broker1"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaBrokerIsControllerException>();
        etcd.Store["/kafka/clusters/events/brokers/broker1/state"].Value.Should().Be("RUNNING");
    }

    [Fact]
    public async Task Handle_LastBrokerWithoutControllerRole_409()
    {
        // Arrange: кластер из одного брокера с нетипичной ролью (изоляция guard'а
        // «последний» от controller-guard'а).
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd, brokers: 1);
        etcd.Seed("/kafka/clusters/events/brokers/broker1/role", "broker");

        // Act
        var result = await Handler(etcd).Handle(
            new RemoveKafkaBrokerCommand("events", "broker1"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaLastBrokerException>();
    }

    [Fact]
    public async Task Handle_UnknownBroker_404()
    {
        // Arrange
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new RemoveKafkaBrokerCommand("events", "broker9"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaBrokerNotFoundException>();
    }
}

public class RotateKafkaPasswordCommandTests
{
    private static RotateKafkaPasswordCommandHandler Handler(FakeKafkaEtcd etcd)
        => new(StoreWithEndpoint(), etcd);

    [Fact]
    public async Task Handle_ActiveCluster_ClaimsRotationTicket()
    {
        // Arrange
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new RotateKafkaPasswordCommand("events", "admin"), CancellationToken.None);

        // Assert: заявка /kafkaworker/rotations/events через клэйм-txn.
        result.IsSuccess.Should().BeTrue();
        result.Value.RequestedBy.Should().Be("admin");
        etcd.Store["/kafkaworker/rotations/events"].Value
            .Should().Contain("\"requested_by\":\"admin\"");
        etcd.Txns.Should().Contain(t => t.Compares.Any(c =>
            c.Key == "/kafkaworker/rotations/events" && c.Version == 0));
    }

    [Fact]
    public async Task Handle_TicketAlreadyLive_409()
    {
        // Arrange: заявка уже стоит.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed("/kafkaworker/rotations/events",
            """{"requested_unix":1750000200,"requested_by":"ops"}""");

        // Act
        var result = await Handler(etcd).Handle(
            new RotateKafkaPasswordCommand("events", "admin"), CancellationToken.None);

        // Assert: панель не перезаписывает живые заявки.
        result.Error.Should().BeOfType<KafkaRotationAlreadyRequestedException>();
        etcd.Store["/kafkaworker/rotations/events"].Value.Should().Contain("ops");
    }

    [Fact]
    public async Task Handle_NotActive_409()
    {
        // Arrange: кластер удаляется.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed("/kafka/clusters/events/config",
            etcd.Store["/kafka/clusters/events/config"].Value.Replace("}", ",\"state\":\"TO_REMOVE\"}"));

        // Act
        var result = await Handler(etcd).Handle(
            new RotateKafkaPasswordCommand("events", "admin"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaClusterNotActiveException>();
    }
}

// ===== Rebalance: заявка и отмена ребалансировки (t02, 02 §10.2-9/10) =====

public class RebalanceCommandTests
{
    private static RequestKafkaRebalanceCommandHandler Handler(FakeKafkaEtcd etcd)
        => new(StoreWithEndpoint(), etcd);

    private static CancelKafkaRebalanceCommandHandler CancelHandler(FakeKafkaEtcd etcd)
        => new(StoreWithEndpoint(), etcd);

    [Fact]
    public async Task RequestRebalance_ActiveCluster_ClaimsTicket()
    {
        // Arrange: Active-кластер без живой заявки.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new RequestKafkaRebalanceCommand("events", "admin"), CancellationToken.None);

        // Assert: заявка /kafkaworker/rebalances/events через клэйм-txn.
        result.IsSuccess.Should().BeTrue();
        result.Value.RequestedBy.Should().Be("admin");
        etcd.Store["/kafkaworker/rebalances/events"].Value
            .Should().Contain("\"requested_by\":\"admin\"");
        etcd.Txns.Should().Contain(t => t.Compares.Any(c =>
            c.Key == "/kafkaworker/rebalances/events" && c.Version == 0));
    }

    [Fact]
    public async Task RequestRebalance_TicketAlreadyLive_409()
    {
        // Arrange: заявка уже стоит — панель не перезаписывает.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed("/kafkaworker/rebalances/events",
            """{"requested_unix":1750000200,"requested_by":"ops"}""");

        // Act
        var result = await Handler(etcd).Handle(
            new RequestKafkaRebalanceCommand("events", "admin"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaRebalanceAlreadyRequestedException>();
        etcd.Store["/kafkaworker/rebalances/events"].Value.Should().Contain("ops");
    }

    [Fact]
    public async Task RequestRebalance_NotActive_409()
    {
        // Arrange: кластер удаляется.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed("/kafka/clusters/events/config",
            etcd.Store["/kafka/clusters/events/config"].Value.Replace("}", ",\"state\":\"TO_REMOVE\"}"));

        // Act
        var result = await Handler(etcd).Handle(
            new RequestKafkaRebalanceCommand("events", "admin"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaClusterNotActiveException>();
    }

    [Fact]
    public async Task CancelRebalance_RemovesTicket()
    {
        // Arrange: живая заявка.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed("/kafkaworker/rebalances/events",
            """{"requested_unix":1750000200,"requested_by":"ops"}""");

        // Act
        var result = await CancelHandler(etcd).Handle(
            new CancelKafkaRebalanceCommand("events"), CancellationToken.None);

        // Assert: заявка снята (поданные батчи Kafka доиграет сама).
        result.IsSuccess.Should().BeTrue();
        etcd.Store.Should().NotContainKey("/kafkaworker/rebalances/events");
    }

    [Fact]
    public async Task CancelRebalance_NoTicket_404()
    {
        // Arrange: пустой etcd.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await CancelHandler(etcd).Handle(
            new CancelKafkaRebalanceCommand("events"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaRebalanceNotFoundException>();
    }
}
