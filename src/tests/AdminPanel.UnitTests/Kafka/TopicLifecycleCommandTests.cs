using System.Text.Json;
using AdminPanel.Api.Operations.Kafka;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Xunit;
using static AdminPanel.UnitTests.Kafka.KafkaCommandHarness;

namespace AdminPanel.UnitTests.Kafka;

// Lifecycle-мутации топиков 9–12 (arch/02 §10.2-9..12, t01): клэйм-txn
// version==0 на desired.create/desired.delete, гварды (топик есть/missing/
// живые заявки/desired), идемпотентность DELETE, отмены заявок.
public class CreateKafkaTopicCommandTests
{
    private static CreateKafkaTopicCommandHandler Handler(FakeKafkaEtcd etcd)
        => new(StoreWithEndpoint(), etcd, TimeProvider.System);

    private static void SeedTopicKey(
        FakeKafkaEtcd etcd, string topic = "orders", bool missing = false, bool withDesired = false)
    {
        SeedActiveCluster(etcd);
        etcd.Seed($"/kafka/clusters/events/topics/{topic}",
            "{\"partitions\":3,\"replication_factor\":3"
            + ",\"configs\":{\"retention.ms\":\"604800000\"}"
            + (withDesired
                ? ",\"desired\":{\"partitions\":6},\"desired_unix\":1756500950,\"desired_by\":\"admin\""
                : "")
            + ",\"synced_unix\":1756500900"
            + ",\"missing\":" + (missing ? "true" : "false") + "}");
    }

    [Fact]
    public async Task Handle_NewTopic_ClaimPutWithDefaults()
    {
        // Arrange: Active-кластер, ключа topics/audit нет — создание разрешено.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new CreateKafkaTopicCommand("events", new CreateTopicRequest("audit"), "admin"),
            CancellationToken.None);

        // Assert: 201-успех; desired.create с развёрнутыми дефолтами config (12/3).
        result.IsSuccess.Should().BeTrue();
        result.Value.Topic.Should().Be("audit");
        result.Value.Partitions.Should().Be(12);
        result.Value.ReplicationFactor.Should().Be(3);
        var raw = etcd.Store["/kafka/clusters/events/topics/audit/desired.create"].Value;
        var value = JsonSerializer.Deserialize<JsonElement>(raw);
        value.GetProperty("partitions").GetInt32().Should().Be(12);
        value.GetProperty("replication_factor").GetInt32().Should().Be(3);
        value.GetProperty("requested_by").GetString().Should().Be("admin");
        value.TryGetProperty("configs", out _).Should().BeFalse("без retention/minISR — брокерные дефолты");
        etcd.Txns.Should().ContainSingle(t =>
            t.Compares.Any(c => c.Key == "/kafka/clusters/events/topics/audit/desired.create" && c.Version == 0));
    }

    [Fact]
    public async Task Handle_ExistingTopic_409()
    {
        // Arrange: факт-ключ topics/orders есть и не missing.
        var etcd = new FakeKafkaEtcd();
        SeedTopicKey(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new CreateKafkaTopicCommand("events", new CreateTopicRequest("orders"), "admin"),
            CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaTopicExistsException>();
    }

    [Fact]
    public async Task Handle_MissingTopicAllowed()
    {
        // Arrange: факт-ключ с missing=true и БЕЗ desired — «пересоздание».
        var etcd = new FakeKafkaEtcd();
        SeedTopicKey(etcd, missing: true);

        // Act
        var result = await Handler(etcd).Handle(
            new CreateKafkaTopicCommand("events", new CreateTopicRequest("orders", 3, 1), "admin"),
            CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        etcd.Store.Should().ContainKey("/kafka/clusters/events/topics/orders/desired.create");
    }

    [Fact]
    public async Task Handle_MissingTopicWithDesired_409()
    {
        // Arrange: missing=true с живым desired — сначала отменить конфиг-заявку.
        var etcd = new FakeKafkaEtcd();
        SeedTopicKey(etcd, missing: true, withDesired: true);

        // Act
        var result = await Handler(etcd).Handle(
            new CreateKafkaTopicCommand("events", new CreateTopicRequest("orders"), "admin"),
            CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaDesiredPendingException>();
    }

    [Fact]
    public async Task Handle_LiveCreateTicket_409()
    {
        // Arrange: desired.create уже стоит — клэйм не пройдёт (и раньше отсечём).
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed("/kafka/clusters/events/topics/audit/desired.create",
            """{"partitions":6,"replication_factor":3,"requested_unix":1750000000,"requested_by":"admin"}""");

        // Act
        var result = await Handler(etcd).Handle(
            new CreateKafkaTopicCommand("events", new CreateTopicRequest("audit"), "admin"),
            CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaLifecyclePendingException>();
    }

    [Fact]
    public async Task Handle_LiveDeleteTicket_409()
    {
        // Arrange: живая delete-заявка — создать нельзя (коллизия).
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed("/kafka/clusters/events/topics/audit/desired.delete",
            """{"requested_unix":1750000100,"requested_by":"admin"}""");

        // Act
        var result = await Handler(etcd).Handle(
            new CreateKafkaTopicCommand("events", new CreateTopicRequest("audit"), "admin"),
            CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaLifecyclePendingException>();
    }

    [Fact]
    public async Task Handle_NotActive_409_NoCluster_404_InvalidBody_400()
    {
        // Arrange: три негатива одного тела.
        var notActive = new FakeKafkaEtcd();
        SeedActiveCluster(notActive);
        notActive.Seed("/kafka/clusters/events/config",
            """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000,"state":"TO_REMOVE"}""");

        // Act / Assert
        (await Handler(notActive).Handle(
                new CreateKafkaTopicCommand("events", new CreateTopicRequest("x"), "admin"), CancellationToken.None))
            .Error.Should().BeOfType<KafkaClusterNotActiveException>();

        var noCluster = new FakeKafkaEtcd();
        (await Handler(noCluster).Handle(
                new CreateKafkaTopicCommand("ghost", new CreateTopicRequest("x"), "admin"), CancellationToken.None))
            .Error.Should().BeOfType<KafkaClusterNotFoundException>();

        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        (await Handler(etcd).Handle(
                new CreateKafkaTopicCommand("events", new CreateTopicRequest("x", 0), "admin"), CancellationToken.None))
            .Error.Should().BeOfType<KafkaValidationException>();
        (await Handler(etcd).Handle(
                new CreateKafkaTopicCommand("events", new CreateTopicRequest("__x"), "admin"), CancellationToken.None))
            .Error.Should().BeOfType<KafkaTopicNotFoundException>("неканоническое/internal имя — 404");
    }
}

public class DeleteKafkaTopicCommandTests
{
    private static DeleteKafkaTopicCommandHandler Handler(FakeKafkaEtcd etcd)
        => new(StoreWithEndpoint(), etcd, TimeProvider.System);

    private const string TicketKey = "/kafka/clusters/events/topics/orders/desired.delete";

    private static void SeedLiveTopic(FakeKafkaEtcd etcd)
    {
        SeedActiveCluster(etcd);
        etcd.Seed("/kafka/clusters/events/topics/orders",
            """{"partitions":3,"replication_factor":3,"configs":{"retention.ms":"604800000"},"synced_unix":1756500900,"missing":false}""");
    }

    [Fact]
    public async Task Handle_ExistingTopic_ClaimPutsDeleteTicket_IdempotentRepeat()
    {
        // Arrange: живой (не missing) топик.
        var etcd = new FakeKafkaEtcd();
        SeedLiveTopic(etcd);

        // Act
        var first = await Handler(etcd).Handle(
            new DeleteKafkaTopicCommand("events", "orders", "admin"), CancellationToken.None);
        var afterFirst = etcd.Store[TicketKey];
        var second = await Handler(etcd).Handle(
            new DeleteKafkaTopicCommand("events", "orders", "admin"), CancellationToken.None);

        // Assert: заявка поставлена клэйм-txn; повторный DELETE при живой
        // заявке — успех БЕЗ записи (value/mod_revision не изменились).
        first.IsSuccess.Should().BeTrue();
        second.IsSuccess.Should().BeTrue();
        var value = JsonSerializer.Deserialize<JsonElement>(afterFirst.Value);
        value.GetProperty("requested_by").GetString().Should().Be("admin");
        value.TryGetProperty("partitions", out _).Should().BeFalse("delete — только аудит");
        etcd.Store[TicketKey].ModRevision.Should().Be(afterFirst.ModRevision,
            "идемпотентный повтор не перезаписывает живую заявку");
        etcd.Store[TicketKey].Version.Should().Be(afterFirst.Version);
    }

    [Fact]
    public async Task Handle_MissingTopic_404()
    {
        // Arrange: missing-топик — удалять нечего.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed("/kafka/clusters/events/topics/orders",
            """{"partitions":3,"replication_factor":3,"configs":{"retention.ms":"604800000"},"synced_unix":1756500900,"missing":true}""");

        // Act
        var result = await Handler(etcd).Handle(
            new DeleteKafkaTopicCommand("events", "orders", "admin"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaTopicNotFoundException>();
    }

    [Fact]
    public async Task Handle_LiveCreateTicket_409()
    {
        // Arrange: живая create-заявка — сначала отменить её.
        var etcd = new FakeKafkaEtcd();
        SeedLiveTopic(etcd);
        etcd.Seed("/kafka/clusters/events/topics/orders/desired.create",
            """{"partitions":6,"replication_factor":3,"requested_unix":1750000000,"requested_by":"admin"}""");

        // Act
        var result = await Handler(etcd).Handle(
            new DeleteKafkaTopicCommand("events", "orders", "admin"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaLifecyclePendingException>();
    }

    [Fact]
    public async Task Handle_LiveDesired_409()
    {
        // Arrange: живая конфиг-заявка desired — явная отмена раньше удаления.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed("/kafka/clusters/events/topics/orders",
            """{"partitions":3,"replication_factor":3,"configs":{"retention.ms":"604800000"},"desired":{"partitions":6},"desired_unix":1756500950,"desired_by":"admin","synced_unix":1756500900,"missing":false}""");

        // Act
        var result = await Handler(etcd).Handle(
            new DeleteKafkaTopicCommand("events", "orders", "admin"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaDesiredPendingException>();
    }
}

public class CancelTopicLifecycleCommandTests
{
    private static CancelTopicLifecycleCommandHandler Handler(FakeKafkaEtcd etcd)
        => new(StoreWithEndpoint(), etcd);

    [Fact]
    public async Task Handle_CancelDelete_RemovesTicket()
    {
        // Arrange: живая delete-заявка (окно деструктивности).
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed("/kafka/clusters/events/topics/orders",
            """{"partitions":3,"replication_factor":3,"configs":{"retention.ms":"604800000"},"synced_unix":1756500900,"missing":false}""");
        etcd.Seed("/kafka/clusters/events/topics/orders/desired.delete",
            """{"requested_unix":1750000100,"requested_by":"admin"}""");

        // Act
        var result = await Handler(etcd).Handle(
            new CancelTopicLifecycleCommand("events", "orders", "delete"), CancellationToken.None);

        // Assert: заявка снята, факт-ключ цел.
        result.IsSuccess.Should().BeTrue();
        etcd.Store.Should().NotContainKey("/kafka/clusters/events/topics/orders/desired.delete");
        etcd.Store.Should().ContainKey("/kafka/clusters/events/topics/orders");
    }

    [Fact]
    public async Task Handle_CancelCreate_NoTicket_404()
    {
        // Arrange: заявки нет.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new CancelTopicLifecycleCommand("events", "audit", "create"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaLifecycleNotFoundException>();
    }

    [Fact]
    public async Task Handle_CancelBadOp_404()
    {
        // Arrange: неканонический op — как «заявки нет».
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new CancelTopicLifecycleCommand("events", "orders", "pause"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaLifecycleNotFoundException>();
    }
}
