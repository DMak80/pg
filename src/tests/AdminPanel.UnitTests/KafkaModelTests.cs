using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using Inspection = AdminPanel.Api.Inspection;
using FluentAssertions;
using Xunit;
using KafkaMappers = AdminPanel.Api.Inspection.KafkaMappers;

namespace AdminPanel.UnitTests;

// Модель домен-снапшота Kafka (arch/02 §10.1; план B2): value-equality записей
// и маппинг config.state → KafkaClusterState (arch/15 §2/§6).
public class KafkaModelTests
{
    [Fact]
    public void ScalarRecords_HaveValueEquality()
    {
        // Arrange: скалярные записи модели, собранные независимо.
        var brokerA = new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40);
        var brokerB = new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40);
        var topicA = new KafkaTopicInfo("orders", 12, 3, 604800000L, 2,
            new TopicDesiredDto(16, 86400000L, null, 1750000000, "admin"), false, 1750000100);
        var topicB = new KafkaTopicInfo("orders", 12, 3, 604800000L, 2,
            new TopicDesiredDto(16, 86400000L, null, 1750000000, "admin"), false, 1750000100);
        var rotationA = new KafkaRotationTicket("events", 1750000200, "admin");
        var rotationB = new KafkaRotationTicket("events", 1750000200, "admin");

        // Act / Assert: коллекции в записях сравниваются по ссылке (семантика
        // record + IReadOnlyList) — value-equality проверяем на скалярных записях.
        brokerA.Equals(brokerB).Should().BeTrue();
        topicA.Equals(topicB).Should().BeTrue();
        rotationA.Equals(rotationB).Should().BeTrue();
    }

    [Fact]
    public void Records_WithDifferentFields_AreNotEqual()
    {
        // Arrange: отличие в одном поле и в одной коллекции.
        var broker = new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40);
        var changedState = broker with { State = "UNREACHABLE" };
        var snapshot = BuildSnapshot();
        var changedCount = BuildSnapshot() with { UnknownKeyCount = 3 };

        // Act / Assert
        broker.Equals(changedState).Should().BeFalse();
        snapshot.Equals(changedCount).Should().BeFalse();
    }

    [Theory]
    [InlineData(null, KafkaClusterState.Active)]       // отсутствие state = Active (arch/15 §2)
    [InlineData("", KafkaClusterState.Active)]
    [InlineData("NOT_INITIALIZED", KafkaClusterState.NotInitialized)]
    [InlineData("TO_REMOVE", KafkaClusterState.ToRemove)]
    [InlineData("SOMETHING_NEW", KafkaClusterState.Active)] // незнакомое — толерантно, Active (arch/15 §6)
    public void ClusterStateMap_ParsesConfigState(string? raw, KafkaClusterState expected)
    {
        // Arrange / Act
        var state = KafkaClusterStates.Parse(raw);

        // Assert
        state.Should().Be(expected);
    }

    private static KafkaSnapshot BuildSnapshot() => new(
        new DateTimeOffset(2026, 8, 30, 12, 0, 0, TimeSpan.Zero),
        EtcdReachable: true,
        ConsecutiveFailures: 0,
        Clusters:
        [
            new KafkaClusterInfo(
                "events", KafkaClusterState.Active,
                Brokers: 3, ReplicationFactor: 3, MinInSyncReplicas: 2, DefaultPartitions: 12,
                DefaultRetentionMs: 604800000, CreatedUnix: 1756500000,
                Endpoints: "host.docker.internal:16001",
                BrokersList:
                [
                    new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40),
                    new KafkaBrokerInfo("broker2", "PROVISIONING", "controller", 2m, 4, 40),
                ],
                Topics:
                [
                    new KafkaTopicInfo("orders", 12, 3, 604800000L, 2,
                        new TopicDesiredDto(16, 86400000L, null, 1750000000, "admin"),
                        Missing: false, SyncedUnix: 1750000100),
                ]),
        ],
        Rotations: [new KafkaRotationTicket("events", 1750000200, "admin")],
        Rebalances: [],
        Reassignments: [],
        WorkerEndpoints: [],
        Probes: [],
        Alerts: [],
        ParseErrors: [new KeyParseError("/kafka/clusters/x/config", "bad json")],
        UnknownKeyCount: 1);
}

// Маппинг lifecycle-тикетов в DTO (t01, arch/03 §7.2): delete — к существующей
// строке, create без топика — «виртуальная» строка с факт-полями null/0.
public class KafkaMappersLifecycleTests
{
    [Fact]
    public void MapDetails_MergesTicketsAndAddsVirtualCreateRow()
    {
        // Arrange: кластер с топиком orders + тикеты orders:delete, audit:create.
        var cluster = new KafkaClusterInfo(
            "events", KafkaClusterState.Active, 3, 3, 2, 12, 604800000, 1756500000,
            "host.docker.internal:16001",
            [new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40)],
            Topics:
            [
                new KafkaTopicInfo("orders", 12, 3, 604800000L, 2,
                    Desired: null, Missing: false, SyncedUnix: 1750000100),
            ],
            LifecycleTickets:
            [
                new KafkaTopicLifecycleTicket("orders", "delete", null, null, null, null, 1750000100, "admin"),
                new KafkaTopicLifecycleTicket("audit", "create", 12, 3, 86400000L, 2, 1750000000, "admin"),
            ]);

        // Act
        var dto = KafkaMappers.MapDetails(cluster, [], [], []);

        // Assert: delete — бейдж у существующей строки orders; audit —
        // виртуальная строка с факт-полями null/0 и параметрами в lifecycle.
        dto.Topics.Should().HaveCount(2);
        var orders = dto.Topics.Single(t => t.Name == "orders");
        orders.Partitions.Should().Be(12);
        orders.Lifecycle!.Op.Should().Be("delete");
        orders.Lifecycle.RequestedBy.Should().Be("admin");

        var audit = dto.Topics.Single(t => t.Name == "audit");
        audit.Partitions.Should().Be(0, "виртуальная строка — факта нет");
        audit.ReplicationFactor.Should().BeNull();
        audit.RetentionMs.Should().BeNull();
        audit.MinInSyncReplicas.Should().BeNull();
        audit.Desired.Should().BeNull();
        audit.Missing.Should().BeFalse();
        audit.Lifecycle!.Op.Should().Be("create");
        audit.Lifecycle.Partitions.Should().Be(12);
        audit.Lifecycle.ReplicationFactor.Should().Be(3);
        audit.Lifecycle.RetentionMs.Should().Be(86400000L);
        audit.Lifecycle.MinInSyncReplicas.Should().Be(2);
    }

    [Fact]
    public void MapDetails_DeleteTicketWithoutTopic_NoVirtualRow()
    {
        // Arrange: delete-тикет на топике без факт-ключа (окно после del ключа) —
        // виртуальная строка не создаётся (спека §5.3: только create).
        var cluster = new KafkaClusterInfo(
            "events", KafkaClusterState.Active, 1, 1, 1, 12, 604800000, 1756500000,
            "host.docker.internal:16001",
            [new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40)],
            Topics: [],
            LifecycleTickets:
            [
                new KafkaTopicLifecycleTicket("gone", "delete", null, null, null, null, 1750000100, "admin"),
            ]);

        // Act
        var dto = KafkaMappers.MapDetails(cluster, [], [], []);

        // Assert
        dto.Topics.Should().BeEmpty();
    }

    [Fact]
    public void MapDetails_CollidingCreateAndDeleteTickets_NoThrowDeleteBadgeWins()
    {
        // Arrange: обе заявки живы на один топик (etcd-мусор, arch/15 §3.1 —
        // панельные клэймы txn на РАЗНЫЕ ключи, гонка двух API-запросов) при
        // живом факт-ключе: читатель обязан терпеть (arch/15 §6).
        var cluster = new KafkaClusterInfo(
            "events", KafkaClusterState.Active, 1, 1, 1, 12, 604800000, 1756500000,
            "host.docker.internal:16001",
            [new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40)],
            Topics:
            [
                new KafkaTopicInfo("orders", 12, 3, 604800000L, 2,
                    Desired: null, Missing: false, SyncedUnix: 1750000100),
            ],
            LifecycleTickets:
            [
                new KafkaTopicLifecycleTicket("orders", "create", 6, 1, null, null, 1750000000, "a"),
                new KafkaTopicLifecycleTicket("orders", "delete", null, null, null, null, 1750000100, "b"),
            ]);

        // Act: не бросает (ToDictionary на дубликате ронял GET в 500).
        var dto = KafkaMappers.MapDetails(cluster, [], [], []);

        // Assert: один бейдж; delete авторитетен (arch/15 §3.1 — delete
        // доминирует, create чистится воркером) — оператор видит/отменяет его.
        dto.Topics.Should().ContainSingle().Which.Name.Should().Be("orders");
        dto.Topics.Single().Lifecycle!.Op.Should().Be("delete");
        dto.Topics.Single().Lifecycle!.RequestedBy.Should().Be("b");
    }

    [Fact]
    public void MapDetails_CollidingTicketsWithoutTopic_SingleVirtualDeleteRow()
    {
        // Arrange: коллизия заявок на топик БЕЗ факт-ключа (пересоздание
        // отсутствующего + параллельное удаление) — виртуальная create-строка
        // не строится: авторитетный delete рендерится, исключений нет.
        var cluster = new KafkaClusterInfo(
            "events", KafkaClusterState.Active, 1, 1, 1, 12, 604800000, 1756500000,
            "host.docker.internal:16001",
            [new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40)],
            Topics: [],
            LifecycleTickets:
            [
                new KafkaTopicLifecycleTicket("audit", "create", 12, 3, null, null, 1750000000, "a"),
                new KafkaTopicLifecycleTicket("audit", "delete", null, null, null, null, 1750000100, "b"),
            ]);

        // Act
        var dto = KafkaMappers.MapDetails(cluster, [], [], []);

        // Assert: строк не добавлено (delete-виртуальных строк нет, спека §5.3),
        // читатель не упал.
        dto.Topics.Should().BeEmpty();
    }

    [Fact]
    public void MapDetails_NoTickets_LifecycleNull()
    {
        // Arrange: заявок нет.
        var cluster = new KafkaClusterInfo(
            "events", KafkaClusterState.Active, 1, 1, 1, 12, 604800000, 1756500000,
            "host.docker.internal:16001",
            [new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40)],
            Topics:
            [
                new KafkaTopicInfo("orders", 12, 3, 604800000L, 2,
                    Desired: null, Missing: false, SyncedUnix: 1750000100),
            ]);

        // Act
        var dto = KafkaMappers.MapDetails(cluster, [], [], []);

        // Assert
        dto.Topics.Single().Lifecycle.Should().BeNull();
    }
}

// Маппинг rebalance/reassignment в DTO деталей и сводки (t02, 03 §7.2).
public class KafkaRebalanceDtoMappingTests
{
    private static KafkaClusterInfo Cluster(string name = "events")
        => new(
            name, KafkaClusterState.Active,
            Brokers: 3, ReplicationFactor: 3, MinInSyncReplicas: 2, DefaultPartitions: 12,
            DefaultRetentionMs: 604800000, CreatedUnix: 1756500000,
            Endpoints: "host.docker.internal:16001",
            BrokersList: [new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40)],
            Topics: []);

    [Fact]
    public void MapDetails_WithTicketAndProgress_FillsFields()
    {
        // Arrange: заявка ребалансировки + drain-прогресс кластера events.
        var rebalances = new[] { new KafkaRebalanceTicket("events", 1750000200, "admin") };
        var reassignments = new[]
        {
            new KafkaReassignmentProgress("events", "drain", "broker4", 12, 5, 1750000215, null),
        };

        // Act
        var dto = Inspection.KafkaMappers.MapDetails(Cluster(), [], rebalances, reassignments);

        // Assert: DTO-поля заполнены по join-имени кластера.
        dto.Rebalance.Should().NotBeNull();
        dto.Rebalance!.RequestedUnix.Should().Be(1750000200);
        dto.Rebalance.RequestedBy.Should().Be("admin");
        dto.Reassignment.Should().NotBeNull();
        dto.Reassignment!.Mode.Should().Be("drain");
        dto.Reassignment.DrainBroker.Should().Be("broker4");
        dto.Reassignment.PartitionsTotal.Should().Be(12);
        dto.Reassignment.PartitionsRemaining.Should().Be(5);
        dto.Reassignment.UpdatedUnix.Should().Be(1750000215);
    }

    [Fact]
    public void MapDetails_NoTicketNoProgress_NullFields()
    {
        // Arrange: ни заявки, ни прогресса (чужой кластер в списках не мешает).
        var rebalances = new[] { new KafkaRebalanceTicket("shop", 1750000300, "ops") };
        var reassignments = new[]
        {
            new KafkaReassignmentProgress("shop", "balance", null, 8, 0, 1750000320, null),
        };

        // Act
        var dto = Inspection.KafkaMappers.MapDetails(Cluster(), [], rebalances, reassignments);

        // Assert: null = операции нет (03 §7.2).
        dto.Rebalance.Should().BeNull();
        dto.Reassignment.Should().BeNull();
    }

    [Fact]
    public void MapSummary_RebalancePendingFlag()
    {
        // Arrange / Act: сводка с живой заявкой и без.
        var pending = Inspection.KafkaMappers.MapSummary(Cluster(), rotationPending: false, rebalancePending: true);
        var idle = Inspection.KafkaMappers.MapSummary(Cluster(), rotationPending: false, rebalancePending: false);

        // Assert
        pending.RebalancePending.Should().BeTrue();
        idle.RebalancePending.Should().BeFalse();
    }
}
