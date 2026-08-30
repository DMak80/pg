using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using Inspection = AdminPanel.Api.Inspection;
using FluentAssertions;
using Xunit;

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
        Probes: [],
        Alerts: [],
        ParseErrors: [new KeyParseError("/kafka/clusters/x/config", "bad json")],
        UnknownKeyCount: 1);
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
