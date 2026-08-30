using AdminPanel.Core;
using AdminPanel.Core.Kafka;
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
        Probes: [],
        Alerts: [],
        ParseErrors: [new KeyParseError("/kafka/clusters/x/config", "bad json")],
        UnknownKeyCount: 1);
}
