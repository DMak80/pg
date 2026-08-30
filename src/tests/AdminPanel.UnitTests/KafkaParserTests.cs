using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер префиксов /kafka/clusters/ и /kafkaworker/rotations/ (arch/15 §2–§4,
// arch/02 §10.1): толерантный разбор на фикстурах-примерах arch/15 §2.1.
public class KafkaParserTests
{
    [Fact]
    public void FullPrefix_TwoClustersWithTopicsAndBrokers()
    {
        // Arrange: сид events (Active, 3 брокера, topics) + pending (NOT_INITIALIZED).
        var kvs = EtcdFixtures.LoadKv("Kafka/kafka-clusters-full.json");

        // Act
        var parsed = KafkaParser.ParseClusters(kvs);

        // Assert
        parsed.Errors.Should().BeEmpty();
        parsed.UnknownKeyCount.Should().Be(0);
        parsed.Clusters.Should().HaveCount(2);

        var events = parsed.Clusters.Single(c => c.Name == "events");
        events.State.Should().Be(KafkaClusterState.Active);
        events.Brokers.Should().Be(3);
        events.ReplicationFactor.Should().Be(3);
        events.MinInSyncReplicas.Should().Be(2);
        events.DefaultPartitions.Should().Be(12);
        events.DefaultRetentionMs.Should().Be(604800000L);
        events.CreatedUnix.Should().Be(1756500000L);
        events.Endpoints.Should().Be("host.docker.internal:16001,host.docker.internal:16002,host.docker.internal:16003");

        events.BrokersList.Should().HaveCount(3);
        var broker1 = events.BrokersList.Single(b => b.Name == "broker1");
        broker1.State.Should().Be("RUNNING");
        broker1.Role.Should().Be("controller");
        broker1.Cpu.Should().Be(2m);
        broker1.MemGi.Should().Be(4);
        broker1.DiskGi.Should().Be(40);
        events.BrokersList.Single(b => b.Name == "broker3").State.Should().Be("PROVISIONING");

        events.Topics.Should().HaveCount(3);
        var orders = events.Topics.Single(t => t.Name == "orders");
        orders.Partitions.Should().Be(12);
        orders.ReplicationFactor.Should().Be(3);
        orders.RetentionMs.Should().Be(604800000L);
        orders.MinInSyncReplicas.Should().Be(2);
        orders.SyncedUnix.Should().Be(1750000100L);
        orders.Missing.Should().BeFalse();
        orders.Desired.Should().NotBeNull();
        orders.Desired!.Partitions.Should().Be(16);
        orders.Desired.RetentionMs.Should().Be(86400000L);
        orders.Desired.MinInSyncReplicas.Should().BeNull();
        orders.Desired.RequestedUnix.Should().Be(1750000000L);
        orders.Desired.RequestedBy.Should().Be("admin");

        var ghost = events.Topics.Single(t => t.Name == "ghost");
        ghost.Missing.Should().BeTrue();
        ghost.Desired.Should().NotBeNull();

        var pending = parsed.Clusters.Single(c => c.Name == "pending");
        pending.State.Should().Be(KafkaClusterState.NotInitialized);
        pending.Endpoints.Should().BeNull();
        pending.BrokersList.Should().HaveCount(3)
            .And.OnlyContain(b => b.State == "NOT_INITIALIZED" && b.Role == null);
        pending.Topics.Should().BeEmpty();
    }

    [Fact]
    public void BrokenValues_ProduceParseErrorsWithoutException()
    {
        // Arrange: битые config/resources/topics + частичный факт-топик (arch/15 §6).
        var kvs = EtcdFixtures.LoadKv("Kafka/kafka-clusters-broken.json");

        // Act
        var parsed = KafkaParser.ParseClusters(kvs);

        // Assert
        parsed.Errors.Should().Contain(e => e.Key == "/kafka/clusters/broken/config");
        parsed.Errors.Should().Contain(e => e.Key == "/kafka/clusters/broken/brokers/broker1/resources");
        parsed.Errors.Should().Contain(e => e.Key == "/kafka/clusters/broken/brokers/broker2/resources");
        parsed.Errors.Should().Contain(e => e.Key == "/kafka/clusters/broken/topics/bad");
        parsed.UnknownKeyCount.Should().Be(1); // /kafka/clusters/broken/surprise

        // Кластер с битым config всё равно в модели (Active, пустые конфиги — без исключения).
        var broken = parsed.Clusters.Single(c => c.Name == "broken");
        broken.State.Should().Be(KafkaClusterState.Active);
        broken.Brokers.Should().Be(0);
        broken.BrokersList.Should().HaveCount(2);
        broken.Topics.Should().BeEmpty();

        // Частичный факт-топик: читается с null-полями.
        var partial = parsed.Clusters.Single(c => c.Name == "broken2").Topics.Single();
        partial.Name.Should().Be("partial");
        partial.Partitions.Should().Be(3);
        partial.ReplicationFactor.Should().BeNull();
        partial.RetentionMs.Should().BeNull();
        partial.Missing.Should().BeFalse();
    }

    [Fact]
    public void StateTolerant_UnknownStateMeansActive()
    {
        // Arrange: незнакомое state-значение (arch/15 §6 — система развивается).
        var kvs = new List<Kv>
        {
            new("/kafka/clusters/odd/config",
                "{\"brokers\":1,\"replication_factor\":1,\"min_insync_replicas\":1," +
                "\"default_partitions\":1,\"default_retention_ms\":1000,\"state\":\"MIGRATING\"}", 1),
        };

        // Act
        var parsed = KafkaParser.ParseClusters(kvs);

        // Assert
        parsed.Clusters.Single().State.Should().Be(KafkaClusterState.Active);
    }

    [Fact]
    public void AppSecrets_ExpectedSkipNotUnknown()
    {
        // Arrange: app_user/app_password панель не читает в модель (arch/02 §10.1) —
        // не unknownKeys, не в модели.
        var kvs = new List<Kv>
        {
            new("/kafka/clusters/events/config",
                "{\"brokers\":1,\"replication_factor\":1,\"min_insync_replicas\":1," +
                "\"default_partitions\":1,\"default_retention_ms\":1000}", 1),
            new("/kafka/clusters/events/app_user", "app", 2),
            new("/kafka/clusters/events/app_password", "secret", 3),
        };

        // Act
        var parsed = KafkaParser.ParseClusters(kvs);

        // Assert
        parsed.UnknownKeyCount.Should().Be(0);
        parsed.Errors.Should().BeEmpty();
    }

    [Fact]
    public void Rotations_ValidAndBroken()
    {
        // Arrange: две валидные заявки + битый JSON (arch/15 §4).
        var kvs = EtcdFixtures.LoadKv("Kafka/kafka-rotations.json");

        // Act
        var parsed = KafkaParser.ParseRotations(kvs);

        // Assert
        parsed.Tickets.Should().HaveCount(2);
        var events = parsed.Tickets.Single(t => t.Cluster == "events");
        events.RequestedUnix.Should().Be(1750000200L);
        events.RequestedBy.Should().Be("admin");
        parsed.Tickets.Single(t => t.Cluster == "shop").RequestedBy.Should().BeNull();
        parsed.Errors.Should().ContainSingle(e => e.Key == "/kafkaworker/rotations/broken");
    }

    [Fact]
    public void Rotations_UnknownShapeIsError()
    {
        // Arrange: неканонический ключ под /kafkaworker/rotations/.
        var kvs = new List<Kv> { new("/kafkaworker/rotations/x/y", "{}", 1) };

        // Act
        var parsed = KafkaParser.ParseRotations(kvs);

        // Assert
        parsed.Tickets.Should().BeEmpty();
        parsed.Errors.Should().ContainSingle().Which.Key.Should().Be("/kafkaworker/rotations/x/y");
    }

    [Fact]
    public void LifecycleCreateTicket_ParsedWithConfigsUnwrapped()
    {
        // Arrange: leaf-ключ заявки создания рядом с факт-ключом (arch/15 §3.1);
        // configs развёрнуты в типизированные поля.
        var kvs = new List<Kv>
        {
            new("/kafka/clusters/events/config",
                "{\"brokers\":3,\"replication_factor\":3,\"min_insync_replicas\":2," +
                "\"default_partitions\":12,\"default_retention_ms\":604800000,\"created_unix\":1}", 1),
            new("/kafka/clusters/events/topics/audit/desired.create",
                "{\"partitions\":12,\"replication_factor\":3," +
                "\"configs\":{\"retention.ms\":\"86400000\"}," +
                "\"requested_unix\":1750000000,\"requested_by\":\"admin\"}", 2),
        };

        // Act
        var parsed = KafkaParser.ParseClusters(kvs);

        // Assert: один create-тикет с развёрнутыми полями; факт-топиков нет.
        parsed.Errors.Should().BeEmpty();
        var cluster = parsed.Clusters.Single(c => c.Name == "events");
        cluster.LifecycleTickets.Should().ContainSingle().Which.Should().BeEquivalentTo(
            new KafkaTopicLifecycleTicket(
                "audit", "create", 12, 3, 86400000L, null, 1750000000L, "admin"));
        cluster.Topics.Should().BeEmpty();
    }

    [Fact]
    public void LifecycleDeleteTicket_ParsedAsAuditOnly()
    {
        // Arrange: заявка удаления — только аудит (arch/15 §2.1).
        var kvs = new List<Kv>
        {
            new("/kafka/clusters/events/config",
                "{\"brokers\":3,\"replication_factor\":3,\"min_insync_replicas\":2," +
                "\"default_partitions\":12,\"default_retention_ms\":604800000}", 1),
            new("/kafka/clusters/events/topics/orders/desired.delete",
                "{\"requested_unix\":1750000100,\"requested_by\":\"admin\"}", 2),
        };

        // Act
        var parsed = KafkaParser.ParseClusters(kvs);

        // Assert
        var ticket = parsed.Clusters.Single().LifecycleTickets.Should().ContainSingle().Which;
        ticket.Topic.Should().Be("orders");
        ticket.Op.Should().Be("delete");
        ticket.Partitions.Should().BeNull();
        ticket.ReplicationFactor.Should().BeNull();
        ticket.RequestedUnix.Should().Be(1750000100L);
    }

    [Fact]
    public void LifecycleTicket_BrokenJsonOrNoAudit_ProduceParseErrors()
    {
        // Arrange: битый JSON + заявка без requested_unix (панель пишет аудит
        // всегда — образец ParseRotations).
        var kvs = new List<Kv>
        {
            new("/kafka/clusters/events/config",
                "{\"brokers\":1,\"replication_factor\":1,\"min_insync_replicas\":1," +
                "\"default_partitions\":1,\"default_retention_ms\":1}", 1),
            new("/kafka/clusters/events/topics/bad/desired.create", "{oops", 2),
            new("/kafka/clusters/events/topics/noaudit/desired.delete", "{\"requested_by\":\"u\"}", 3),
        };

        // Act
        var parsed = KafkaParser.ParseClusters(kvs);

        // Assert: оба — parseError, тикеты не созданы.
        parsed.Clusters.Single().LifecycleTickets.Should().BeEmpty();
        parsed.Errors.Should().Contain(e => e.Key == "/kafka/clusters/events/topics/bad/desired.create");
        parsed.Errors.Should().Contain(e => e.Key == "/kafka/clusters/events/topics/noaudit/desired.delete");
    }

    [Fact]
    public void LifecycleTicket_UnknownTopicsLeaf_CountsUnknownKey()
    {
        // Arrange: неизвестный leaf под topics/<T>/ — счётчик, не ошибка (arch/15 §6).
        var kvs = new List<Kv>
        {
            new("/kafka/clusters/events/config",
                "{\"brokers\":1,\"replication_factor\":1,\"min_insync_replicas\":1," +
                "\"default_partitions\":1,\"default_retention_ms\":1}", 1),
            new("/kafka/clusters/events/topics/x/desired.pause", "{}", 2),
        };

        // Act
        var parsed = KafkaParser.ParseClusters(kvs);

        // Assert
        parsed.UnknownKeyCount.Should().Be(1);
        parsed.Clusters.Single().LifecycleTickets.Should().BeEmpty();
    }

    [Fact]
    public void Rebalances_ValidAndBroken()
    {
        // Arrange: две валидные заявки + битый JSON (формат ротаций, t02 §4).
        var kvs = EtcdFixtures.LoadKv("Kafka/kafka-rebalances.json");

        // Act
        var parsed = KafkaParser.ParseRebalances(kvs);

        // Assert
        parsed.Tickets.Should().HaveCount(2);
        var events = parsed.Tickets.Single(t => t.Cluster == "events");
        events.RequestedUnix.Should().Be(1750000200L);
        events.RequestedBy.Should().Be("admin");
        parsed.Tickets.Single(t => t.Cluster == "shop").RequestedBy.Should().BeNull();
        parsed.Errors.Should().ContainSingle(e => e.Key == "/kafkaworker/rebalances/broken");
    }

    [Fact]
    public void Rebalances_UnknownShapeIsError()
    {
        // Arrange: мусорный префикс-ключ под /kafkaworker/rebalances/.
        var kvs = new List<Kv> { new("/kafkaworker/rebalances/x/y", "{}", 1) };

        // Act
        var parsed = KafkaParser.ParseRebalances(kvs);

        // Assert
        parsed.Tickets.Should().BeEmpty();
        parsed.Errors.Should().ContainSingle().Which.Key.Should().Be("/kafkaworker/rebalances/x/y");
    }

    [Fact]
    public void Reassignments_ValidAndBroken()
    {
        // Arrange: drain-прогресс + balance-прогресс + битые ключи (нет
        // обязательных полей / битый JSON).
        var kvs = EtcdFixtures.LoadKv("Kafka/kafka-reassignments.json");

        // Act
        var parsed = KafkaParser.ParseReassignments(kvs);

        // Assert
        parsed.Progress.Should().HaveCount(2);
        var events = parsed.Progress.Single(p => p.Cluster == "events");
        events.Mode.Should().Be("drain");
        events.DrainBroker.Should().Be("broker4");
        events.PartitionsTotal.Should().Be(12);
        events.PartitionsRemaining.Should().Be(5);
        events.UpdatedUnix.Should().Be(1750000215L);
        events.LastError.Should().BeNull();
        var shop = parsed.Progress.Single(p => p.Cluster == "shop");
        shop.Mode.Should().Be("balance");
        shop.DrainBroker.Should().BeNull();
        parsed.Errors.Should().HaveCount(2);
        parsed.Errors.Select(e => e.Key).Should().Contain("/kafkaworker/reassignments/nofields")
            .And.Contain("/kafkaworker/reassignments/broken");
    }

    [Fact]
    public void Reassignments_UnknownShapeIsError()
    {
        // Arrange: мусорный префикс-ключ под /kafkaworker/reassignments/.
        var kvs = new List<Kv> { new("/kafkaworker/reassignments/x/y", "{}", 1) };

        // Act
        var parsed = KafkaParser.ParseReassignments(kvs);

        // Assert
        parsed.Progress.Should().BeEmpty();
        parsed.Errors.Should().ContainSingle().Which.Key.Should().Be("/kafkaworker/reassignments/x/y");
    }
}
