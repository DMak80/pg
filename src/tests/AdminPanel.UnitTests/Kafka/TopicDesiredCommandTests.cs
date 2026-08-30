using System.Text.Json;
using AdminPanel.Api.Operations.Kafka;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Xunit;
using static AdminPanel.UnitTests.Kafka.KafkaCommandHarness;

namespace AdminPanel.UnitTests.Kafka;

// desired-мутации топиков (arch/02 §10.2-7/8, план C2): RMW-txn по
// mod_revision, валидация «partitions только больше фактического», грани
// 404 (кластер/топик/missing/имя) и 409/503.
public class UpsertTopicDesiredCommandTests
{
    private const string TopicKey = "/kafka/clusters/events/topics/orders";

    private static UpsertTopicDesiredCommandHandler Handler(FakeKafkaEtcd etcd)
        => new(StoreWithEndpoint(), etcd, TimeProvider.System);

    private static void SeedTopic(
        FakeKafkaEtcd etcd, string? desiredJson = null, bool missing = false, int partitions = 3)
    {
        SeedActiveCluster(etcd);
        var raw = "{\"partitions\":" + partitions
            + ",\"replication_factor\":3"
            + ",\"configs\":{\"retention.ms\":\"604800000\"}"
            + (desiredJson is null ? "" : "," + desiredJson)
            + ",\"synced_unix\":1756500900"
            + ",\"missing\":" + (missing ? "true" : "false") + "}";
        etcd.Seed(TopicKey, raw);
    }

    [Fact]
    public async Task Handle_ValidRetention_RmwPutsWithDesired()
    {
        // Arrange: факт топика без заявки.
        var etcd = new FakeKafkaEtcd();
        SeedTopic(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new UpsertTopicDesiredCommand("events", "orders",
                new TopicDesiredRequest(RetentionMs: 86400000), "admin"), CancellationToken.None);

        // Assert: desired-поля в ключе, факт не тронут, txn по mod_revision.
        result.IsSuccess.Should().BeTrue();
        var value = JsonSerializer.Deserialize<JsonElement>(etcd.Store[TopicKey].Value);
        value.GetProperty("partitions").GetInt32().Should().Be(3, "факт не меняется заявкой");
        value.GetProperty("desired").GetProperty("configs").GetProperty("retention.ms").GetString()
            .Should().Be("86400000");
        value.GetProperty("desired_by").GetString().Should().Be("admin");
        value.GetProperty("desired_unix").GetInt64().Should().BeGreaterThan(0);
        etcd.Txns.Should().ContainSingle(t =>
            t.Compares.Any(c => c.Key == TopicKey && c.ModRevision.HasValue));
    }

    [Fact]
    public async Task Handle_PartitionsUp_DesiredCarriesPartitions()
    {
        // Arrange
        var etcd = new FakeKafkaEtcd();
        SeedTopic(etcd, partitions: 3);

        // Act
        var result = await Handler(etcd).Handle(
            new UpsertTopicDesiredCommand("events", "orders",
                new TopicDesiredRequest(Partitions: 6), "admin"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var value = JsonSerializer.Deserialize<JsonElement>(etcd.Store[TopicKey].Value);
        value.GetProperty("desired").GetProperty("partitions").GetInt32().Should().Be(6);
    }

    [Fact]
    public async Task Handle_PartitionsNotGreaterThanFact_400_NoDesiredWritten()
    {
        // Arrange: заявка на уменьшение (3 → 2) — панель отсекает (spec §3.2).
        var etcd = new FakeKafkaEtcd();
        SeedTopic(etcd, partitions: 3);

        // Act
        var result = await Handler(etcd).Handle(
            new UpsertTopicDesiredCommand("events", "orders",
                new TopicDesiredRequest(Partitions: 2), "admin"), CancellationToken.None);

        // Assert: 400, desired в ключе отсутствует (негатив подшага 5 чека 55).
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<KafkaValidationException>();
        etcd.Store[TopicKey].Value.Should().NotContain("\"desired\"");
    }

    [Fact]
    public async Task Handle_NoFields_400()
    {
        // Arrange
        var etcd = new FakeKafkaEtcd();
        SeedTopic(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new UpsertTopicDesiredCommand("events", "orders", new TopicDesiredRequest(), "admin"),
            CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaValidationException>();
    }

    [Fact]
    public async Task Handle_TopicKeyMissing_404()
    {
        // Arrange: топика нет в реестре.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new UpsertTopicDesiredCommand("events", "orders",
                new TopicDesiredRequest(RetentionMs: 86400000), "admin"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaTopicNotFoundException>();
    }

    [Fact]
    public async Task Handle_MissingTopic_404()
    {
        // Arrange: топик помечен missing=true — заявка не исполнима.
        var etcd = new FakeKafkaEtcd();
        SeedTopic(etcd, missing: true);

        // Act
        var result = await Handler(etcd).Handle(
            new UpsertTopicDesiredCommand("events", "orders",
                new TopicDesiredRequest(RetentionMs: 86400000), "admin"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaTopicNotFoundException>()
            .Which.Message.Should().Contain("отсутствует");
    }

    [Theory]
    [InlineData("__consumer_offsets")] // internal-топик
    [InlineData("bad name!")]          // неканоническое имя
    [InlineData("")]
    public async Task Handle_TopicNameInvalid_404(string topic)
    {
        // Arrange
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new UpsertTopicDesiredCommand("events", topic,
                new TopicDesiredRequest(RetentionMs: 86400000), "admin"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaTopicNotFoundException>();
    }

    [Fact]
    public async Task Handle_ClusterNotActive_409()
    {
        // Arrange: кластер в TO_REMOVE.
        var etcd = new FakeKafkaEtcd();
        SeedTopic(etcd);
        etcd.Seed("/kafka/clusters/events/config",
            """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000,"state":"TO_REMOVE"}""");

        // Act
        var result = await Handler(etcd).Handle(
            new UpsertTopicDesiredCommand("events", "orders",
                new TopicDesiredRequest(RetentionMs: 86400000), "admin"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaClusterNotActiveException>();
    }

    [Fact]
    public async Task Handle_ClusterMissing_404()
    {
        // Arrange
        var etcd = new FakeKafkaEtcd();

        // Act
        var result = await Handler(etcd).Handle(
            new UpsertTopicDesiredCommand("ghost", "orders",
                new TopicDesiredRequest(RetentionMs: 86400000), "admin"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaClusterNotFoundException>();
    }

    [Fact]
    public async Task Handle_RmwLost_503()
    {
        // Arrange: конкурент перезаписал ключ между read и txn.
        var etcd = new FakeKafkaEtcd();
        SeedTopic(etcd);
        etcd.OnTxn = () => etcd.Seed(TopicKey,
            """{"partitions":3,"replication_factor":3,"configs":{"retention.ms":"604800000"},"synced_unix":1756500900,"missing":false}""");

        // Act
        var result = await Handler(etcd).Handle(
            new UpsertTopicDesiredCommand("events", "orders",
                new TopicDesiredRequest(RetentionMs: 86400000), "admin"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaConcurrentWriteException>();
    }

    [Fact]
    public async Task Handle_BitwiseBrokenKey_503()
    {
        // Arrange: ключ топика — битый JSON.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed(TopicKey, "{broken");

        // Act
        var result = await Handler(etcd).Handle(
            new UpsertTopicDesiredCommand("events", "orders",
                new TopicDesiredRequest(RetentionMs: 86400000), "admin"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<InvalidKafkaTopicKeyException>();
    }
}

public class CancelTopicDesiredCommandTests
{
    private const string TopicKey = "/kafka/clusters/events/topics/orders";

    private static CancelTopicDesiredCommandHandler Handler(FakeKafkaEtcd etcd)
        => new(StoreWithEndpoint(), etcd);

    private static void SeedTopicWithDesired(FakeKafkaEtcd etcd)
    {
        SeedActiveCluster(etcd);
        etcd.Seed(TopicKey,
            """{"partitions":3,"replication_factor":3,"configs":{"retention.ms":"604800000"},"desired":{"configs":{"retention.ms":"432000000"}},"desired_unix":1756500950,"desired_by":"admin","synced_unix":1756500900,"missing":true}""");
    }

    [Fact]
    public async Task Handle_Valid_RemovesDesiredKeepsFact()
    {
        // Arrange: missing-топик с живой заявкой (главный сценарий отмены).
        var etcd = new FakeKafkaEtcd();
        SeedTopicWithDesired(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new CancelTopicDesiredCommand("events", "orders"), CancellationToken.None);

        // Assert: desired-поля исчезли, факт и missing сохранены.
        result.IsSuccess.Should().BeTrue();
        var value = etcd.Store[TopicKey].Value;
        value.Should().NotContainAny("\"desired\"", "\"desired_unix\"", "\"desired_by\"");
        value.Should().Contain("\"partitions\":3").And.Contain("\"missing\":true");
    }

    [Fact]
    public async Task Handle_NoDesired_404()
    {
        // Arrange: заявки нет.
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);
        etcd.Seed(TopicKey,
            """{"partitions":3,"replication_factor":3,"configs":{"retention.ms":"604800000"},"synced_unix":1756500900,"missing":false}""");

        // Act
        var result = await Handler(etcd).Handle(
            new CancelTopicDesiredCommand("events", "orders"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaTopicDesiredNotFoundException>();
    }

    [Fact]
    public async Task Handle_TopicKeyMissing_404()
    {
        // Arrange
        var etcd = new FakeKafkaEtcd();
        SeedActiveCluster(etcd);

        // Act
        var result = await Handler(etcd).Handle(
            new CancelTopicDesiredCommand("events", "orders"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaTopicNotFoundException>();
    }

    [Fact]
    public async Task Handle_RmwLost_503()
    {
        // Arrange: конкурентная запись в txn-окне.
        var etcd = new FakeKafkaEtcd();
        SeedTopicWithDesired(etcd);
        etcd.OnTxn = () => etcd.Seed(TopicKey,
            """{"partitions":3,"replication_factor":3,"configs":{"retention.ms":"604800000"},"desired":{"partitions":9},"desired_unix":1756500990,"desired_by":"panel","synced_unix":1756500900,"missing":true}""");

        // Act
        var result = await Handler(etcd).Handle(
            new CancelTopicDesiredCommand("events", "orders"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<KafkaConcurrentWriteException>();
    }
}
