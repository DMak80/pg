using FluentAssertions;
using KafkaWorker.Core.Model;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.UnitTests.Provisioning;

// Тесты converge dynamic broker configs (arch/16 §5 E): маппинг полей заявки
// → Kafka-конфиги, diff по фактическим значениям, alter на всех брокерах,
// no-op при совпадении.

public class ClusterConfigConvergerTests
{
    private static KafkaClusterConfig Config(int retentionMs = 604800000, int partitions = 12, int rf = 3, int minIsr = 2)
        => new(3, rf, minIsr, partitions, retentionMs, 1756500000L, null);

    [Fact]
    public void Decide_AllDiffer_FullDiff()
    {
        // Arrange: фактические конфиги отличаются от заявки по всем полям.
        var current = new Dictionary<string, string>
        {
            ["log.retention.ms"] = "86400000",
            ["num.partitions"] = "6",
            ["default.replication.factor"] = "1",
            ["min.insync.replicas"] = "1",
        };

        // Act: decide.
        var changes = ConvergeDecider.Decide(Config(), current);

        // Assert: полный набор изменений — значения заявки.
        changes.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["log.retention.ms"] = "604800000",
            ["num.partitions"] = "12",
            ["default.replication.factor"] = "3",
            ["min.insync.replicas"] = "2",
        });
    }

    [Fact]
    public void Decide_Equal_NoChanges()
    {
        // Arrange: факт = цель.
        var current = new Dictionary<string, string>
        {
            ["log.retention.ms"] = "604800000",
            ["num.partitions"] = "12",
            ["default.replication.factor"] = "3",
            ["min.insync.replicas"] = "2",
        };

        // Act: decide.
        var changes = ConvergeDecider.Decide(Config(), current);

        // Assert: пустой diff (no-op).
        changes.Should().BeEmpty();
    }

    [Fact]
    public void Decide_PartialDiff_OnlyChanged()
    {
        // Arrange: изменили только retention (панель PUT /config).
        var current = new Dictionary<string, string>
        {
            ["log.retention.ms"] = "86400000",
            ["num.partitions"] = "12",
            ["default.replication.factor"] = "3",
            ["min.insync.replicas"] = "2",
        };

        // Act: decide.
        var changes = ConvergeDecider.Decide(Config(retentionMs: 3600000), current);

        // Assert: только retention.
        changes.Should().BeEquivalentTo(new Dictionary<string, string> { ["log.retention.ms"] = "3600000" });
    }

    [Fact]
    public void Decide_MissingFactEntry_Changed()
    {
        // Arrange: фактические конфиги не содержат части ключей (default у брокера).
        var current = new Dictionary<string, string> { ["log.retention.ms"] = "604800000" };

        // Act: decide.
        var changes = ConvergeDecider.Decide(Config(), current);

        // Assert: отсутствующие = отличаются (будут заданы Set'ом).
        changes.Should().HaveCount(3);
    }

    private sealed class Factory(FakeKafkaAdminClient client) : IKafkaAdminClientFactory
    {
        public IKafkaAdminClient Create(string bootstrap, string user, string password, string? caPem) => client;
    }

    [Fact]
    public async Task Apply_Differs_AltersAllBrokers()
    {
        // Arrange: кластер из 2 брокеров; факт отличается.
        var admin = new FakeKafkaAdminClient
        {
            ClusterView = new KafkaClusterView(
                [new KafkaBrokerView(1, "b1"), new KafkaBrokerView(2, "b2")], ControllerId: 1),
            BrokerConfigs = new Dictionary<string, string> { ["log.retention.ms"] = "86400000" },
        };
        var converger = new ClusterConfigConverger(new Factory(admin));

        // Act: converge.
        var result = await converger.ApplyAsync("events", "h:9094", "app", "pw", null, Config(), CancellationToken.None);

        // Assert: alter на каждом брокере с полным diff; после apply факт сходится.
        result.IsSuccess.Should().BeTrue();
        admin.AlterCalls.Should().HaveCount(2);
        admin.AlterCalls[0].BrokerId.Should().Be(1);
        admin.AlterCalls[1].BrokerId.Should().Be(2);
        admin.AlterCalls[0].Configs["log.retention.ms"].Should().Be("604800000");
        admin.BrokerConfigs!["num.partitions"].Should().Be("12");
    }

    [Fact]
    public async Task Apply_Equal_NoAlterCalls()
    {
        // Arrange: факт уже = цель.
        var admin = new FakeKafkaAdminClient
        {
            ClusterView = new KafkaClusterView([new KafkaBrokerView(1, "b1")], ControllerId: 1),
            BrokerConfigs = new Dictionary<string, string>
            {
                ["log.retention.ms"] = "604800000",
                ["num.partitions"] = "12",
                ["default.replication.factor"] = "3",
                ["min.insync.replicas"] = "2",
            },
        };
        var converger = new ClusterConfigConverger(new Factory(admin));

        // Act: converge.
        var result = await converger.ApplyAsync("events", "h:9094", "app", "pw", null, Config(), CancellationToken.None);

        // Assert: no-op — alter не вызывался.
        result.IsSuccess.Should().BeTrue();
        admin.AlterCalls.Should().BeEmpty();
    }

    [Fact]
    public async Task Apply_ClusterNotReady_Fails()
    {
        // Arrange: DescribeCluster падает (кластер поднимается).
        var admin = new FakeKafkaAdminClient { ClusterError = new ApplicationException("not ready") };
        var converger = new ClusterConfigConverger(new Factory(admin));

        // Act: converge.
        var result = await converger.ApplyAsync("events", "h:9094", "app", "pw", null, Config(), CancellationToken.None);

        // Assert: Failed (transient — следующий тик повторит).
        result.IsSuccess.Should().BeFalse();
        admin.AlterCalls.Should().BeEmpty();
    }
}
