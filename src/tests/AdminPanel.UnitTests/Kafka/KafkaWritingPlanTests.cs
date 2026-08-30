using AdminPanel.Etcd.Writing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests.Kafka;

// План создания kafka-кластера + валидатор + config-RMW (arch/02 §10.2/§10.3):
// границы полей таблицей, канонический JSON, перезапись mutable-полей.
public class KafkaWritingPlanTests
{
    [Fact]
    public void Build_Defaults_CanonicalConfigAndBrokerPuts()
    {
        // Arrange: запрос только с именем — дефолты 3/3/2/12/7д/2/2/20.
        var request = new CreateKafkaClusterRequest("events");

        // Act
        var plan = KafkaClusterCreatePlan.Build(request, 1756500000);

        // Assert: config — канон arch/15 §2.1 со state заявки; 6 ключей брокеров.
        plan.ConfigKey.Should().Be("/kafka/clusters/events/config");
        plan.ConfigValue.Should().Be(
            """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000,"state":"NOT_INITIALIZED"}""");
        plan.Puts.Should().HaveCount(6);
        plan.Puts.Should().Contain(p => p.Key == "/kafka/clusters/events/brokers/broker1/state"
                                        && p.Value == "NOT_INITIALIZED");
        plan.Puts.Should().Contain(p => p.Key == "/kafka/clusters/events/brokers/broker3/resources"
                                        && p.Value.Contains("\"cpu\":\"2\"")
                                        && p.Value.Contains("\"disk\":\"20Gi\""));
        plan.CanonicalCpu.Should().Be("2");
        plan.CanonicalMem.Should().Be("2Gi");
        plan.CanonicalDisk.Should().Be("20Gi");
    }

    [Theory]
    [InlineData("", false)]          // пустое имя
    [InlineData("Shop", false)]      // заглавная
    [InlineData("sh-op", false)]     // дефис
    [InlineData("a123_ok", true)]    // канон
    public void Validate_NamePattern(string name, bool valid)
    {
        // Arrange / Act
        var errors = KafkaCreateValidator.Validate(new CreateKafkaClusterRequest(name));

        // Assert
        errors.Any(e => e.Field == "name").Should().Be(!valid);
    }

    [Theory]
    [InlineData(0, false)]     // меньше 1
    [InlineData(1, true)]      // минимум
    [InlineData(9, true)]      // максимум
    [InlineData(10, false)]    // больше 9
    public void Validate_BrokersRange(int brokers, bool valid)
    {
        // Arrange / Act
        var errors = KafkaCreateValidator.Validate(new CreateKafkaClusterRequest("events", Brokers: brokers));

        // Assert
        errors.Any(e => e.Field == "brokers").Should().Be(!valid);
    }

    [Fact]
    public void Validate_RfExceedsBrokers_Rejected()
    {
        // Arrange: RF=5 при 3 брокерах.
        var request = new CreateKafkaClusterRequest("events", Brokers: 3, ReplicationFactor: 5);

        // Act
        var errors = KafkaCreateValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Field == "replicationFactor"
                                     && e.Message.Contains("превышать brokers"));
    }

    [Theory]
    [InlineData(0, false)]    // меньше 1
    [InlineData(2, true)]     // == RF ок
    [InlineData(4, false)]    // > RF
    public void Validate_MinIsrAgainstRf(int minIsr, bool valid)
    {
        // Arrange / Act: RF=3.
        var errors = KafkaCreateValidator.Validate(
            new CreateKafkaClusterRequest("events", ReplicationFactor: 3, MinInSyncReplicas: minIsr));

        // Assert
        errors.Any(e => e.Field == "minInSyncReplicas").Should().Be(!valid);
    }

    [Fact]
    public void Validate_ResourceBounds()
    {
        // Arrange: cpu 0.001 (< 0.01), mem 0, disk 70000.
        var request = new CreateKafkaClusterRequest("events", Cpu: 0.001m, MemGi: 0, DiskGi: 70000);

        // Act
        var errors = KafkaCreateValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Field == "cpu");
        errors.Should().Contain(e => e.Field == "memGi");
        errors.Should().Contain(e => e.Field == "diskGi");
    }

    [Fact]
    public void ConfigJson_UpdateKeepsStateAndCreated()
    {
        // Arrange: Active-config (без state).
        var config = KafkaConfigJson.TryParse(
            """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}""");

        // Act: обновление retention + partitions.
        var updated = config!.With(new KafkaConfigUpdateRequest(DefaultPartitions: 24, DefaultRetentionMs: 86400000));

        // Assert: только mutable-поля меняются; state/created_unix на месте.
        var json = updated.Serialize();
        json.Should().Contain("\"default_partitions\":24").And.Contain("\"default_retention_ms\":86400000");
        json.Should().Contain("\"created_unix\":1756500000");
        json.Should().NotContain("\"state\"");
        updated.ReplicationFactor.Should().Be(3);
    }

    [Fact]
    public void ConfigJson_StateFlipForRemoval()
    {
        // Arrange / Act
        var config = KafkaConfigJson.TryParse("""{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000}""")!;

        // Assert: TO_REMOVE добавляется с сохранением полей.
        config.WithState("TO_REMOVE").Serialize().Should().Contain("\"state\":\"TO_REMOVE\"");
    }

    [Fact]
    public void ConfigJson_BrokenJson_ReturnsNull()
    {
        // Arrange / Act / Assert
        KafkaConfigJson.TryParse("{oops").Should().BeNull();
    }
}
