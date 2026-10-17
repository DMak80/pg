using FluentAssertions;
using Microsoft.Extensions.Configuration;
using OwnS3.Replication;

namespace OwnS3.UnitTests.Replication;

// Конфигурация репликации (глава 07 §11): дефолты канона + биндинг
// канонического ключа OwnS3:Replication:* (контракт env-трансляции
// Program.cs: OWNS3_REPLICATION_RECONCILE_INTERVAL_SECONDS → этот ключ).
public class ReplicationOptionsTests
{
    [Fact]
    public void ReconcileInterval_Default_Is3Hours()
    {
        // Arrange/Act — дефолтный экземпляр (вне конфигурации)
        var options = new ReplicationOptions();

        // Assert — период reconcile 3 ч (§9 триггер 4: страховой контур, не
        // основной канал доставки; тестовые контуры переопределяют env до 10 с)
        options.ReconcileIntervalSeconds.Should().Be(10800);
    }

    [Fact]
    public void ReconcileInterval_BindsFromCanonicalConfigKey()
    {
        // Arrange — канонический ключ секции (тот, куда пишет env-трансляция)
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection([new("OwnS3:Replication:ReconcileIntervalSeconds", "90")])
            .Build();
        var options = new ReplicationOptions();

        // Act
        config.Bind(ReplicationOptions.SectionName, options);

        // Assert — int-биндинг ключа работает, значение переопределено
        options.ReconcileIntervalSeconds.Should().Be(90);
    }

    [Fact]
    public void ProbeTimeoutMs_Default_Is2000()
    {
        // Arrange/Act — дефолтный экземпляр (вне конфигурации)
        var options = new ReplicationOptions();

        // Assert — per-target таймаут ping-пробы 2000 мс (глава 07 §11)
        options.ProbeTimeoutMs.Should().Be(2000);
    }

    [Fact]
    public void ProbeTimeoutMs_BindsFromCanonicalConfigKey()
    {
        // Arrange — канонический ключ секции (тот, куда пишет env-трансляция
        // OWNS3_REPLICATION_PROBE_TIMEOUT_MS → OwnS3:Replication:ProbeTimeoutMs)
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection([new("OwnS3:Replication:ProbeTimeoutMs", "500")])
            .Build();
        var options = new ReplicationOptions();

        // Act
        config.Bind(ReplicationOptions.SectionName, options);

        // Assert — int-биндинг ключа работает, значение переопределено
        options.ProbeTimeoutMs.Should().Be(500);
    }
}
