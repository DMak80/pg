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
    public void ReconcileInterval_Default_Is60Seconds()
    {
        // Arrange/Act — дефолтный экземпляр (вне конфигурации)
        var options = new ReplicationOptions();

        // Assert — период reconcile 60 с (§9 триггер 4: самозаживление suspend-проходов)
        options.ReconcileIntervalSeconds.Should().Be(60);
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
}
