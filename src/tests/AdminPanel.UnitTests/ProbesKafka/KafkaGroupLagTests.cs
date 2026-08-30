using AdminPanel.Probes.Kafka;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests.ProbesKafka;

// KafkaGroupLag (план C3-шаг 1): чистая функция (end, committed) → totalLag.
// Отсутствие коммита = весь end; отрицательный лаг (retention срезал) = 0.
public class KafkaGroupLagTests
{
    [Fact]
    public void Total_CommittedBehindEnd_SumsPerPartition()
    {
        // Arrange: 2 партиции, лаги 5 и 3.
        var end = new Dictionary<(string Topic, int Partition), long>
        {
            [("orders", 0)] = 100,
            [("orders", 1)] = 50,
        };
        var committed = new Dictionary<(string Topic, int Partition), long>
        {
            [("orders", 0)] = 95,
            [("orders", 1)] = 47,
        };

        // Act
        var total = KafkaGroupLag.Total(end, committed);

        // Assert
        total.Should().Be(8);
    }

    [Fact]
    public void Total_NoCommit_WholeEndIsLag()
    {
        // Arrange: коммита нет вообще (группа ни разу не закоммитилась).
        var end = new Dictionary<(string Topic, int Partition), long>
        {
            [("orders", 0)] = 10,
            [("payments", 3)] = 5,
        };

        // Act
        var total = KafkaGroupLag.Total(end, new Dictionary<(string Topic, int Partition), long>());

        // Assert
        total.Should().Be(15, "отсутствие коммита = весь end как лаг");
    }

    [Fact]
    public void Total_CommitAheadOfEnd_ClampedToZero()
    {
        // Arrange: retention удалил сегменты — committed (200) > end (150).
        var end = new Dictionary<(string Topic, int Partition), long> { [("orders", 0)] = 150 };
        var committed = new Dictionary<(string Topic, int Partition), long> { [("orders", 0)] = 200 };

        // Act
        var total = KafkaGroupLag.Total(end, committed);

        // Assert
        total.Should().Be(0, "отрицательный лаг по партиции не уменьшает общий");
    }

    [Fact]
    public void Total_MixedPartitions_NoCommitPartitionCountsAsEnd()
    {
        // Arrange: одна партиция с коммитом (лаг 2), вторая без (лаг 9).
        var end = new Dictionary<(string Topic, int Partition), long>
        {
            [("a", 0)] = 7,
            [("b", 0)] = 9,
        };
        var committed = new Dictionary<(string Topic, int Partition), long> { [("a", 0)] = 5 };

        // Act
        var total = KafkaGroupLag.Total(end, committed);

        // Assert
        total.Should().Be(2 + 9);
    }
}
