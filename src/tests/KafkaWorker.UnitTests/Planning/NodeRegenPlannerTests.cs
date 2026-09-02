using FluentAssertions;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using Xunit;

namespace KafkaWorker.UnitTests.Planning;

// Сверка лимитов контейнера с декларацией resources (t06, spec §5.2 J2 / §5.3).
// ВАЖНО: формула cpu повторяет арифметику ЗАПИСИ DockerEngine — decimal →
// double → (long)(cores * 1e9) — иначе значения, непредставимые точно в
// double (0.01, 1.15), дают вечное «расхождение» и цикл рестартов
// (ревью Фазы 4, замечание 4).
public class NodeRegenPlannerTests
{
    [Theory]
    [InlineData("2", 2_000_000_000L)]
    [InlineData("0.5", 500_000_000L)]
    [InlineData("0.01", 10_000_000L)]     // double(0.01)*1e9 == double-арифметика записи
    [InlineData("1.15", 1_150_000_000L)]  // непредставимо в double точно: произведение
                                           // 1.1499999999…*1e9 округляется до double
                                           // 1150000000.0 (проверено фактом записи)
    public void ExpectedNanoCpus_MatchesDockerEngineWriteArithmetic(string cpu, long nano)
    {
        // Act
        var actual = NodeRegenPlanner.ExpectedNanoCpus(decimal.Parse(cpu, System.Globalization.CultureInfo.InvariantCulture));

        // Assert
        actual.Should().Be(nano);
    }

    [Fact]
    public void ExpectedMemoryBytes_IsGiBShifted()
    {
        // Act
        var actual = NodeRegenPlanner.ExpectedMemoryBytes(4);

        // Assert
        actual.Should().Be(4L * 1024 * 1024 * 1024);
    }

    [Fact]
    public void NeedsRegen_EqualLimits_False()
    {
        // Arrange — лимиты контейнера получены той же арифметикой (запись)
        var decl = new BrokerResources(2m, 4, 40);

        // Act
        var needs = NodeRegenPlanner.NeedsRegen(decl, new NodeLimits(2_000_000_000L, 4L << 30));

        // Assert
        needs.Should().BeFalse();
    }

    [Fact]
    public void NeedsRegen_DecimalUnfriendlyCpuEqualByWriteArithmetic_False()
    {
        // Arrange — 1.15 ядер: запись даёт 1149999999 нано; сверка обязана
        // сойтись с фактом инспекта (тот же расчёт), а не с decimal-идеалом
        var decl = new BrokerResources(1.15m, 4, 40);

        // Act
        var needs = NodeRegenPlanner.NeedsRegen(decl, new NodeLimits(
            NodeRegenPlanner.ExpectedNanoCpus(1.15m), 4L << 30));

        // Assert
        needs.Should().BeFalse();
    }

    [Theory]
    [InlineData(1_000_000_000L, 4L << 30)] // cpu расходится
    [InlineData(2_000_000_000L, 2L << 30)] // mem расходится
    [InlineData(0L, 0L)]                    // контейнер без лимитов
    public void NeedsRegen_AnyDivergence_True(long nano, long mem)
    {
        // Arrange
        var decl = new BrokerResources(2m, 4, 40);

        // Act
        var needs = NodeRegenPlanner.NeedsRegen(decl, new NodeLimits(nano, mem));

        // Assert
        needs.Should().BeTrue();
    }
}
