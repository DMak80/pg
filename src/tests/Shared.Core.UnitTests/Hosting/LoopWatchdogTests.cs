using Shared.Core.HealthChecks;

namespace Shared.Core.UnitTests.Hosting;

// Юнит-тесты watchdog зависших циклов: формулы порогов LoopStaleness — единый
// источник healthz + watchdog (симметрия: порог healthz ×1, watchdog ×2).
public sealed class LoopStalenessTests
{
    [Theory]
    [InlineData(5, 5, 30)]    // дефолты воркеров: 3×max(5,5)+15
    [InlineData(10, 3, 45)]   // скан доминирует
    [InlineData(2, 8, 39)]    // keepalive доминирует
    public void FastLoops_Formula(int scan, int keepalive, int expectedSec)
    {
        // Act
        var staleAfter = LoopStaleness.FastLoops(scan, keepalive);

        // Assert
        staleAfter.Should().Be(TimeSpan.FromSeconds(expectedSec));
    }

    [Theory]
    [InlineData(5, 360, 64815)] // дефолт: 3×max(5, 21600)+15 — часы между тиками лидера
    [InlineData(10, 10, 1815)]  // интервал в минутах доминирует над сканом
    public void SnapshotLoop_Formula(int scan, int intervalMin, int expectedSec)
    {
        // Act
        var staleAfter = LoopStaleness.SnapshotLoop(scan, intervalMin);

        // Assert
        staleAfter.Should().Be(TimeSpan.FromSeconds(expectedSec));
    }

    [Fact]
    public void Symmetry_WatchdogThreshold_IsHealthzTimesMultiplier()
    {
        // Arrange: дефолтные интервалы воркера и множитель 2
        const int multiplier = 2;

        // Act: порог healthz и порог watchdog — одна формула, разные множители
        var healthz = LoopStaleness.FastLoops(5, 5);
        var watchdog = TimeSpan.FromTicks(healthz.Ticks * multiplier);

        // Assert: окно Degraded→рестарт = 30 c (алертам оператора и длинным тикам)
        healthz.Should().Be(TimeSpan.FromSeconds(30));
        watchdog.Should().Be(TimeSpan.FromSeconds(60));
    }
}
