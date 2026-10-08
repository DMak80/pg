using Shared.Core.Hosting;

namespace Shared.Core.UnitTests.Hosting;

// Fail-fast инвариант живости старта: тики быстрых циклов чаще ПОРОГА СНОСА
// (Multiplier × CheckIntervalSec), не окна.
public sealed class WatchdogConfigGuardTests
{
    [Fact]
    public void ScanМеньшеПорога_НеБросает()
    {
        // Arrange/Act/Assert: дефолты (5/5, порог 2×15=30) — старт разрешён
        var act = () => WatchdogConfigGuard.EnsureFastLoopsBelowStaleThreshold(
            5, 5, new WatchdogOptions());
        act.Should().NotThrow();
    }

    [Theory]
    [InlineData(30, 5, 2, 15)] // scan == порогу 30
    [InlineData(5, 30, 2, 15)] // keepalive == порогу
    [InlineData(60, 5, 2, 15)] // scan > порога
    [InlineData(5, 5, 4, 15)]  // порог 60, scan 5 — ок (контроль позитивного на кастоме)
    public void ГраницыПорога_FailFastЛиРазрешение(int scan, int keepalive, int multiplier, int window)
    {
        // Arrange
        var watchdog = new WatchdogOptions { Multiplier = multiplier, CheckIntervalSec = window };
        var threshold = multiplier * window;
        var act = () => WatchdogConfigGuard.EnsureFastLoopsBelowStaleThreshold(scan, keepalive, watchdog);

        // Act/Assert: scan/keepalive ≥ порога → понятная ошибка с направлением
        // лечения (увеличить CheckIntervalSec/Multiplier); меньше — не бросает
        if (Math.Max(scan, keepalive) >= threshold)
            act.Should().Throw<ApplicationException>()
                .Which.Message.Should().Contain("CheckIntervalSec");
        else
            act.Should().NotThrow();
    }
}
