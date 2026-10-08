using System.Diagnostics.Metrics;
using FluentAssertions;
using Metrics.SdGenerator;

namespace Metrics.SdGenerator.UnitTests;

// Самонаблюдение генератора: до первого успеха серии нет (null), MarkSuccess —
// unix-время не убывает (свойство — стейт ObservableGauge; реальный scrape — integration/E2E).
public class SdGeneratorMetricsTests : IDisposable
{
    private readonly Meter _meter = new("sd-unit-metrics");

    public void Dispose() => _meter.Dispose();

    [Fact]
    public void BeforeFirstSuccess_SeriesStateIsNull()
    {
        // Arrange
        var metrics = new SdGeneratorMetrics(_meter, TimeProvider.System);

        // Act
        var value = metrics.LastSuccessUnix;

        // Assert: до первого успеха серия не эмитится
        value.Should().BeNull();
    }

    [Fact]
    public void MarkSuccess_Twice_NeverDecreases()
    {
        // Arrange
        var metrics = new SdGeneratorMetrics(_meter, TimeProvider.System);

        // Act
        metrics.MarkSuccess();
        var first = metrics.LastSuccessUnix;
        Thread.Sleep(10);
        metrics.MarkSuccess();
        var second = metrics.LastSuccessUnix;

        // Assert
        first.Should().NotBeNull();
        second.Should().NotBeNull();
        second!.Value.Should().BeGreaterThanOrEqualTo(first!.Value);
    }
}
