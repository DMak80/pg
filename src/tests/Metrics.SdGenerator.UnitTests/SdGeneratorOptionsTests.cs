using FluentAssertions;
using Metrics.SdGenerator;

namespace Metrics.SdGenerator.UnitTests;

// Опции генератора: нормализация интервала тика (spec §6.1, дефолт <=0 → 15).
public class SdGeneratorOptionsTests
{
    [Theory]
    [InlineData(0, 15)]
    [InlineData(-5, 15)]
    [InlineData(1, 1)]
    [InlineData(15, 15)]
    [InlineData(60, 60)]
    public void NormalizeInterval_ClampsNonPositive(int given, int expected)
    {
        // Act
        var actual = SdGeneratorOptions.NormalizeInterval(given);

        // Assert
        actual.Should().Be(expected);
    }
}
