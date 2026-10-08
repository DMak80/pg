using Shared.Core.Hosting;

namespace Shared.Core.UnitTests.Hosting;

// Пульсирующий сон: законное ожидание длиннее порога сноса watchdog —
// чанками короче окна проверки, отметка активности в каждом чанке.
public sealed class PulsingDelayTests
{
    [Fact]
    public async Task SleepAsync_ЧанкКорочеОкна_ОтметкаНаКаждыйЧанк()
    {
        // Arrange: сон 3 c при окне 1 c → чанк = max(1 c, 0.5 c) = 1 c
        // (в проде окно 15 c → чанк 7.5 c < 15 c)
        var pulses = 0;

        // Act
        await PulsingDelay.SleepAsync(
            TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1), () => pulses++,
            TestContext.Current.CancellationToken);

        // Assert: отметки в каждом чанке (несколько чанков покрыты), каждый
        // чанк короче окна — активность обновляется чаще проверки
        pulses.Should().BeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task SleepAsync_Отмена_ВыходитБыстро()
    {
        // Arrange: долгий сон + отмена через 300 мс (меньше чанка 1 c)
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var pulses = 0;

        // Act: Task.Delay чанка бросает OperationCanceledException — выход по отмене
        try
        {
            await PulsingDelay.SleepAsync(
                TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(1), () => pulses++, cts.Token);
        }
        catch (OperationCanceledException)
        {
            // штатный выход по отмене — не спал минуту
        }

        // Assert: вышел по отмене (полных чанков не больше одного)
        pulses.Should().BeLessThanOrEqualTo(1);
    }
}
