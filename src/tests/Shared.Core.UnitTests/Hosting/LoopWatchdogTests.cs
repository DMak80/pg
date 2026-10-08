using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Core.HealthChecks;
using Shared.Core.Hosting;

namespace Shared.Core.UnitTests.Hosting;

// Юнит-тесты формул LoopStaleness: пороги staleness — потребитель ТОЛЬКО
// healthz loops-alive; watchdog порог сноса считает от собственных опций
// (Multiplier × CheckIntervalSec) и эти формулы не читает.
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
}

// Механика watchdog: grace старта, firing (ровно один StopApplication + лог +
// маркер метрики), живые циклы не трогают, Enabled=false — не регистрируется.
public sealed class LoopWatchdogTests
{
    // Управляемая витальность: снимок фиксирован в конструкторе.
    private sealed class FakeVitality(params LoopHeartbeat[] beats)
        : ILoopsVitality
    {
        public IReadOnlyList<LoopHeartbeat> Snapshot() => beats;
    }

    // Счётчик вызовов StopApplication (IHostApplicationLifetime подменять в DI
    // нельзя, но watchdog принимает его конструктором — считаем напрямую).
    private sealed class FakeLifetime : IHostApplicationLifetime
    {
        public int StopCalls;
        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => CancellationToken.None;
        public CancellationToken ApplicationStopped => CancellationToken.None;
        public void StopApplication() => StopCalls++;
    }

    // Собирающий логгер: проверка критического события перед остановкой.
    private sealed class CollectingLogger : ILogger<LoopWatchdog>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel level, EventId eventId, TState state, Exception? e,
            Func<TState, Exception?, string> formatter) => Entries.Add((level, formatter(state, e)));
    }

    private static WatchdogOptions QuickOptions() => new()
    {
        Multiplier = 2, CheckIntervalSec = 1, StopDelaySec = 0
    };

    private static async Task<int> PollUntilAsync(Func<bool> done, TimeSpan budget)
    {
        var waited = TimeSpan.Zero;
        var step = TimeSpan.FromMilliseconds(250);
        while (!done() && waited < budget) // каждое ожидание ≤250 мс ≪ 30 c
        {
            await Task.Delay(step);
            waited += step;
        }
        return 0;
    }

    [Fact]
    public async Task StaleActivity_FiresOnce_LogsCritical_AndMarksMetric()
    {
        // Arrange: активности нет ни тиком, ни отметкой — возраст LastActivityAt
        // (60 c) превысил порог (10 c) и вышел за grace
        var lifetime = new FakeLifetime();
        var logger = new CollectingLogger();
        var marks = new List<string>();
        var sut = new LoopWatchdog(
            new FakeVitality(new LoopHeartbeat("reconcile",
                DateTimeOffset.UtcNow - TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(10))),
            lifetime, logger, TimeProvider.System, QuickOptions(), marks.Add);
        using var cts = new CancellationTokenSource();

        // Act: одно наблюдение превышения достаточно — ждём firing
        await sut.StartAsync(cts.Token);
        await PollUntilAsync(() => lifetime.StopCalls > 0, TimeSpan.FromSeconds(10));

        // Assert: ровно один StopApplication, критический журнал, маркер метрики
        lifetime.StopCalls.Should().Be(1);
        sut.StaleLoop.Should().Be("reconcile");
        sut.Armed.Should().BeTrue();
        marks.Should().ContainSingle().Which.Should().Be("reconcile");
        logger.Entries.Should().Contain(e =>
            e.Level == LogLevel.Critical && e.Message.Contains("reconcile")
            && e.Message.Contains("не проявлял активности")
            && e.Message.Contains("self-restart"));
        cts.Cancel();
    }

    [Fact]
    public async Task FreshActivity_EvenLongIteration_DoesNotStop()
    {
        // Arrange: прогресс-отметки внутри порога — долгая, но живая итерация;
        // тик может быть давним (снимок отдаёт активность секундной давности)
        var lifetime = new FakeLifetime();
        var sut = new LoopWatchdog(
            new FakeVitality(new LoopHeartbeat("reconcile",
                DateTimeOffset.UtcNow - TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10))),
            lifetime, NullLogger<LoopWatchdog>.Instance, TimeProvider.System, QuickOptions());
        using var cts = new CancellationTokenSource();

        // Act: наблюдаем 4 с (4 проверки при CheckIntervalSec=1) и останавливаем сами
        await sut.StartAsync(cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(4), TestContext.Current.CancellationToken);
        cts.Cancel();

        // Assert: хост не останавливался
        lifetime.StopCalls.Should().Be(0);
        sut.StaleLoop.Should().BeNull();
    }

    [Fact]
    public async Task NullActivity_InGraceWindow_DoesNotFire()
    {
        // Arrange: цикл ещё не проявлял активности, grace = 2×порог = 4 c от запуска watchdog
        var lifetime = new FakeLifetime();
        var sut = new LoopWatchdog(
            new FakeVitality(new LoopHeartbeat("snapshot", null, TimeSpan.FromSeconds(2))),
            lifetime, NullLogger<LoopWatchdog>.Instance, TimeProvider.System, QuickOptions());
        using var cts = new CancellationTokenSource();

        // Act: наблюдаем МЕНЬШЕ grace-окна
        await sut.StartAsync(cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        var inGrace = lifetime.StopCalls;
        cts.Cancel();

        // Assert: null-отметка в grace — не firing (цикл имеет право стартовать)
        inGrace.Should().Be(0);
    }

    [Fact]
    public async Task NullActivity_AfterGrace_Fires()
    {
        // Arrange: null-отметка (активности не было вовсе) и порог 1 c → grace = 2 c
        var lifetime = new FakeLifetime();
        var sut = new LoopWatchdog(
            new FakeVitality(new LoopHeartbeat("snapshot", null, TimeSpan.FromSeconds(1))),
            lifetime, NullLogger<LoopWatchdog>.Instance, TimeProvider.System, QuickOptions());
        using var cts = new CancellationTokenSource();

        // Act: ждём исчерпания grace (~2 c) + первой проверки
        await sut.StartAsync(cts.Token);
        await PollUntilAsync(() => lifetime.StopCalls > 0, TimeSpan.FromSeconds(10));

        // Assert: активности не было за grace — рестарт
        lifetime.StopCalls.Should().Be(1);
        cts.Cancel();
    }

    [Fact]
    public void AddLoopWatchdog_Disabled_RegistersNothing()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddLoopWatchdog(new WatchdogOptions { Enabled = false });

        // Assert: ни синглтона, ни hosted-сервиса (поведение как до задачи)
        services.Should().NotContain(d => d.ServiceType == typeof(LoopWatchdog));
        services.Should().NotContain(d => d.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public void AddLoopWatchdog_Enabled_RegistersSingletonAndHosted()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddLoopWatchdog(new WatchdogOptions());

        // Assert: синглтон + hosted-обёртка над ним (паттерн циклов Program.cs)
        services.Should().Contain(d => d.ServiceType == typeof(LoopWatchdog));
        services.Should().Contain(d => d.ServiceType == typeof(IHostedService));
    }
}
