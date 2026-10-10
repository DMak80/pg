using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using OwnS3.App.Pipeline;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Фоновый сервис чисток: немедленный старт-проход заполняет дисковые gauge
// до первого await (StartAsync возвращается после него), период — 15 минут.
public class VolumeCleanupServiceTests
{
    [Fact]
    public async Task StartAsync_ImmediatePass_UpdatesDiskGauges()
    {
        // Arrange: temp-том + метрики + сервис (полный teardown)
        var root = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
        var volume = new XlVolume(root, TimeProvider.System);
        volume.Initialize();
        var metrics = new OwnS3Metrics(new Meter("owns3-test-" + Guid.NewGuid().ToString("N")));
        var logger = new CollectingLogger();
        var service = new VolumeCleanupService(volume, metrics, logger);
        try
        {
            // Act: старт → немедленный проход → стоп (периодический цикл не ждём)
            await service.StartAsync(TestContext.Current.CancellationToken);
            await service.StopAsync(TestContext.Current.CancellationToken);

            // Assert: gauge диска заполнены первым же проходом (критерий §10.6)
            metrics.DiskUsedBytes.Should().BeGreaterThan(0,
                "первый проход обязан заполнить gauge; лог сервиса: {0}", string.Join(" | ", logger.Entries));
            metrics.DiskTotalBytes.Should().BeGreaterThan(0);
            metrics.DiskUsedBytes.Should().BeLessOrEqualTo(metrics.DiskTotalBytes);
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }

    // Диагностический коллектор сообщений сервиса (причина незаполненного gauge).
    private sealed class CollectingLogger : ILogger<VolumeCleanupService>
    {
        public List<string> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Entries.Add(logLevel + ": " + formatter(state, exception)
                + (exception is null ? string.Empty : " EX: " + exception.Message));
    }
}
