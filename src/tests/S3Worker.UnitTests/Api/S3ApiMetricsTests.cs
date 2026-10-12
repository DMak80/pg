using System.Diagnostics.Metrics;
using FluentAssertions;
using S3Worker.App.Api;
using Xunit;

namespace S3Worker.UnitTests.Api;

// Счётчик операций API (spec §9): s3worker_api_operations_total{operation,code}
// — инкремент фиксируется MeterListener'ом с тегами (паттерн Shared.Metrics).
public class S3ApiMetricsTests
{
    [Fact]
    public void Track_ИнкрементСТегами()
    {
        // Arrange: Meter + слушатель с собирающим списком.
        using var meter = new Meter("S3Worker.Test");
        var metrics = new S3ApiMetrics(meter);
        var seen = new List<(string Operation, string Code)>();
        using var listener = new MeterListener();
        listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
        {
            if (instrument.Name != "s3worker_api_operations_total")
                return;
            var pairs = tags.ToArray();
            seen.Add((
                pairs.First(t => t.Key == "operation").Value!.ToString()!,
                pairs.First(t => t.Key == "code").Value!.ToString()!));
        });
        listener.InstrumentPublished = (instrument, enable) =>
        {
            if (instrument.Meter.Name == "S3Worker.Test")
                enable.EnableMeasurementEvents(instrument);
        };
        listener.Start();

        // Act: два инкремента разных кодов одной операции.
        metrics.Track("create-cluster", 201);
        metrics.Track("create-cluster", 409);

        // Assert: обе пары зафиксированы.
        seen.Should().BeEquivalentTo([
            ("create-cluster", "201"),
            ("create-cluster", "409"),
        ]);
    }
}
