namespace Metrics.SdGenerator;

// Самонаблюдение генератора (arch/18 §5.2 job sd-generator): gauge
// sd_generator_last_success_timestamp_seconds — unix-время последнего успешного
// тика (консервативно: обновляется только успехом, пустой префикс = успех).
// Meter приходит через ctor (канон репо: DI-Meter из AddAppMetrics, паттерн
// KafkaWorker.App; standalone/юниты — свой). Meter в Dispose НЕ диспозим —
// он принадлежит владельцу/DI (канон WorkerMetricsInstrumentation).
public sealed class SdGeneratorMetrics : IDisposable
{
    public const string MeterName = "SdGenerator";
    public const string LastSuccessInstrument = "sd_generator.last_success_timestamp_seconds";

    private readonly TimeProvider _time;
    private readonly System.Diagnostics.Metrics.ObservableGauge<double> _gauge;
    private long? _lastSuccessUnix;
    private bool _disposed;

    public SdGeneratorMetrics(System.Diagnostics.Metrics.Meter meter, TimeProvider time)
    {
        _time = time;
        _gauge = meter.CreateObservableGauge<double>(
            LastSuccessInstrument,
            () => !_disposed && _lastSuccessUnix is { } value
                ? [new(value)]
                : []);
    }

    /// <summary>null — серия не эмитится (до первого успеха).</summary>
    public long? LastSuccessUnix => _lastSuccessUnix;

    /// <summary>Обновляет стейт ObservableGauge временем последнего успешного тика.</summary>
    public void MarkSuccess() => _lastSuccessUnix = _time.GetUtcNow().ToUnixTimeSeconds();

    public void Dispose()
    {
        // Dispose — только прекращение эмиссии гейджа (после него колбэк не отдаёт
        // серий); сам Meter жив — им владеет DI/владелец.
        _disposed = true;
    }
}
