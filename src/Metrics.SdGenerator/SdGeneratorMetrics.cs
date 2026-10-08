namespace Metrics.SdGenerator;

// Каркас Task 2: только константы — на MeterName ссылается Program.cs
// (AddAppMetrics); ctor(meter, time), LastSuccessUnix, MarkSuccess, Dispose — Task 5.
public sealed class SdGeneratorMetrics
{
    public const string MeterName = "SdGenerator";
    public const string LastSuccessInstrument = "sd_generator.last_success_timestamp_seconds";
}
