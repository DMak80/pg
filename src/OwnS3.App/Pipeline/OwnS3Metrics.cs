using System.Diagnostics.Metrics;

namespace OwnS3.App.Pipeline;

// Операционные метрики ownS3 (arch/18 + arch/owns3/05 §5): counter
// ownS3.requests{operation,code} и histogram ownS3.request.duration{operation};
// gauge ownS3.disk.{used,total}.bytes — обновляет VolumeCleanupService.
// Финальные имена экспорта — ownS3_requests_total / ownS3_request_duration_seconds /
// ownS3_disk_used_bytes / ownS3_disk_total_bytes.
public sealed class OwnS3Metrics
{
    private readonly Counter<long> _requests;
    private readonly Histogram<double> _duration;
    private long _diskUsedBytes;
    private long _diskTotalBytes;

    public OwnS3Metrics(Meter meter)
    {
        _requests = meter.CreateCounter<long>("ownS3.requests", unit: "{request}",
            description: "S3-запросы по операциям и кодам ответов");
        _duration = meter.CreateHistogram<double>("ownS3.request.duration", unit: "s",
            description: "Длительность обработки S3-запроса по операциям");
        // Регистрация gauge в Meter (экземпляры инструментов держит сам Meter):
        // финальные серии ownS3_disk_used_bytes / ownS3_disk_total_bytes —
        // прецедент ownS3_requests_total
        meter.CreateObservableGauge<long>("ownS3.disk.used.bytes",
            () => new Measurement<long>(Interlocked.Read(ref _diskUsedBytes)),
            unit: "By", description: "Занято на томе данных");
        meter.CreateObservableGauge<long>("ownS3.disk.total.bytes",
            () => new Measurement<long>(Interlocked.Read(ref _diskTotalBytes)),
            unit: "By", description: "Полный размер тома данных");
    }

    // Дисковые gauge (потокобезопасно; заполняет VolumeCleanupService).
    internal long DiskUsedBytes
    {
        get => Interlocked.Read(ref _diskUsedBytes);
        set => Interlocked.Exchange(ref _diskUsedBytes, value);
    }

    internal long DiskTotalBytes
    {
        get => Interlocked.Read(ref _diskTotalBytes);
        set => Interlocked.Exchange(ref _diskTotalBytes, value);
    }

    public void RequestCompleted(string operation, int code, TimeSpan duration)
    {
        _requests.Add(1, new KeyValuePair<string, object?>("operation", operation),
            new KeyValuePair<string, object?>("code", code.ToString()));
        _duration.Record(duration.TotalSeconds, new KeyValuePair<string, object?>("operation", operation));
    }
}
