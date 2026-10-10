using System.Diagnostics.Metrics;

namespace OwnS3.App.Pipeline;

// Операционные метрики ownS3 (arch/18 + arch/owns3/05 §5): counter
// ownS3.requests{operation,code} и histogram ownS3.request.duration{operation};
// финальные имена экспорта — ownS3_requests_total / ownS3_request_duration_seconds.
public sealed class OwnS3Metrics(Meter meter)
{
    private readonly Counter<long> _requests = meter.CreateCounter<long>(
        "ownS3.requests", unit: "{request}",
        description: "S3-запросы по операциям и кодам ответов");
    private readonly Histogram<double> _duration = meter.CreateHistogram<double>(
        "ownS3.request.duration", unit: "s",
        description: "Длительность обработки S3-запроса по операциям");

    public void RequestCompleted(string operation, int code, TimeSpan duration)
    {
        _requests.Add(1, new KeyValuePair<string, object?>("operation", operation),
            new KeyValuePair<string, object?>("code", code.ToString()));
        _duration.Record(duration.TotalSeconds, new KeyValuePair<string, object?>("operation", operation));
    }
}
