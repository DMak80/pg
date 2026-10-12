using System.Diagnostics.Metrics;

namespace S3Worker.App.Api;

// Счётчик операций API (spec §9): s3worker_api_operations_total{operation,code} —
// инкремент в map-обработчиках ApiModule (успех и ошибки — один хук в финале).
// Экспозиция — общий /metrics (Task 3, AddAppMetrics).
public sealed class S3ApiMetrics(Meter meter, string name = "s3worker_api_operations_total")
{
    private readonly Counter<long> _counter = meter.CreateCounter<long>(name);

    // operation ∈ create-cluster | delete-cluster | add-node | delete-node |
    // update-node-resources | post-rotation | sync-probe | seed-demo.
    public void Track(string operation, int statusCode)
        => _counter.Add(1, new KeyValuePair<string, object?>("operation", operation),
            new KeyValuePair<string, object?>("code", statusCode.ToString(System.Globalization.CultureInfo.InvariantCulture)));
}
