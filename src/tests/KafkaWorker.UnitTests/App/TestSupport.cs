using Microsoft.Extensions.Options;
using KafkaWorker.App;

namespace KafkaWorker.UnitTests.App;

// IOptionsMonitor-дабл (порт PgWorker.UnitTests/App/TestSupport.cs): фиксированные
// настройки KafkaWorkerOptions для тестов циклов/health (t09).
internal sealed class FixedOptionsMonitor(KafkaWorkerOptions value) : IOptionsMonitor<KafkaWorkerOptions>
{
    public KafkaWorkerOptions CurrentValue => value;

    public IDisposable? OnChange(Action<KafkaWorkerOptions, string?> listener) => null;

    public KafkaWorkerOptions Get(string? name) => value;
}

// Шлюз, бросающий сетевые исключения (t09; spec §3.2): .NET DNS-флейп
// «Name or service not known» летит из HttpClient наружу.
internal sealed class ThrowingEtcd : KafkaWorker.Etcd.Client.IEtcdGateway
{
    public Task<KafkaWorker.Core.Result<IReadOnlyList<KafkaWorker.Etcd.Client.Kv>>> RangeAsync(
        string endpoint, string prefix, CancellationToken ct)
        => throw new HttpRequestException($"Name or service not known ({new Uri(endpoint).Host}:2379)");

    public Task<KafkaWorker.Core.Result<KafkaWorker.Etcd.Client.Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result<KafkaWorker.Etcd.Client.TxnResult>> TxnAsync(
        string endpoint, KafkaWorker.Etcd.Client.TxnRequest req, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result<long>> StatusAsync(string endpoint, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result> DefragmentAsync(string endpoint, CancellationToken ct)
        => throw new HttpRequestException("unreachable");
}
