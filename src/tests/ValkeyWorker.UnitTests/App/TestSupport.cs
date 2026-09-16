using Microsoft.Extensions.Options;
using ValkeyWorker.App;
using Shared.Etcd.Client;

namespace ValkeyWorker.UnitTests.App;

// IOptionsMonitor-дабл (порт KafkaWorker.UnitTests/App/TestSupport.cs): фиксированные
// настройки ValkeyWorkerOptions для тестов циклов/health (t09).
internal sealed class FixedOptionsMonitor(ValkeyWorkerOptions value) : IOptionsMonitor<ValkeyWorkerOptions>
{
    public ValkeyWorkerOptions CurrentValue => value;

    public IDisposable? OnChange(Action<ValkeyWorkerOptions, string?> listener) => null;

    public ValkeyWorkerOptions Get(string? name) => value;
}

// Шлюз, бросающий сетевые исключения (t09): .NET DNS-флейп
// «Name or service not known» летит из HttpClient наружу.
internal sealed class ThrowingEtcd : IEtcdGateway
{
    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(
        string endpoint, string prefix, CancellationToken ct)
        => throw new HttpRequestException($"Name or service not known ({new Uri(endpoint).Host}:2379)");

    public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<Result<TxnResult>> TxnAsync(
        string endpoint, TxnRequest req, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
        => throw new HttpRequestException("unreachable");
}
