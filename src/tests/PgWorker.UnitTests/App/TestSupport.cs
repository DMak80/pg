using Microsoft.Extensions.Options;
using PgWorker.App;
using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.UnitTests.App;

// Общие тест-даблы App-тестов (задачи 23–24).

// Фиксированный IOptionsMonitor (значение не меняется в тесте).
internal sealed class FixedOptionsMonitor(PgWorkerOptions value) : IOptionsMonitor<PgWorkerOptions>
{
    public PgWorkerOptions CurrentValue => value;

    public IDisposable? OnChange(Action<PgWorkerOptions, string?> listener) => null;

    public PgWorkerOptions Get(string? name) => value;
}

// Полностью недоступный etcd: любой вызов — ошибка сети.
internal sealed class DeadEtcd : IEtcdGateway
{
    private static Result<T> Fail<T>()
        => Result<T>.Failed(new HttpRequestException("etcd недоступен"));

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        => Task.FromResult(Fail<IReadOnlyList<Kv>>());

    public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        => Task.FromResult(Fail<Kv?>());

    public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        => Task.FromResult(Result.Failed(new HttpRequestException("etcd недоступен")));

    public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        => Task.FromResult(Result.Failed(new HttpRequestException("etcd недоступен")));

    public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
        => Task.FromResult(Fail<TxnResult>());

    public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
        => Task.FromResult(Fail<long>());

    public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
        => Task.FromResult(Result.Failed(new HttpRequestException("etcd недоступен")));

    public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
        => Task.FromResult(Result.Failed(new HttpRequestException("etcd недоступен")));

    public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Fail<byte[]>());
}
