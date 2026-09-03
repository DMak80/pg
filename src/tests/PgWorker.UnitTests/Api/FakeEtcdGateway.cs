using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.UnitTests.Api;

// Мини-имитация etcd в памяти для юнит-тестов API-хендлеров (t07): range/get/
// put/delete + txn-compare version==0. Порт fake из Etcd/CoordinationTests.
internal sealed class FakeEtcdGateway : IEtcdGateway
{
    public Dictionary<string, string> Store { get; } = [];

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<Kv>>.Success(
            Store.Where(p => p.Key.StartsWith(prefix, StringComparison.Ordinal))
                .Select(p => new Kv(p.Key, p.Value, 1)).ToList()));

    public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        => Task.FromResult(Result<Kv?>.Success(Store.TryGetValue(key, out var v) ? new Kv(key, v, 1) : null));

    public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
    {
        Store[key] = value;
        return Task.FromResult(Result.Success());
    }

    public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
    {
        foreach (var key in Store.Keys.Where(k => prefix
                     ? k.StartsWith(keyOrPrefix, StringComparison.Ordinal)
                     : k == keyOrPrefix).ToList())
            Store.Remove(key);
        return Task.FromResult(Result.Success());
    }

    public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
        => Task.FromResult(Result<TxnResult>.Success(new TxnResult(
            req.Compare.All(c => c.Target == TxnTarget.Version
                && (!Store.ContainsKey(c.Key) && c.Num == 0 || Store.ContainsKey(c.Key) && c.Num != 0)))));

    // Не нужно юнит-тестам API-хендлеров (только чтение/txn выше); реализация
    // возвращает успех, чтобы fake удовлетворял интерфейсу.
    public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
        => Task.FromResult(Result<long>.Success(1));

    public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<byte[]>.Failed(new NotSupportedException("fake: snapshot")));

    public Task<Result<long>> StatusAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<long>.Success(1));

    public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result.Success());
}
