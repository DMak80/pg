using Shared.Etcd.Client;

namespace Shared.Etcd.UnitTests;

// Канон in-memory-фейка etcd для тестов координации (t09): txn-compare
// version/value, lease grant/revoke/keepalive. Перенесён из Pg-копии
// CoordinationTests; расширен TxnFault (инъекция сбоя txn), Seed (прямой посев
// ключа) и PutFault (инъекция сбоя Put — кейс «неудачный Put — без эвента»).
internal sealed class FakeCoordinationGateway : IEtcdGateway
{
    public Dictionary<string, string> Store = [];
    public readonly Dictionary<string, long> KeyLeases = [];
    public readonly HashSet<long> LiveLeases = [];
    public readonly List<TxnRequest> Txns = [];
    public readonly List<long> KeepaliveCalls = [];
    public Func<long, Result>? KeepaliveOverride;
    public Func<Result<long>>? GrantOverride;

    // Инъекция сбоя txn (Race/сбой etcd): если задан — возвращает её результат.
    public Func<TxnRequest, Result<TxnResult>>? TxnFault;

    // Инъекция сбоя Put (etcd недоступен): если задан — возвращает её результат.
    public Func<string, Result>? PutFault;

    // Прямой посев ключа (имитация перехвата к чужого инстанса): put БЕЗ lease —
    // снимает привязку ключа к прежнему lease (семантика etcd), иначе revoke
    // держателя удалил бы перезаписанный чужой ключ.
    public void Seed(string key, string value)
    {
        Store[key] = value;
        KeyLeases.Remove(key);
    }

    private long _nextLease = 100;

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
    {
        var kvs = Store
            .Where(p => p.Key.StartsWith(prefix, StringComparison.Ordinal))
            .Select(p => new Kv(p.Key, p.Value, 1))
            .ToList();
        return Task.FromResult(Result<IReadOnlyList<Kv>>.Success(kvs));
    }

    public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        => Task.FromResult(Result<Kv?>.Success(
            Store.TryGetValue(key, out var v) ? new Kv(key, v, 1) : null));

    public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
    {
        if (PutFault is { } fault)
            return Task.FromResult(fault(key));
        Store[key] = value;
        if (lease is { } l)
            KeyLeases[key] = l;
        return Task.FromResult(Result.Success());
    }

    public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
    {
        var keys = Store.Keys
            .Where(k => prefix
                ? k.StartsWith(keyOrPrefix, StringComparison.Ordinal)
                : k == keyOrPrefix)
            .ToList();
        foreach (var key in keys)
        {
            Store.Remove(key);
            KeyLeases.Remove(key);
        }

        return Task.FromResult(Result.Success());
    }

    public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
    {
        if (TxnFault is { } f)
            return Task.FromResult(f(req));
        Txns.Add(req);
        var succeeded = req.Compare.All(c => c.Target switch
        {
            TxnTarget.Version => !Store.ContainsKey(c.Key) && c.Num == 0
                || (Store.ContainsKey(c.Key) && c.Num != 0),
            TxnTarget.Value => Store.TryGetValue(c.Key, out var v) && v == c.Arg,
            TxnTarget.ModRevision => true, // fake не моделирует ревизии
            _ => false,
        });
        if (succeeded)
            foreach (var op in req.Success)
                Apply(op);
        else
            foreach (var op in req.Failure)
                Apply(op);
        return Task.FromResult(Result<TxnResult>.Success(new TxnResult(succeeded)));
    }

    private void Apply(TxnOp op)
    {
        switch (op)
        {
            case TxnOp.Put put:
                Store[put.Key] = put.Value;
                if (put.Lease is { } l)
                    KeyLeases[put.Key] = l;
                break;
            case TxnOp.Delete del:
                DeleteAsync(string.Empty, del.Key, del.Prefix, CancellationToken.None).GetAwaiter().GetResult();
                break;
        }
    }

    public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
    {
        if (GrantOverride is { } over)
            return Task.FromResult(over());
        var id = ++_nextLease;
        LiveLeases.Add(id);
        return Task.FromResult(Result<long>.Success(id));
    }

    public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
    {
        LiveLeases.Remove(lease);
        foreach (var key in KeyLeases.Where(p => p.Value == lease).Select(p => p.Key).ToList())
        {
            Store.Remove(key);
            KeyLeases.Remove(key);
        }

        return Task.FromResult(Result.Success());
    }

    public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
    {
        KeepaliveCalls.Add(lease);
        return Task.FromResult(
            KeepaliveOverride is { } over ? over(lease) : Result.Success());
    }

    public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<byte[]>.Success([1, 2, 3]));

    public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<EtcdStatusPayload>.Success(new EtcdStatusPayload(null, null, null, null, null, 1)));

    public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

    public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

    public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result.Success());
}
