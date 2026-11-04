using PgWorker.Core;
using Shared.Etcd.Client;

namespace PgWorker.UnitTests.Api;

// Мини-имитация etcd в памяти для юнит-тестов API-хендлеров : range/get/
// put/delete + txn-compare version==0. Порт fake из Etcd/CoordinationTests.
internal sealed class FakeEtcdGateway : IEtcdGateway
{
    public Dictionary<string, string> Store { get; } = [];

    // Мод-ревизии ключей (инкремент на каждый put; 0 — ключа нет): для
    // txn-компарера TxnTarget.ModRevision (как реальный etcd).
    private readonly System.Collections.Generic.Dictionary<string, ulong> _modRev = [];
    private ulong _revision;

    // Однократный хук после GetAsync (t15: гонка «сторонний put между чтением
    // и bump'ом счётчика»).
    public Action? OnAfterGet { get; set; }

    // Транспортный отказ чтения выбранных ключей (t15: transient-ветка чтения
    // счётчика wal-life); null — отказов нет.
    public Func<string, bool>? FailGet { get; set; }

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<Kv>>.Success(
            Store.Where(p => p.Key.StartsWith(prefix, StringComparison.Ordinal))
                .Select(p => new Kv(p.Key, p.Value, ModRevOf(p.Key))).ToList()));

    public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
    {
        if (FailGet?.Invoke(key) == true)
            return Task.FromResult(Result<Kv?>.Failed(new EtcdUnreachableException("etcd down")));
        // Гонка-хук (t15): однократное стороннее изменение ключа после чтения.
        var hook = OnAfterGet;
        if (hook is not null)
        {
            OnAfterGet = null;
            hook();
        }
        return Task.FromResult(Result<Kv?>.Success(
            Store.TryGetValue(key, out var v) ? new Kv(key, v, ModRevOf(key)) : null));
    }

    public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
    {
        Store[key] = value;
        TouchModRev(key);
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
    {
        // compare: Version (ключа нет/есть) и ModRevision (равенство с реальной
        // ревизией ключа; ключа нет — ревизия 0); сошёлся — применяем success-ветку.
        var succeeded = req.Compare.All(c => c.Target switch
        {
            TxnTarget.Version
                => (!Store.ContainsKey(c.Key) && c.Num == 0) || (Store.ContainsKey(c.Key) && c.Num != 0),
            TxnTarget.ModRevision => ModRevOf(c.Key) == (ulong)c.Num,
            _ => false,
        });
        if (succeeded)
            foreach (var op in req.Success)
                ApplyTxnOp(op);
        return Task.FromResult(Result<TxnResult>.Success(new TxnResult(succeeded)));
    }

    private void ApplyTxnOp(TxnOp op)
    {
        switch (op)
        {
            case TxnOp.Put put:
                Store[put.Key] = put.Value;
                TouchModRev(put.Key);
                break;
            case TxnOp.Delete delete:
                Store.Remove(delete.Key);
                break;
        }
    }

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

    public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<EtcdStatusPayload>.Success(new EtcdStatusPayload(null, null, null, null, null, 1)));

    public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

    // Member-API этими тестами не используется — заглушки интерфейса.
    public Task<Result<EtcdMember>> MemberAddAsync(string endpoint, IReadOnlyList<string> peerUrls, bool isLearner, CancellationToken ct)
        => throw new NotSupportedException("member-API не поддерживается этим фейком");

    public Task<Result> MemberRemoveAsync(string endpoint, ulong memberId, CancellationToken ct)
        => throw new NotSupportedException("member-API не поддерживается этим фейком");

    public Task<Result> MemberPromoteAsync(string endpoint, ulong memberId, CancellationToken ct)
        => throw new NotSupportedException("member-API не поддерживается этим фейком");

    public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

    public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result.Success());

    private ulong ModRevOf(string key)
        => _modRev.TryGetValue(key, out var rev) ? rev : 0;

    private void TouchModRev(string key)
        => _modRev[key] = ++_revision;
}
