using Shared.Etcd.Client;

namespace EtcdNode.UnitTests.Fakes;

// Фейковый шлюз etcd на словаре ключ→значение (юниты резолвера цели слепков):
// RangeAsync — префиксный фильтр, GetAsync — точечное чтение; Put/Delete/txn
// МУТАЦИОННЫЕ (меняют Store, как FakeCoordinationGateway), txn-compare сверяется
// со словарём (Version — наличие ключа, Value — равенство, ModRevision — всегда
// совпадает: ревизии фейк не моделирует); member-API — NotSupportedException.
internal sealed class FakeEtcdGateway(IReadOnlyDictionary<string, string> kv) : IEtcdGateway
{
    // Константная ревизия засеянных ключей: юнитам история изменений не нужна.
    private const ulong Revision = 1;

    // Мутируемое хранилище (засеивается из словаря конструктора): тесты проверяют
    // формат записи чтением значений отсюда (fake.Store[key]).
    public readonly Dictionary<string, string> Store = new(kv);

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
    {
        // Все ключи с данным префиксом, детерминированный порядок — по ключу.
        IReadOnlyList<Kv> found = Store
            .Where(p => p.Key.StartsWith(prefix, StringComparison.Ordinal))
            .OrderBy(p => p.Key, StringComparer.Ordinal)
            .Select(p => new Kv(p.Key, p.Value, Revision))
            .ToArray();
        return Task.FromResult(Result<IReadOnlyList<Kv>>.Success(found));
    }

    public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        => Task.FromResult(Result<Kv?>.Success(
            Store.TryGetValue(key, out var value) ? new Kv(key, value, Revision) : null));

    public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
    {
        Store[key] = value;
        return Task.FromResult(Result.Success());
    }

    public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
    {
        // Точечное либо префиксное удаление — из Store.
        var keys = Store.Keys
            .Where(k => prefix
                ? k.StartsWith(keyOrPrefix, StringComparison.Ordinal)
                : k == keyOrPrefix)
            .ToArray();
        foreach (var key in keys)
            Store.Remove(key);
        return Task.FromResult(Result.Success());
    }

    public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
    {
        // Compare по словарю — как в FakeCoordinationGateway.
        var succeeded = req.Compare.All(c => c.Target switch
        {
            // NotExists (Equal, Num=0) — ключа нет; Exists (Greater) — ключ есть.
            TxnTarget.Version => c.Pred == TxnPredicate.Equal
                ? !Store.ContainsKey(c.Key) && c.Num == 0
                : Store.ContainsKey(c.Key),
            // ValueEqual — значение совпадает с ожидаемым.
            TxnTarget.Value => Store.TryGetValue(c.Key, out var v) && v == c.Arg,
            // Ревизии фейк не моделирует — сравнение всегда сходится.
            TxnTarget.ModRevision => true,
            _ => false,
        });
        foreach (var op in succeeded ? req.Success : req.Failure)
            Apply(op);
        return Task.FromResult(Result<TxnResult>.Success(new TxnResult(succeeded)));
    }

    // Применение операции txn-ветки к Store.
    private void Apply(TxnOp op)
    {
        switch (op)
        {
            case TxnOp.Put put:
                Store[put.Key] = put.Value;
                break;
            case TxnOp.Delete del:
            {
                var keys = Store.Keys
                    .Where(k => del.Prefix
                        ? k.StartsWith(del.Key, StringComparison.Ordinal)
                        : k == del.Key)
                    .ToArray();
                foreach (var key in keys)
                    Store.Remove(key);
                break;
            }
        }
    }

    public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
        => Task.FromResult(Result<long>.Success(1));

    public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<byte[]>.Success([]));

    public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<EtcdStatusPayload>.Success(
            new EtcdStatusPayload(null, null, null, null, null, null)));

    public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
        => throw new NotSupportedException("member-API фейком не поддерживается");

    public Task<Result<EtcdMember>> MemberAddAsync(
        string endpoint, IReadOnlyList<string> peerUrls, bool isLearner, CancellationToken ct)
        => throw new NotSupportedException("member-API фейком не поддерживается");

    public Task<Result> MemberRemoveAsync(string endpoint, ulong memberId, CancellationToken ct)
        => throw new NotSupportedException("member-API фейком не поддерживается");

    public Task<Result> MemberPromoteAsync(string endpoint, ulong memberId, CancellationToken ct)
        => throw new NotSupportedException("member-API фейком не поддерживается");

    public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

    public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result.Success());
}
