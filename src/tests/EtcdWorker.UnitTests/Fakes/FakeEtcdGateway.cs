using Shared.Etcd.Client;

namespace EtcdWorker.UnitTests.Fakes;

// Фейковый шлюз etcd на словаре ключ→значение (юниты резолвера и т.п.):
// RangeAsync — префиксный фильтр, GetAsync — точечное чтение; остальные
// kv-операции — Success-заглушки, member-API — NotSupportedException.
internal sealed class FakeEtcdGateway(IReadOnlyDictionary<string, string> kv) : IEtcdGateway
{
    // Константная ревизия: юнитам история изменений не нужна.
    private const ulong Revision = 1;

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
    {
        // Все ключи с данным префиксом, детерминированный порядок — по ключу.
        IReadOnlyList<Kv> found = kv
            .Where(p => p.Key.StartsWith(prefix, StringComparison.Ordinal))
            .OrderBy(p => p.Key, StringComparer.Ordinal)
            .Select(p => new Kv(p.Key, p.Value, Revision))
            .ToArray();
        return Task.FromResult(Result<IReadOnlyList<Kv>>.Success(found));
    }

    public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        => Task.FromResult(Result<Kv?>.Success(
            kv.TryGetValue(key, out var value) ? new Kv(key, value, Revision) : null));

    public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
        => Task.FromResult(Result<TxnResult>.Success(new TxnResult(true)));

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
