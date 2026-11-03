using Shared.Etcd.Client;
using Shared.Etcd.Coordination;

namespace Shared.Etcd.UnitTests;

// Фейк gateway для опросного кэша (EtcdPrefixPollCacheTests): Store с ревизиями,
// хук мутации перед каждым Range (детерминизм сценариев опроса) и инъекция
// сбоя Range по номеру вызова.
internal sealed class FakePollGateway : IEtcdGateway
{
    public Dictionary<string, (string Value, long ModRevision)> Store = [];
    public int RangeCalls;

    // Хук перед каждым Range: по RangeCalls мутирует Store (сценарий опроса).
    public Action? OnRange;

    // Инъекция сбоя Range: вызов № → сбой (не Success → опрос считается не удавшимся).
    public Func<int, Result<IReadOnlyList<Kv>>>? RangeFault;

    public void Seed(string key, string value, long modRevision) => Store[key] = (value, modRevision);

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
    {
        RangeCalls++;
        OnRange?.Invoke();
        if (RangeFault is { } fault)
        {
            var faulted = fault(RangeCalls);
            if (!faulted.IsSuccess)
                return Task.FromResult(faulted);
        }

        IReadOnlyList<Kv> kvs = Store
            .Where(p => p.Key.StartsWith(prefix, StringComparison.Ordinal))
            .Select(p => new Kv(p.Key, p.Value.Item1, (ulong)p.Value.Item2))
            .ToList();
        return Task.FromResult(Result<IReadOnlyList<Kv>>.Success(kvs));
    }

    // --- Прочие операции интерфейса кэшем/тестами не используются ---

    public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result<EtcdMember>> MemberAddAsync(
        string endpoint, IReadOnlyList<string> peerUrls, bool isLearner, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result> MemberRemoveAsync(string endpoint, ulong memberId, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result> MemberPromoteAsync(string endpoint, ulong memberId, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");

    public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
        => throw new NotSupportedException("опросный кэш использует только Range");
}
