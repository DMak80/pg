using Shared.Etcd.Client;
using Shared.Etcd.Coordination;

namespace Shared.Etcd.UnitTests;

// Фейк gateway для кэша дельт (EtcdWatchedCacheTests): Store с ревизиями +
// сценарии watch-попыток. Каждая попытка watch снимает сценарий из очереди
// (сценарий доставляет события через OnEvent и возвращает исход потока);
// без сценария поток держится открытым до отмены (долгоживущая подписка).
internal sealed class FakeWatchGateway : IEtcdGateway, IEtcdWatchApi
{
    public sealed record WatchContext(
        string Endpoint,
        long StartRevision,
        Func<EtcdWatchEvent, Task> OnEvent,
        CancellationToken Ct);

    public Dictionary<string, (string Value, long ModRevision)> Store = [];
    public int RangeCalls;
    public List<(string Endpoint, long StartRevision)> WatchAttempts = [];
    public Queue<Func<WatchContext, Task<EtcdWatchStop>>> Scripts = [];

    public void Seed(string key, string value, long modRevision) => Store[key] = (value, modRevision);

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
    {
        RangeCalls++;
        IReadOnlyList<Kv> kvs = Store
            .Where(p => p.Key.StartsWith(prefix, StringComparison.Ordinal))
            .Select(p => new Kv(p.Key, p.Value.Item1, (ulong)p.Value.Item2))
            .ToList();
        return Task.FromResult(Result<IReadOnlyList<Kv>>.Success(kvs));
    }

    public async Task<Result<EtcdWatchStop>> WatchAsync(
        string endpoint, string prefix, long startRevision, Func<EtcdWatchEvent, Task> onEvent, CancellationToken ct)
    {
        WatchAttempts.Add((endpoint, startRevision));
        if (Scripts.Count > 0)
            return await Scripts.Dequeue()(new WatchContext(endpoint, startRevision, onEvent, ct));

        // Без сценария — долгоживущая подписка: держим поток открытым до отмены.
        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        }
        catch (OperationCanceledException)
        {
        }

        return Result<EtcdWatchStop>.Success(new EtcdWatchStop(EtcdWatchStopReason.Cancelled));
    }

    // --- Прочие операции интерфейса кэшем/тестами не используются ---

    public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result<EtcdMember>> MemberAddAsync(
        string endpoint, IReadOnlyList<string> peerUrls, bool isLearner, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result> MemberRemoveAsync(string endpoint, ulong memberId, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result> MemberPromoteAsync(string endpoint, ulong memberId, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");

    public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
        => throw new NotSupportedException("кэш дельт использует только Range + watch");
}
