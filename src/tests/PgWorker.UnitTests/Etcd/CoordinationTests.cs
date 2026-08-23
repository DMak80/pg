using PgWorker.Core;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using Xunit;

namespace PgWorker.UnitTests.Etcd;

// ClaimStore (клэймы/лидерство) и WorkJournal (/pgworker/work, /pgworker/evacuations) — задача 12.
public class CoordinationTests
{
    // Мини-имитация etcd в памяти: txn-compare version/value, lease grant/revoke/keepalive.
    private sealed class FakeGateway : IEtcdGateway
    {
        public Dictionary<string, string> Store = [];
        public readonly Dictionary<string, long> KeyLeases = [];
        public readonly HashSet<long> LiveLeases = [];
        public readonly List<TxnRequest> Txns = [];
        public readonly List<long> KeepaliveCalls = [];
        public Func<long, Result>? KeepaliveOverride;

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
    }

    private static ClaimStore NewStore(FakeGateway gateway)
        => new(["http://etcd:2379"], gateway, TimeProvider.System);

    private static WorkJournal NewJournal(FakeGateway gateway) => new(gateway, ["http://etcd:2379"]);

    [Fact]
    public async Task ClaimStore_TryClaimCluster_TxnCompareVersionZero_PutsLeasedKey()
    {
        // Arrange
        var gateway = new FakeGateway();
        var store = NewStore(gateway);

        // Act
        var result = await store.TryClaimClusterAsync("shop", CancellationToken.None);

        // Assert: txn с compare version==0 на /pgworker/claims/<C>, ключ записан с instance
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeTrue();
        var txn = gateway.Txns.Should().ContainSingle().Subject;
        var compare = txn.Compare.Should().ContainSingle().Subject;
        compare.Key.Should().Be("/pgworker/claims/shop");
        compare.Target.Should().Be(TxnTarget.Version);
        compare.Num.Should().Be(0);
        var put = txn.Success.Should().ContainSingle().Subject.Should().BeAssignableTo<TxnOp.Put>().Which;
        put.Lease.Should().NotBeNull();
        gateway.Store.Should().ContainKey("/pgworker/claims/shop");
        gateway.Store["/pgworker/claims/shop"].Should().Contain(store.InstanceId);
        store.IsMine("shop").Should().BeTrue();
    }

    [Fact]
    public async Task ClaimStore_TryClaimCluster_AlreadyClaimedByOther_ReturnsFalse()
    {
        // Arrange — чужой клэйм уже в etcd
        var gateway = new FakeGateway
        {
            Store = new Dictionary<string, string> { ["/pgworker/claims/shop"] = """{"instance":"other"}""" },
        };
        var store = NewStore(gateway);

        // Act
        var result = await store.TryClaimClusterAsync("shop", CancellationToken.None);

        // Assert: txn не сошёлся → false (занят), не ошибка
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeFalse();
        store.IsMine("shop").Should().BeFalse();
    }

    [Fact]
    public async Task ClaimStore_TryBecomeLeader_WritesLeaderKey()
    {
        // Arrange
        var gateway = new FakeGateway();
        var store = NewStore(gateway);

        // Act
        var result = await store.TryBecomeLeaderAsync(CancellationToken.None);

        // Assert
        result.Value.Should().BeTrue();
        store.IsLeader.Should().BeTrue();
        gateway.Store.Should().ContainKey("/pgworker/leader");
    }

    [Fact]
    public async Task ClaimStore_KeepaliveTick_ExtendsAllLiveLeases()
    {
        // Arrange — 2 клэйма + лидерство
        var gateway = new FakeGateway();
        var store = NewStore(gateway);
        await store.TryClaimClusterAsync("shop", CancellationToken.None);
        await store.TryClaimClusterAsync("billing", CancellationToken.None);
        await store.TryBecomeLeaderAsync(CancellationToken.None);
        gateway.KeepaliveCalls.Clear();

        // Act
        await store.KeepaliveTickAsync(CancellationToken.None);

        // Assert: продлены все три lease
        gateway.KeepaliveCalls.Should().HaveCount(3);
    }

    [Fact]
    public async Task ClaimStore_ReleaseCluster_DeletesKeyAndRevokesLease()
    {
        // Arrange
        var gateway = new FakeGateway();
        var store = NewStore(gateway);
        await store.TryClaimClusterAsync("shop", CancellationToken.None);

        // Act
        await store.ReleaseClusterAsync("shop", CancellationToken.None);

        // Assert: ключ удалён, lease отозван, IsMine=false
        gateway.Store.Should().NotContainKey("/pgworker/claims/shop");
        gateway.LiveLeases.Should().BeEmpty();
        store.IsMine("shop").Should().BeFalse();
    }

    [Fact]
    public async Task ClaimStore_KeepaliveFailure_ClaimIsLost_AndCanBeReclaimed()
    {
        // Arrange — keepalive отвечает ошибкой (lease истёк на стороне etcd; etcd сам
        // удалил ключи под истёкшим lease — имитируем это в fake)
        var gateway = new FakeGateway();
        var store = NewStore(gateway);
        await store.TryClaimClusterAsync("shop", CancellationToken.None);
        gateway.KeepaliveOverride = lease =>
        {
            foreach (var key in gateway.KeyLeases.Where(p => p.Value == lease).Select(p => p.Key).ToList())
            {
                gateway.Store.Remove(key);
                gateway.KeyLeases.Remove(key);
            }

            gateway.LiveLeases.Remove(lease);
            return Result.Failed(new ApplicationException("lease expired"));
        };

        // Act
        await store.KeepaliveTickAsync(CancellationToken.None);
        var lostAfterTick = store.IsMine("shop");
        var reclaimed = await store.TryClaimClusterAsync("shop", CancellationToken.None);

        // Assert: клэйм потерян (IsMine=false), следующий TryClaim пере-захватывает
        lostAfterTick.Should().BeFalse();
        reclaimed.Value.Should().BeTrue();
        store.IsMine("shop").Should().BeTrue();
    }

    [Fact]
    public async Task ClaimStore_DisposeAsync_RevokesAllLeases()
    {
        // Arrange
        var gateway = new FakeGateway();
        var store = NewStore(gateway);
        await store.TryClaimClusterAsync("shop", CancellationToken.None);
        await store.TryBecomeLeaderAsync(CancellationToken.None);

        // Act
        await store.DisposeAsync();

        // Assert: все lease отозваны, ключи под ними исчезли
        gateway.LiveLeases.Should().BeEmpty();
        gateway.Store.Should().NotContainKey("/pgworker/claims/shop");
        gateway.Store.Should().NotContainKey("/pgworker/leader");
    }

    [Fact]
    public async Task WorkJournal_WritePhase_PutsCamelCaseJson()
    {
        // Arrange
        var gateway = new FakeGateway();
        var journal = NewJournal(gateway);

        // Act
        var result = await journal.WritePhaseAsync("shop", "provision", "planned", "inst-1", null, CancellationToken.None);

        // Assert: ключ /pgworker/work/<C>, camelCase-поля
        result.IsSuccess.Should().BeTrue();
        gateway.Store.Should().ContainKey("/pgworker/work/shop");
        var raw = gateway.Store["/pgworker/work/shop"];
        raw.Should().Contain("\"op\":\"provision\"");
        raw.Should().Contain("\"phase\":\"planned\"");
        raw.Should().Contain("\"instance\":\"inst-1\"");
        raw.Should().Contain("\"updated_unix\":");
    }

    [Fact]
    public async Task WorkJournal_RoundTrip_WorkState()
    {
        // Arrange
        var gateway = new FakeGateway();
        var journal = NewJournal(gateway);
        await journal.WritePhaseAsync("shop", "deprovision", "removing-nodes", "inst-2", "boom", CancellationToken.None);

        // Act
        var result = await journal.ReadAsync("shop", CancellationToken.None);

        // Assert: все поля пережили запись/чтение без потерь
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeNull();
        var state = result.Value!;
        state.Op.Should().Be("deprovision");
        state.Phase.Should().Be("removing-nodes");
        state.Instance.Should().Be("inst-2");
        state.LastError.Should().Be("boom");
        state.UpdatedUnix.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task WorkJournal_RoundTrip_EvacuationJournal()
    {
        // Arrange
        var gateway = new FakeGateway();
        var journal = NewJournal(gateway);
        var original = new EvacuationJournal(
            new Dictionary<int, string> { [0] = "shard1", [3] = "shard1" },
            "shard-dead",
            1755900000,
            "QUARANTINED",
            1755900600);

        // Act
        await journal.WriteEvacuationAsync("shop", "shard2", original, CancellationToken.None);
        var result = await journal.ReadEvacuationAsync("shop", "shard2", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeNull();
        var journalRead = result.Value!;
        journalRead.Buckets.Should().BeEquivalentTo(original.Buckets);
        journalRead.Reason.Should().Be("shard-dead");
        journalRead.State.Should().Be("QUARANTINED");
        journalRead.ReturnedUnix.Should().Be(1755900600);
    }
}
