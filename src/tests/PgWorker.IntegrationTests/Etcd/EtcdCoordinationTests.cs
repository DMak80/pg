using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using Xunit;

namespace PgWorker.IntegrationTests.Etcd;

// Координация на реальном etcd (задача 13, spec §4.3): клэймы, txn-compare, lease, snapshot.
[Collection(EtcdCollection.Name)]
public class EtcdCoordinationTests(EtcdFixture fixture)
{
    private EtcdGateway Gateway => fixture.Gateway;

    private string Endpoint => fixture.Endpoint;

    private ClaimStore NewClaimStore() => new([Endpoint], Gateway, TimeProvider.System);

    [Fact]
    public async Task TwoClaimStores_MutualExclusion()
    {
        // Arrange — два «инстанса» PgWorker
        var first = NewClaimStore();
        var second = NewClaimStore();
        var ct = TestContext.Current.CancellationToken;

        // Act
        var firstClaim = await first.TryClaimClusterAsync("shop", ct);
        var secondClaim = await second.TryClaimClusterAsync("shop", ct);

        // Assert: exclusivity обработки кластера одним инстансом (Д2)
        firstClaim.IsSuccess.Should().BeTrue();
        firstClaim.Value.Should().BeTrue();
        secondClaim.IsSuccess.Should().BeTrue();
        secondClaim.Value.Should().BeFalse();

        await first.DisposeAsync();
        await second.DisposeAsync();
    }

    [Fact]
    public async Task ClaimExpiry_AfterLeaseTtl_SecondInstanceTakesOver()
    {
        // Arrange — «умерший» держатель: leased-ключ TTL 2с, который никто не продлевает
        var ct = TestContext.Current.CancellationToken;
        var grant = await Gateway.LeaseGrantAsync(Endpoint, 2, ct);
        grant.IsSuccess.Should().BeTrue();
        var claimTxn = await Gateway.TxnAsync(
            Endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists("/pgworker/claims/shop")],
                [new TxnOp.Put("/pgworker/claims/shop", """{"instance":"dead"}""", grant.Value)]),
            ct);
        claimTxn.Value.Succeeded.Should().BeTrue();

        // Act — lease истекает, etcd сам удаляет ключ; второй инстанс захватывает
        await Task.Delay(3000, ct);
        var second = NewClaimStore();
        var reclaimed = await second.TryClaimClusterAsync("shop", ct);

        // Assert
        reclaimed.IsSuccess.Should().BeTrue();
        reclaimed.Value.Should().BeTrue();
        second.IsMine("shop").Should().BeTrue();
        await second.DisposeAsync();
    }

    [Fact]
    public async Task TxnCompareValue_ConcurrentFlipRejected()
    {
        // Arrange — routing бакета принадлежит shard2 (чужой конкурент пишет shard1)
        var ct = TestContext.Current.CancellationToken;
        const string key = "/clusters/shop/buckets/routing/bucket_1";
        var put = await Gateway.PutAsync(Endpoint, key, "shard2", lease: null, ct);
        put.IsSuccess.Should().BeTrue();

        // Act — flip с устаревшим ожиданием (конкурент уже перебил значение)
        var stale = await Gateway.TxnAsync(
            Endpoint,
            TxnRequest.Of([TxnCompare.ValueEqual(key, "shard1")], [new TxnOp.Put(key, "shard3", null)]),
            ct);
        var actual = await Gateway.TxnAsync(
            Endpoint,
            TxnRequest.Of([TxnCompare.ValueEqual(key, "shard2")], [new TxnOp.Put(key, "shard1", null)]),
            ct);

        // Assert: чужой flip отклонён, актуальный применён (арх/11 §5 шаг 4.7)
        stale.Value.Succeeded.Should().BeFalse();
        actual.Value.Succeeded.Should().BeTrue();
        var after = await Gateway.GetAsync(Endpoint, key, ct);
        after.Value!.Value.Should().Be("shard1");
    }

    [Fact]
    public async Task LeasedPut_ExpiresAfterTtl()
    {
        // Arrange — ключ на коротком lease
        var ct = TestContext.Current.CancellationToken;
        var grant = await Gateway.LeaseGrantAsync(Endpoint, 2, ct);
        await Gateway.PutAsync(Endpoint, "/pgworker/test-lease", "v", grant.Value, ct);

        // Act
        var before = await Gateway.GetAsync(Endpoint, "/pgworker/test-lease", ct);
        await Task.Delay(3000, ct);
        var after = await Gateway.GetAsync(Endpoint, "/pgworker/test-lease", ct);

        // Assert: ключ исчез сам по истечении lease
        before.Value.Should().NotBeNull();
        after.Value.Should().BeNull();
    }

    [Fact]
    public async Task SnapshotSave_ReturnsNonEmptyBlob()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await Gateway.PutAsync(Endpoint, "/pgworker/test-snapshot", "v", lease: null, ct);

        // Act
        var result = await Gateway.SnapshotSaveAsync(Endpoint, ct);

        // Assert: реальный слепок БД etcd (P12) — непустой бинарник
        result.IsSuccess.Should().BeTrue(result.Error?.ToString());
        result.Value.Should().NotBeEmpty();
        result.Value.Length.Should().BeGreaterThan(1024);
    }
}
