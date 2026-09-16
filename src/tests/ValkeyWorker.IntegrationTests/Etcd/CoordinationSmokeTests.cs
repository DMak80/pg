using FluentAssertions;
using Shared.Etcd.Client;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Etcd;

// Смоук координации /valkeyworker/ на реальном etcd (порт ClaimStoreTests +
// PortAllocLockRaceTests KafkaWorker в урезанном виде): claim/takeover и
// portalloc-lock взаимоисключение — ОДНО отличие от kfw: префикс /valkeyworker.
// Глубокие протоколы уже покрыты Shared.Etcd.UnitTests — смоук только на префикс.
[Collection(ValkeyEtcdCollection.Name)]
public class CoordinationSmokeTests(ValkeyEtcdFixture fixture)
{
    private string Endpoint => fixture.Endpoint;

    private EtcdGateway Gateway => fixture.Gateway;

    private ClaimStore NewClaimStore() => new("/valkeyworker", [Endpoint], Gateway, TimeProvider.System);

    [Fact]
    public async Task TwoClaimStores_MutualExclusion()
    {
        // Arrange: два «инстанса» ValkeyWorker, имя кластера — уникальное на прогон
        // (etcd фикстуры общий для классов коллекции).
        var cluster = $"smoke{Guid.NewGuid().ToString("N")[..8]}";
        var first = NewClaimStore();
        var second = NewClaimStore();
        var ct = TestContext.Current.CancellationToken;

        // Act: оба пытаются захватить кластер.
        var firstClaim = await first.TryClaimClusterAsync(cluster, ct);
        var secondClaim = await second.TryClaimClusterAsync(cluster, ct);

        // Assert: exclusivity — кластер обрабатывает один инстанс.
        firstClaim.IsSuccess.Should().BeTrue();
        firstClaim.Value.Should().BeTrue();
        secondClaim.IsSuccess.Should().BeTrue();
        secondClaim.Value.Should().BeFalse();

        // Cleanup: первый освобождает — второй забирает (takeover по release).
        await first.ReleaseClusterAsync(cluster, ct);
        var reclaimed = await second.TryClaimClusterAsync(cluster, ct);
        reclaimed.Value.Should().BeTrue();

        await first.DisposeAsync();
        await second.DisposeAsync();
    }

    [Fact]
    public async Task ClaimExpiry_AfterLeaseTtl_SecondInstanceTakesOver()
    {
        // Arrange: «умерший» держатель — leased-ключ TTL 2 с, который никто не продлевает.
        var cluster = $"dead{Guid.NewGuid().ToString("N")[..8]}";
        var ct = TestContext.Current.CancellationToken;
        var grant = await Gateway.LeaseGrantAsync(Endpoint, 2, ct);
        grant.IsSuccess.Should().BeTrue();
        var claimTxn = await Gateway.TxnAsync(
            Endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists($"/valkeyworker/claims/{cluster}")],
                [new TxnOp.Put($"/valkeyworker/claims/{cluster}", """{"instance":"dead"}""", grant.Value)]),
            ct);
        claimTxn.Value.Succeeded.Should().BeTrue();

        // Act: lease истекает, etcd сам удаляет ключ; второй инстанс захватывает.
        await Task.Delay(3000, ct);
        var second = NewClaimStore();
        var reclaimed = await second.TryClaimClusterAsync(cluster, ct);

        // Assert: takeover ≤ TTL 15 с (здесь ускорено TTL 2 с).
        reclaimed.IsSuccess.Should().BeTrue();
        reclaimed.Value.Should().BeTrue();
        await second.DisposeAsync();
    }

    [Fact]
    public async Task PortAllocLock_MutualExclusionOnRealEtcd()
    {
        // Arrange: два инстанса PortAllocLock на префиксе /valkeyworker.
        var ct = TestContext.Current.CancellationToken;
        var first = new PortAllocLock("/valkeyworker", [Endpoint], Gateway, TimeProvider.System, "inst-1");
        var second = new PortAllocLock("/valkeyworker", [Endpoint], Gateway, TimeProvider.System, "inst-2");

        // Act: первый захватывает; второй при живом первом — отказ; после release — успех.
        var firstAcquired = await first.TryAcquireAsync(ct);
        var secondAcquired = await second.TryAcquireAsync(ct);
        await first.ReleaseAsync();
        var reclaimed = await second.TryAcquireAsync(ct);
        await second.ReleaseAsync();

        // Assert: взаимоисключение + освобождение (ключ клэйма исчез).
        firstAcquired.Value.Should().BeTrue();
        secondAcquired.Value.Should().BeFalse();
        reclaimed.Value.Should().BeTrue();
        var lockKey = await Gateway.GetAsync(Endpoint, first.Key, ct);
        lockKey.Value.Should().BeNull();
    }
}
