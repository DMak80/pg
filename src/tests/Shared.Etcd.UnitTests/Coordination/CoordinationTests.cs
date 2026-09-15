using Shared.Etcd.Coordination;
using Xunit;

namespace Shared.Etcd.UnitTests.Coordination;

// ClaimStore (клэймы/лидерство) — t09: перенос Pg-копии с параметризацией
// префикса (ключи /unit/… вместо /pgworker/…).
public class CoordinationTests
{
    private const string Prefix = "/unit";

    private static ClaimStore NewStore(FakeCoordinationGateway gateway, string? advertiseApiUrl = null)
        => new(Prefix, ["http://etcd:2379"], gateway, TimeProvider.System, advertiseApiUrl);

    [Fact]
    public async Task ClaimStore_TryClaimCluster_TxnCompareVersionZero_PutsLeasedKey()
    {
        // Arrange
        var gateway = new FakeCoordinationGateway();
        var store = NewStore(gateway);

        // Act
        var result = await store.TryClaimClusterAsync("shop", CancellationToken.None);

        // Assert: txn с compare version==0 на /unit/claims/<C>, ключ записан с instance
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeTrue();
        var txn = gateway.Txns.Should().ContainSingle().Subject;
        var compare = txn.Compare.Should().ContainSingle().Subject;
        compare.Key.Should().Be($"{Prefix}/claims/shop");
        compare.Target.Should().Be(TxnTarget.Version);
        compare.Num.Should().Be(0);
        var put = txn.Success.Should().ContainSingle().Subject.Should().BeAssignableTo<TxnOp.Put>().Which;
        put.Lease.Should().NotBeNull();
        gateway.Store.Should().ContainKey($"{Prefix}/claims/shop");
        gateway.Store[$"{Prefix}/claims/shop"].Should().Contain(store.InstanceId);
        store.IsMine("shop").Should().BeTrue();
    }

    [Fact]
    public async Task ClaimStore_TryClaimCluster_AlreadyClaimedByOther_ReturnsFalse()
    {
        // Arrange — чужой клэйм уже в etcd
        var gateway = new FakeCoordinationGateway
        {
            Store = new Dictionary<string, string> { [$"{Prefix}/claims/shop"] = """{"instance":"other"}""" },
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
        var gateway = new FakeCoordinationGateway();
        var store = NewStore(gateway);

        // Act
        var result = await store.TryBecomeLeaderAsync(CancellationToken.None);

        // Assert
        result.Value.Should().BeTrue();
        store.IsLeader.Should().BeTrue();
        gateway.Store.Should().ContainKey($"{Prefix}/leader");
    }

    [Fact]
    public async Task ClaimStore_KeepaliveTick_ExtendsAllLiveLeases()
    {
        // Arrange — 2 клэйма + лидерство
        var gateway = new FakeCoordinationGateway();
        var store = NewStore(gateway);
        await store.TryClaimClusterAsync("shop", CancellationToken.None);
        await store.TryClaimClusterAsync("billing", CancellationToken.None);
        await store.TryBecomeLeaderAsync(CancellationToken.None);
        gateway.KeepaliveCalls.Clear();

        // Act
        await store.KeepaliveTickAsync(CancellationToken.None);

        // Assert: продлены все четыре lease — 2 клэйма + лидерство + instance-ключ
        // (тик не только продлевает, но и (пере)ставит instance-ключ — ревью Ф7)
        gateway.KeepaliveCalls.Should().HaveCount(4);
    }

    [Fact]
    public async Task ClaimStore_ReleaseCluster_DeletesKeyAndRevokesLease()
    {
        // Arrange
        var gateway = new FakeCoordinationGateway();
        var store = NewStore(gateway);
        await store.TryClaimClusterAsync("shop", CancellationToken.None);

        // Act
        await store.ReleaseClusterAsync("shop", CancellationToken.None);

        // Assert: ключ удалён, lease отозван, IsMine=false
        gateway.Store.Should().NotContainKey($"{Prefix}/claims/shop");
        gateway.LiveLeases.Should().BeEmpty();
        store.IsMine("shop").Should().BeFalse();
    }

    [Fact]
    public async Task ClaimStore_KeepaliveFailure_ClaimIsLost_AndCanBeReclaimed()
    {
        // Arrange — keepalive отвечает ошибкой (lease истёк на стороне etcd; etcd сам
        // удалил ключи под истёкшим lease — имитируем это в fake)
        var gateway = new FakeCoordinationGateway();
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

    // Ревью Ф7 [impl, major]: etcd недоступен в момент первого тика — grant падает,
    // ключи instances/<id> и api/<id> не ставятся; после возвращения etcd следующий
    // тик пере-ставит их сам (без фикса — только рестартом процесса).
    [Fact]
    public async Task ClaimStore_EtcdDownAtStart_InstanceAndApiKeysPlacedOnNextTick()
    {
        // Arrange — grant отвечает ошибкой («etcd недоступен»)
        var gateway = new FakeCoordinationGateway
        {
            GrantOverride = () => Result<long>.Failed(new ApplicationException("etcd недоступен")),
        };
        var store = NewStore(gateway, advertiseApiUrl: "http://worker:8080");

        // Act — первый тик: постановка не удалась, ключей нет
        await store.KeepaliveTickAsync(CancellationToken.None);
        gateway.Store.Should().NotContainKey($"{Prefix}/instances/{store.InstanceId}");
        gateway.Store.Should().NotContainKey($"{Prefix}/api/{store.InstanceId}");

        // etcd вернулся — следующий тик ставит оба ключа сам
        gateway.GrantOverride = null;
        await store.KeepaliveTickAsync(CancellationToken.None);

        // Assert
        gateway.Store.Should().ContainKey($"{Prefix}/instances/{store.InstanceId}");
        gateway.Store.Should().ContainKey($"{Prefix}/api/{store.InstanceId}");
        await store.DisposeAsync();
    }

    // Ревью Ф7 [impl, major]: потеря instance-lease в рантайме (etcd недоступен
    // дольше TTL) — тик фиксирует потерю; следующий тик пере-ставит ключи с новым
    // lease и свежим since_unix (парсер панели важен факт наличия ключа).
    [Fact]
    public async Task ClaimStore_InstanceLeaseLostOnKeepalive_ApiKeyRestoredOnNextTick()
    {
        // Arrange — ключи поставлены первым тиком
        var gateway = new FakeCoordinationGateway();
        var store = NewStore(gateway, advertiseApiUrl: "http://worker:8080");
        await store.KeepaliveTickAsync(CancellationToken.None);
        var firstLease = gateway.KeyLeases[$"{Prefix}/instances/{store.InstanceId}"];

        // Act — keepalive падает: etcd удалил ключи истёкшего lease (имитация в fake)
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
        await store.KeepaliveTickAsync(CancellationToken.None); // тик фиксирует потерю

        // etcd снова доступен — следующий тик восстанавливает ключи
        gateway.KeepaliveOverride = null;
        await store.KeepaliveTickAsync(CancellationToken.None);

        // Assert — оба ключа на месте под свежим lease
        gateway.Store.Should().ContainKey($"{Prefix}/instances/{store.InstanceId}");
        gateway.Store.Should().ContainKey($"{Prefix}/api/{store.InstanceId}");
        var secondLease = gateway.KeyLeases[$"{Prefix}/instances/{store.InstanceId}"];
        secondLease.Should().BeGreaterThan(firstLease);
        await store.DisposeAsync();
    }

    [Fact]
    public async Task ClaimStore_DisposeAsync_RevokesAllLeases()
    {
        // Arrange
        var gateway = new FakeCoordinationGateway();
        var store = NewStore(gateway);
        await store.TryClaimClusterAsync("shop", CancellationToken.None);
        await store.TryBecomeLeaderAsync(CancellationToken.None);

        // Act
        await store.DisposeAsync();

        // Assert: все lease отозваны, ключи под ними исчезли
        gateway.LiveLeases.Should().BeEmpty();
        gateway.Store.Should().NotContainKey($"{Prefix}/claims/shop");
        gateway.Store.Should().NotContainKey($"{Prefix}/leader");
    }
}
