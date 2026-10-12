using FluentAssertions;
using Shared.Etcd.Client;
using S3Worker.IntegrationTests.Etcd;
using Xunit;

namespace S3Worker.IntegrationTests.Etcd;

// Смоук координации /s3worker/ на реальном etcd (порт CoordinationSmokeTests
// valkey + кейс leader-избрания): claim-exclusivity/takeover, portalloc-lock
// взаимоисключение, journal-фазы, api-дискавери, лидерство снапшотов.
// Глубокие протоколы покрыты Shared.Etcd.UnitTests — смоук на префикс /s3worker.
[Collection(S3EtcdCollection.Name)]
public class CoordinationSmokeTests(S3EtcdFixture fixture)
{
    private string Endpoint => fixture.Endpoint;

    private EtcdGateway Gateway => fixture.Gateway;

    private string Prefix => fixture.TestPrefix;

    private ClaimStore NewClaimStore(string? advertiseUrl = null)
        => new(Prefix, [Endpoint], Gateway, TimeProvider.System, advertiseUrl);

    [Fact]
    public async Task TwoClaimStores_ClaimGrantedToExactlyOne()
    {
        // Arrange: два «инстанса» S3Worker; имя кластера — уникальное на прогон
        // (etcd фикстуры общий для классов коллекции).
        var cluster = $"smoke{Guid.NewGuid().ToString("N")[..8]}";
        var first = NewClaimStore();
        var second = NewClaimStore();
        var ct = TestContext.Current.CancellationToken;

        // Act: оба пытаются захватить кластер.
        var firstClaim = await first.TryClaimClusterAsync(cluster, ct);
        var secondClaim = await second.TryClaimClusterAsync(cluster, ct);

        // Assert: exclusivity (S5, arch/17) — клэйм получает ровно один.
        firstClaim.IsSuccess.Should().BeTrue();
        firstClaim.Value.Should().BeTrue();
        secondClaim.IsSuccess.Should().BeTrue();
        secondClaim.Value.Should().BeFalse();

        // Cleanup: release → второй забирает (takeover по release).
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
                [TxnCompare.NotExists($"{Prefix}/claims/{cluster}")],
                [new TxnOp.Put($"{Prefix}/claims/{cluster}", """{"instance":"dead"}""", grant.Value)]),
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
    public async Task TwoClaimStores_ExactlyOneLeader_LeaseExpiryReelects()
    {
        // Arrange: два контекста ClaimStore; лидерство снапшотов — глобальный клэйм.
        var ct = TestContext.Current.CancellationToken;
        var first = NewClaimStore();
        var second = NewClaimStore();

        // Act: оба претендуют на лидерство.
        var firstLeader = await first.TryBecomeLeaderAsync(ct);
        var secondLeader = await second.TryBecomeLeaderAsync(ct);

        // Assert: лидер один.
        firstLeader.Value.Should().BeTrue();
        secondLeader.Value.Should().BeFalse();

        // Lease истекает (TryBecomeLeader берёт TTL 15 с; продления нет —
        // держатель «умер»): ждём фактического удаления ключа etcd → перевод.
        var holderLease = await Gateway.GetAsync(Endpoint, $"{Prefix}/leader", ct);
        holderLease.Value.Should().NotBeNull();
        await Task.Delay(16_000, ct);
        var reelected = await second.TryBecomeLeaderAsync(ct);
        reelected.Value.Should().BeTrue();

        await first.DisposeAsync();
        await second.DisposeAsync();
    }

    [Fact]
    public async Task PortAllocLock_MutualExclusionOnRealEtcd()
    {
        // Arrange: два инстанса PortAllocLock на префиксе /s3worker.
        var ct = TestContext.Current.CancellationToken;
        var first = new PortAllocLock(Prefix, [Endpoint], Gateway, TimeProvider.System, "inst-1");
        var second = new PortAllocLock(Prefix, [Endpoint], Gateway, TimeProvider.System, "inst-2");

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

    [Fact]
    public async Task WorkJournal_WritePhaseOverwrites_KeepsStructure()
    {
        // Arrange: журнал фаз процесса /s3worker/work/<C> (arch/23 §8).
        var cluster = $"jrn{Guid.NewGuid().ToString("N")[..8]}";
        var ct = TestContext.Current.CancellationToken;
        var journal = new WorkJournal(Prefix, Gateway, [Endpoint]);

        // Act: две фазы подряд (S6: перезапись фазы, не затирая структуру).
        var first = await journal.WritePhaseAsync(cluster, "provision", "started", "inst-a", null, ct);
        var second = await journal.WritePhaseAsync(cluster, "provision", "done", "inst-a", null, ct);

        // Assert: обе записи успешны; чтение — последняя фаза, структура цела.
        first.IsSuccess.Should().BeTrue();
        second.IsSuccess.Should().BeTrue();
        var state = await journal.ReadAsync(cluster, ct);
        state.IsSuccess.Should().BeTrue();
        state.Value.Should().NotBeNull();
        state.Value!.Op.Should().Be("provision");
        state.Value.Phase.Should().Be("done");
        state.Value.Instance.Should().Be("inst-a");
        state.Value.UpdatedUnix.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task ApiKey_KeepaliveLoopWritesDiscovery()
    {
        // Arrange: keepalive-контур ClaimStore с advertise-URL (arch/23 §8:
        // /s3worker/api/<id> — дискавери API, ставит сам инстанс).
        var ct = TestContext.Current.CancellationToken;
        var claims = NewClaimStore("https://localhost:8080");

        // Act: запуск keepalive-контура + ожидание записи api-ключа (≤ 5 c).
        await claims.StartAsync(ct);
        var deadline = DateTime.UtcNow.AddSeconds(5);
        IReadOnlyList<Kv> apiKeys = [];
        while (DateTime.UtcNow < deadline)
        {
            var range = await Gateway.RangeAsync(Endpoint, $"{Prefix}/api/", ct);
            if (range.IsSuccess && range.Value.Count > 0)
            {
                apiKeys = range.Value;
                break;
            }

            await Task.Delay(200, ct);
        }

        // Assert: Range /s3worker/api/ непуст — ключ жив (lease продлевается).
        apiKeys.Should().NotBeEmpty();
        await claims.DisposeAsync();
    }
}
