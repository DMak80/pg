using FluentAssertions;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using Xunit;

namespace KafkaWorker.IntegrationTests.Etcd;

// Координация /kafkaworker/ на реальном etcd (арх-план A14; порт
// EtcdCoordinationTests PgWorker под kafka-префикс): exclusivity клэймов
// и takeover по истечении lease TTL.
[Collection(Kafka.KafkaCollection.Name)]
public class ClaimStoreTests(Kafka.KafkaClusterFixture fixture)
{
    private string Endpoint => fixture.Endpoint;

    private EtcdGateway Gateway => fixture.Gateway;

    private ClaimStore NewClaimStore() => new([Endpoint], Gateway, TimeProvider.System);

    [Fact]
    public async Task TwoClaimStores_MutualExclusion()
    {
        // Arrange: два «инстанса» KafkaWorker.
        var first = NewClaimStore();
        var second = NewClaimStore();
        var ct = TestContext.Current.CancellationToken;

        // Act: оба пытаются захватить кластер.
        var firstClaim = await first.TryClaimClusterAsync("events", ct);
        var secondClaim = await second.TryClaimClusterAsync("events", ct);

        // Assert: exclusivity — кластер обрабатывает один инстанс (arch/16 §6).
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
        // Arrange: «умерший» держатель — leased-ключ TTL 2 с, который никто не продлевает.
        var ct = TestContext.Current.CancellationToken;
        var grant = await Gateway.LeaseGrantAsync(Endpoint, 2, ct);
        grant.IsSuccess.Should().BeTrue();
        var claimTxn = await Gateway.TxnAsync(
            Endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists("/kafkaworker/claims/events")],
                [new TxnOp.Put("/kafkaworker/claims/events", """{"instance":"dead"}""", grant.Value)]),
            ct);
        claimTxn.Value.Succeeded.Should().BeTrue();

        // Act: lease истекает, etcd сам удаляет ключ; второй инстанс захватывает.
        await Task.Delay(3000, ct);
        var second = NewClaimStore();
        var reclaimed = await second.TryClaimClusterAsync("events", ct);

        // Assert: takeover ≤ TTL 15 с (spec §9.7) — здесь ускорено TTL 2 с.
        reclaimed.IsSuccess.Should().BeTrue();
        reclaimed.Value.Should().BeTrue();
        await second.DisposeAsync();
    }
}
