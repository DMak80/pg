using PgWorker.Moves;
using Xunit;

namespace PgWorker.IntegrationTests.Etcd;

// Контракт заявок/статуса переездов на реальном etcd (t01 задача 18, spec §10
// AC7): старейшая заявка по requested_unix, конкурентный flip-txn, атомарное
// удаление статус-ключа. Ключи тестов — в своих кластерах /pgworker/moves/.
[Collection(EtcdCollection.Name)]
public class MoveContractTests(EtcdFixture fixture)
{
    private string Endpoint => fixture.Endpoint;

    // AAA: AC7 заявок — старейшая по requested_unix выбирается на реальном etcd
    [Fact]
    public async Task Requests_OldestWins()
    {
        // Arrange — две заявки кластера с разными requested_unix (Д2: одна активная)
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "movescontract";
        await fixture.Gateway.PutAsync(Endpoint, MoveNames.MoveKey(cluster, "bucket_1"),
            """{"op":"move","to":"shard2","requested_unix":20}""", lease: null, ct);
        await fixture.Gateway.PutAsync(Endpoint, MoveNames.MoveKey(cluster, "bucket_2"),
            """{"op":"rollback","requested_unix":10}""", lease: null, ct);
        var store = new MoveRequestsStore(fixture.Gateway, [Endpoint]);

        // Act
        var oldest = await store.OldestAsync(cluster, ct);

        // Assert — выбран минимальный requested_unix (ordering контракта заявок)
        oldest.IsSuccess.Should().BeTrue(oldest.Error?.ToString());
        oldest.Value!.Request!.Value.Bucket.Should().Be("bucket_2");
        oldest.Value.Request.Value.Request.Op.Should().Be(MoveOp.Rollback);

        // Cleanup — ключи теста не переживают прогон (общий etcd коллекции)
        await store.DeleteAsync(cluster, "bucket_1", CancellationToken.None);
        await store.DeleteAsync(cluster, "bucket_2", CancellationToken.None);
    }

    // AAA: AC7 flip — конкурентная txn на реальном etcd: второй flip не проходит
    [Fact]
    public async Task Flip_CompetingTxn_Fails()
    {
        // Arrange — routing бакета на shard1, статус переезда SYNCING
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "flipcompete";
        const string bucket = "bucket_0";
        await fixture.Gateway.PutAsync(Endpoint, MoveNames.RoutingKey(cluster, bucket), "shard1", lease: null, ct);
        var store = new MoveStatusStore(fixture.Gateway, [Endpoint]);
        var put = await store.PutAsync(cluster,
            new MoveStatus(bucket, MoveStates.Syncing, "shard1", "shard2", 1, 2, "copy-wait"), ct);
        put.IsSuccess.Should().BeTrue(put.Error?.ToString());

        // Act — первый flip проходит; конкурентный (тот же устаревший cur) — нет
        var first = await store.FlipAsync(cluster, bucket, "shard1", "shard2", ct: ct);
        var competing = await store.FlipAsync(cluster, bucket, "shard1", "shardX", ct: ct);

        // Assert — чужой flip отклонён txn-compare, значение не перебито
        first.Value.Should().BeTrue("routing соответствовал cur=shard1");
        competing.Value.Should().BeFalse("конкурентный flip с устаревшим cur обязан не сойтись");
        var routing = await fixture.Gateway.GetAsync(Endpoint, MoveNames.RoutingKey(cluster, bucket), ct);
        routing.Value!.Value.Should().Be("shard2");

        // Cleanup
        await fixture.Gateway.DeleteAsync(Endpoint, MoveNames.RoutingKey(cluster, bucket), prefix: false, CancellationToken.None);
    }

    // AAA: успешный flip удаляет статус-ключ той же транзакцией (нет ключа = ACTIVE)
    [Fact]
    public async Task Flip_DropsStatusAtomically()
    {
        // Arrange — routing на shard1 + живой статус-ключ SYNCING
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "flipatomic";
        const string bucket = "bucket_0";
        await fixture.Gateway.PutAsync(Endpoint, MoveNames.RoutingKey(cluster, bucket), "shard1", lease: null, ct);
        var store = new MoveStatusStore(fixture.Gateway, [Endpoint]);
        await store.PutAsync(cluster,
            new MoveStatus(bucket, MoveStates.Frozen, "shard1", "shard2", 1, 2, "flip"), ct);

        // Act
        var flipped = await store.FlipAsync(cluster, bucket, "shard1", "shard2", ct: ct);

        // Assert — той же txn: routing переведён, статус-ключ исчез (ACTIVE)
        flipped.Value.Should().BeTrue(flipped.Error?.ToString());
        var status = await store.GetAsync(cluster, bucket, ct);
        status.Value.Should().BeNull("flip удаляет статус-ключ атомарно с routing");

        // Cleanup
        await fixture.Gateway.DeleteAsync(Endpoint, MoveNames.RoutingKey(cluster, bucket), prefix: false, CancellationToken.None);
    }
}
