using PgWorker.Moves;
using PgWorker.UnitTests.Provisioning;
using FakeEtcd = PgWorker.UnitTests.Provisioning.Fakes.FakeEtcd;

namespace PgWorker.UnitTests.Moves;

public class MoveStatusStoreTests
{
    // AAA: put/get round-trip статус-ключа
    [Fact]
    public async Task Get_AfterPut_ReturnsStatus()
    {
        // Arrange
        var etcd = new FakeEtcd();
        var store = new MoveStatusStore(etcd, ["http://x"]);
        var put = new MoveStatus("bucket_42", MoveStates.Syncing, "shard1", "shard2", 1, 2, "ddl");

        // Act
        await store.PutAsync("shop", put, CancellationToken.None);
        var got = await store.GetAsync("shop", "bucket_42", CancellationToken.None);

        // Assert
        got.Value!.State.Should().Be(MoveStates.Syncing);
        got.Value.Phase.Should().Be("ddl");
    }

    // AAA: нет ключа = ACTIVE (null)
    [Fact]
    public async Task Get_MissingKey_ReturnsNull()
    {
        // Arrange
        var store = new MoveStatusStore(new FakeEtcd(), ["http://x"]);

        // Act
        var got = await store.GetAsync("shop", "bucket_42", CancellationToken.None);

        // Assert
        got.Value.Should().BeNull("нет статус-ключа = бакет ACTIVE");
    }

    // AAA: flip — атомарная txn: routing → новый + delete status (скрипт etcd_flip)
    [Fact]
    public async Task FlipAsync_ReplacesRoutingAndDropsStatus()
    {
        // Arrange
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), "shard1");
        var store = new MoveStatusStore(etcd, ["http://x"]);
        await store.PutAsync("shop", new MoveStatus("bucket_42", MoveStates.Frozen, "shard1", "shard2", 1, 2, "flip"), CancellationToken.None);

        // Act
        var flipped = await store.FlipAsync("shop", "bucket_42", "shard1", "shard2", ct: CancellationToken.None);

        // Assert
        flipped.Value.Should().BeTrue("routing соответствовал cur");
        etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard2");
        etcd.Store.ContainsKey(MoveNames.StatusKey("shop", "bucket_42")).Should().BeFalse("статус-ключ удалён той же txn");
    }

    // AAA: flip с пост-flip статусом (rollback-доведение, ревью №1) — атомарная
    //      txn кладёт фазу доведения ВМЕСТО удаления статус-ключа: маркер «flip был»
    //      не теряется даже при сбое etcd между flip и записью фазы
    [Fact]
    public async Task FlipAsync_WithPostFlipStatus_PutsPhaseInsteadOfDelete()
    {
        // Arrange
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), "shard1");
        var store = new MoveStatusStore(etcd, ["http://x"]);
        await store.PutAsync("shop", new MoveStatus("bucket_42", MoveStates.Frozen, "shard2", "shard1", 1, 2, "flip"), CancellationToken.None);
        var postFlip = new MoveStatus("bucket_42", MoveStates.Frozen, "shard1", "shard2", 1, 3, MovePhases.RollbackPostFlip);

        // Act
        var flipped = await store.FlipAsync("shop", "bucket_42", "shard1", "shard2", postFlip, CancellationToken.None);

        // Assert
        flipped.Value.Should().BeTrue("routing соответствовал cur");
        etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard2",
            "routing перевёрнут той же txn");
        etcd.Store[MoveNames.StatusKey("shop", "bucket_42")].Value.Should().Be(postFlip.Serialize(),
            "фаза доведения положена атомарно с flip — не отдельным put");
    }

    // AAA: конкурентный flip (routing изменился под руками) — Succeeded=false, всё нетронуто
    [Fact]
    public async Task FlipAsync_CompetingChange_FailsCleanly()
    {
        // Arrange
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), "shard9"); // конкурент уже перевёл
        var store = new MoveStatusStore(etcd, ["http://x"]);

        // Act
        var flipped = await store.FlipAsync("shop", "bucket_42", "shard1", "shard2", ct: CancellationToken.None);

        // Assert
        flipped.Value.Should().BeFalse("compare по routing=cur обязан не сойтись");
        etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard9");
    }
}
