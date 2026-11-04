using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Job;
using PgWorker.UnitTests.Api;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Счётчик жизни цепочки wal-life (arch/backups/05 §1, spec §4.6): чтение
// отсутствующего/битого ключа, идемпотентность инкремента (маркер времени и
// reason-гейт foreign-chain), конкурентный txn с повторной итерацией, детект
// свежего факта «чужая цепочка».
public class WalLifeStoreTests
{
    private const string Cluster = "c1";
    private const string Shard = "shard1";

    private static string Key => BackupNames.WalLifeKey(Cluster, Shard);

    private static WalLifeStore Store(FakeEtcdGateway etcd)
        => new(etcd, ["http://etcd-1:2379"]);

    // AAA: ключа нет — ReadAsync null (жизнь 0), ReadValueAsync 0.
    [Fact]
    public async Task Read_MissingKey_LifeZero()
    {
        // Arrange
        var etcd = new FakeEtcdGateway();
        var store = Store(etcd);

        // Act
        var read = await store.ReadAsync(Cluster, Shard, CancellationToken.None);
        var value = await store.ReadValueAsync(Cluster, Shard, CancellationToken.None);

        // Assert
        read.IsSuccess.Should().BeTrue();
        read.Value.Should().BeNull();
        value.Value.Should().Be(0u);
    }

    // AAA: bump Restore при жизни 2 — 3, reason restore, время записано.
    [Fact]
    public async Task Bump_Restore_IncrementsAndWritesReason()
    {
        // Arrange — текущее значение: жизнь 2, bump 50, причина shard-rebuilt
        var etcd = new FakeEtcdGateway();
        etcd.Store[Key] = """{"chain_life":2,"bumped_unix":50,"reason":"shard-rebuilt"}""";
        var store = Store(etcd);

        // Act
        var bumped = await store.BumpAsync(
            Cluster, Shard, WalLifeReason.Restore, startedUnix: 100, CancellationToken.None);
        var value = await store.ReadValueAsync(Cluster, Shard, CancellationToken.None);

        // Assert — жизнь 3, причина restore, bumped_unix свежий
        bumped.Value.Should().Be(3u);
        value.Value.Should().Be(3u);
        WalLifeStateJson.TryParse(etcd.Store[Key])!.ChainLife.Should().Be(3u);
        WalLifeStateJson.TryParse(etcd.Store[Key])!.Reason.Should().Be(WalLifeReason.Restore);
        WalLifeStateJson.TryParse(etcd.Store[Key])!.BumpedUnix.Should().BeGreaterThan(100);
    }

    // AAA: повторный bump той же заявки (bumped_unix ≥ её старта) не инкрементирует.
    [Fact]
    public async Task Bump_RepeatSameRequest_Idempotent()
    {
        // Arrange — bump заявки уже сделан
        var etcd = new FakeEtcdGateway();
        var store = Store(etcd);
        var first = await store.BumpAsync(
            Cluster, Shard, WalLifeReason.Restore, startedUnix: 100, CancellationToken.None);

        // Act — повторный тик той же заявки
        var second = await store.BumpAsync(
            Cluster, Shard, WalLifeReason.Restore, startedUnix: 100, CancellationToken.None);

        // Assert — без инкремента
        first.Value.Should().Be(1u);
        second.Value.Should().Be(1u);
        (await store.ReadValueAsync(Cluster, Shard, CancellationToken.None)).Value.Should().Be(1u);
    }

    // AAA: reason-гейт foreign-chain — повторный детект не считает; после
    // restore-bump (другая причина) новый детект инкрементирует.
    [Fact]
    public async Task Bump_ForeignChain_ReasonGate()
    {
        // Arrange — первый детект чужой цепочки
        var etcd = new FakeEtcdGateway();
        var store = Store(etcd);
        var first = await store.BumpAsync(
            Cluster, Shard, WalLifeReason.ForeignChain, startedUnix: null, CancellationToken.None);

        // Act — повторный детект той же лежащей цепочки
        var repeat = await store.BumpAsync(
            Cluster, Shard, WalLifeReason.ForeignChain, startedUnix: null, CancellationToken.None);

        // Act — restore-заявка сменила причину, затем снова детект
        var restore = await store.BumpAsync(
            Cluster, Shard, WalLifeReason.Restore, startedUnix: 200, CancellationToken.None);
        var afterRestore = await store.BumpAsync(
            Cluster, Shard, WalLifeReason.ForeignChain, startedUnix: null, CancellationToken.None);

        // Assert
        first.Value.Should().Be(1u);
        repeat.Value.Should().Be(1u);
        restore.Value.Should().Be(2u);
        afterRestore.Value.Should().Be(3u);
    }

    // AAA: конкурентный txn — сторонний put между чтением и bump'ом: compare
    // ModRevision не сходится, вторая итерация инкрементирует от свежего значения.
    [Fact]
    public async Task Bump_ConcurrentModification_RetriesFromFreshValue()
    {
        // Arrange — ключ жизни 1; после первого чтения сторонний писатель ставит 5
        var etcd = new FakeEtcdGateway();
        etcd.Store[Key] = """{"chain_life":1,"bumped_unix":10,"reason":"restore"}""";
        etcd.OnAfterGet = () => etcd.Store[Key] =
            """{"chain_life":5,"bumped_unix":20,"reason":"foreign-chain"}""";
        var store = Store(etcd);

        // Act — bump shard-rebuilt (другая причина — гейты не мешают)
        var bumped = await store.BumpAsync(
            Cluster, Shard, WalLifeReason.ShardRebuilt, startedUnix: 30, CancellationToken.None);

        // Assert — инкремент от свежего значения (5 → 6), одна повторная итерация
        bumped.IsSuccess.Should().BeTrue();
        bumped.Value.Should().Be(6u);
        WalLifeStateJson.TryParse(etcd.Store[Key])!.Reason.Should().Be(WalLifeReason.ShardRebuilt);
    }

    // AAA (spec §4.5): свежий факт «чужая цепочка» — failed_permanent с причиной;
    // прочие состояния/ошибки фактом не считаются.
    [Fact]
    public void IsForeignChainFact_DetectsOnlyForeignChainPermanent()
    {
        // Arrange
        var positive = new WalReceiverAgentFacts(
            "n1", "master", "failed_permanent", false, null,
            "wal-archive-conflict: foreign WAL chain detected");
        var otherPermanent = new WalReceiverAgentFacts(
            "n1", "master", "failed_permanent", false, null, "segment size mismatch");
        var streaming = new WalReceiverAgentFacts(
            "n1", "master", "streaming", false, null, "foreign WAL chain");

        // Act / Assert
        WalLifeStore.IsForeignChainFact(positive).Should().BeTrue();
        WalLifeStore.IsForeignChainFact(otherPermanent).Should().BeFalse();
        WalLifeStore.IsForeignChainFact(streaming).Should().BeFalse();
    }
}
