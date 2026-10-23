using FluentAssertions;
using PgWorker.Backups;
using PgWorker.WalReceiver;
using Shared.Core;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Резолв стартовой позиции приёмника от хвоста S3 (t27, arch/19 §3): хвост —
// конец последнего сегмента непрерывной части цепочки (WalChain), дыра — старт
// от конца непрерывной части (повторная доставка выше дыры идемпотентна), пустой
// префикс — null (старт от restart_lsn слота).
public class WalReceiverTailTests
{
    /// <summary>Мини-фейк IBackupS3: ListWalAsync от фиксированного списка,
    /// остальные операции не нужны резолверу.</summary>
    private sealed class FakeS3(IReadOnlyList<WalObject> objects) : IBackupS3
    {
        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(Result<IReadOnlyList<WalObject>>.Success(objects));

        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct)
            => throw new NotSupportedException();

        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
            string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(
            string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result<string>> GetObjectAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result<string>> DownloadTextAsync(
            string cluster, string shard, string objectKey, CancellationToken ct = default)
            => throw new NotSupportedException();
    }

    private static WalObject Seg(string name) => new(name, DateTimeOffset.UnixEpoch);

    private static WalTail? Resolve(params WalObject[] objects)
    {
        // Arrange / Act
        var result = S3TailResolver.ResolveTailAsync(new FakeS3(objects), "c1", "shard1", CancellationToken.None)
            .GetAwaiter().GetResult();

        // Assert-контракт: успех без исключения
        Assert.True(result.IsSuccess, result.Error?.Message ?? "ok");
        return result.Value;
    }

    [Fact]
    public void ContinuousChain_TailIsLastEnd()
    {
        // Arrange
        var tail = Resolve(
            Seg("000000010000000000000001"),
            Seg("000000010000000000000002"),
            Seg("000000010000000000000003"),
            Seg("000000010000000000000004"),
            Seg("000000010000000000000005"));

        // Act / Assert — конец сегмента 5: (5+1)<<24; имя хвоста — последний сегмент
        tail!.Position.Should().Be(6UL << 24);
        tail.SegmentName.Should().Be("000000010000000000000005");
    }

    [Fact]
    public void Hole_TailIsEndOfContinuousPart()
    {
        // Arrange — сегменты 1, 2, 4 (сегмент 3 отсутствует)
        var tail = Resolve(
            Seg("000000010000000000000001"),
            Seg("000000010000000000000002"),
            Seg("000000010000000000000004"));

        // Act / Assert — конец непрерывной части (сегмент 2), старт выше дыры
        tail!.Position.Should().Be(3UL << 24);
    }

    [Fact]
    public void EmptyPrefix_Null()
    {
        // Arrange / Act / Assert
        Resolve().Should().BeNull();
    }

    [Fact]
    public void TliSwitch_WithHistory_ChainContinuous()
    {
        // Arrange — последний сегмент TLI 1 (seg 1), history TLI 2,
        // первый сегмент TLI 2 (seg 2 == Next(1))
        var tail = Resolve(
            Seg("000000010000000000000001"),
            Seg("000000020000000000000002"),
            Seg("00000002.history"));

        // Act / Assert — цепочка непрерывна, хвост — конец сегмента TLI 2
        tail!.Position.Should().Be(3UL << 24);
    }

    [Fact]
    public void EndLsn_Consistent_WithWalFileName()
    {
        // Arrange
        var parsed = WalFileName.TryParse("000000010000000000000002")!.Value;

        // Act
        var endLsn = S3TailResolver.EndLsn(parsed);

        // Assert — конец сегмента 2 → обратная FromLsn-математика даёт сегмент 3
        endLsn.Should().Be(3UL << 24);
        WalFileName.FromLsn(1, $"{endLsn >> 32:X}/{endLsn & 0xFFFFFFFF:X}").Name
            .Should().Be("000000010000000000000003");
    }
}
