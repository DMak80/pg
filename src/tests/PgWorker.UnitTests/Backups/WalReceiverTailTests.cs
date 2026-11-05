using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Etcd.S3;
using PgWorker.PgNode;
using PgWorker.UnitTests.Api;
using Shared.Core;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Резолв стартовой позиции приёмника от хвоста S3 (arch/19 §3): хвост —
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

        public Task<Result> PutObjectMd5Async(string key, byte[] data, string md5Hex, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result<string>> GetObjectAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();

// файловые методы интерфейса в этом тесте не используются —
        // заглушка (фейк нужен только перечисленным выше операциям).
        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default)
            => Task.FromResult(Result.Failed(new ApplicationException("put-file: not supported by this fake")));

        public Task<Result> GetObjectToFileAsync(string cluster, string shard, string key, string destFilePath, CancellationToken ct = default)
            => Task.FromResult(Result.Failed(new ApplicationException("get-to-file: not supported by this fake")));
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

    // AAA (восстановленный шард): restore откатил LSN назад — сегменты прежней
    // ветки выше точки форка остались в wal/, а живой TLI начинается с МЕНЬШЕГО
    // сегмента. Хвост — конец живого (максимального) TLI, не откатанной ветки:
    // иначе стартовая LSN оказывается впереди flush-позиции сервера и
    // START_REPLICATION отвергается вечно (arch/pgnode/03 §3).
    [Fact]
    public void RestoreRolledBackBranch_TailIsNewestTli()
    {
        // Arrange — TLI 1: сегменты 1..15 непрерывны; TLI 3: сегмент 14 (форк ниже)
        var objects = Enumerable.Range(1, 15)
            .Select(i => Seg($"0000000100000000000000{i:x2}"))
            .Append(Seg("00000003000000000000000e"))
            .Append(Seg("00000002.history"))
            .Append(Seg("00000003.history"))
            .ToArray();

        // Act
        var tail = Resolve(objects);

        // Assert — хвост — конец сегмента 14 живой ветки TLI 3, а не откатанного 15
        tail!.Position.Should().Be(15UL << 24);
        tail.SegmentName.Should().Be("00000003000000000000000e");
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

    // ---: WalReceiverTargets — номер жизни в конфигурации сессии (t15, spec §4.2) ----

    private const string Cluster = "c1";
    private const string Shard = "shard1";

    /// <summary>Пул-контекст для резолва targets (одна нода, креды есть).</summary>
    private sealed class FixedPools : IS3PoolStore
    {
        public Task<Result<S3Pool?>> ReadAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<S3Pool?>.Success(new S3Pool(
                name, false,
                [new S3PoolNode("s3a", "RUNNING", "host1", "s3a", 9000)],
                ["s3a:9000"], new S3PoolSecrets("ak", "sk"))));

        public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(["p1"]));
    }

    // Фейк etcd: пароль + policy; wal-life — по наличию ключа.
    private static FakeEtcdGateway TargetsEtcd(string? walLife)
    {
        var etcd = new FakeEtcdGateway();
        etcd.Store[$"/clusters/{Cluster}/backup_password"] = "secret";
        etcd.Store[$"/pgworker/backups/{Cluster}/policy"] =
            """{"s3":{"pool":"p1","write_master":["s3a"],"write_replica":["s3a"]}}""";
        if (walLife is not null)
            etcd.Store[BackupNames.WalLifeKey(Cluster, Shard)] = walLife;
        return etcd;
    }

    private static Task<WalReceiverTargetsResult> ResolveTargetsAsync(FakeEtcdGateway etcd)
        => new WalReceiverTargets(etcd, ["http://etcd-1:2379"], new FixedPools())
            .ResolveAsync(Cluster, Shard, CancellationToken.None);

    // AAA: ключ wal-life есть — номер жизни из него попадает в конфигурацию сессии.
    [Fact]
    public async Task Targets_WalLifeKeyPresent_ChainLifeResolved()
    {
        // Arrange — счётчик жизни 5 (reason restore)
        var etcd = TargetsEtcd("""{"chain_life":5,"bumped_unix":1757500000,"reason":"restore"}""");

        // Act
        var targets = await ResolveTargetsAsync(etcd);

        // Assert — резолв успешен, номер жизни = 5
        targets.WaitingPolicy.Should().BeFalse();
        targets.ChainLife.Should().Be(5u);
    }

    // AAA: ключа wal-life нет — жизнь 0 (первая), сессия не блокируется.
    [Fact]
    public async Task Targets_WalLifeKeyMissing_ChainLifeZero()
    {
        // Arrange — счётчика нет
        var etcd = TargetsEtcd(walLife: null);

        // Act
        var targets = await ResolveTargetsAsync(etcd);

        // Assert
        targets.WaitingPolicy.Should().BeFalse();
        targets.ChainLife.Should().Be(0u);
    }

    // AAA: транспортный отказ чтения счётчика — transient-ожидание (тот же
    // класс, что policy), НЕ молчаливый 0 (неверный номер потерял бы хвост).
    [Fact]
    public async Task Targets_WalLifeReadFailed_TransientWaiting()
    {
        // Arrange — падает только ключ wal-life
        var etcd = TargetsEtcd(walLife: """{"chain_life":5}""");
        etcd.FailGet = key => key.EndsWith("/wal-life", StringComparison.Ordinal);

        // Act
        var targets = await ResolveTargetsAsync(etcd);

        // Assert
        targets.WaitingPolicy.Should().BeTrue();
        targets.Error.Should().Contain("wal-life read failed");
    }
}
