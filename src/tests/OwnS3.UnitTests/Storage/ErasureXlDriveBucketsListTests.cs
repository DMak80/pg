using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Storage;
using OwnS3.Storage.Erasure;
using Shared.Core;

namespace OwnS3.UnitTests.Storage;

// Тесты delete/бакетов/листингов ErasureXlDrive (t40, план Task 10).
public class ErasureXlDriveBucketsListTests : IDisposable
{
    private readonly List<string> _roots = [];
    private readonly ErasurePool _pool;
    private readonly ErasureXlDrive _drive;

    public ErasureXlDriveBucketsListTests()
    {
        for (var i = 0; i < 8; i++)
        {
            var root = Path.Combine(Path.GetTempPath(), "owns3-ecbl-" + Guid.NewGuid().ToString("N"));
            _roots.Add(root);
        }

        // 2 сета по 4 (тесты межсетевых кворумов)
        _pool = new ErasurePool(_roots, new ErasureTopology(8, 4, 1), TimeProvider.System);
        _pool.Initialize();
        _drive = new ErasureXlDrive(_pool, defaultParity: 1, TimeProvider.System,
            NullLogger<ErasureXlDrive>.Instance);
    }

    public void Dispose()
    {
        // Teardown: temp-корни удаляются при любом исходе
        _pool.Dispose();
        foreach (var root in _roots)
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
    }

    private async Task PutAsync(string bucket, string key, byte[] body)
    {
        using var staging = _drive.CreateStaging(bucket, key);
        var digest = (await _drive.WritePartAsync(staging, 1, new MemoryStream(body), body.LongLength,
            CancellationToken.None)).Throw().Value;
        var record = new XlMetaRecord(Guid.Empty, body.LongLength, DateTimeOffset.UnixEpoch,
            digest.ETagHex, "text/plain", new Dictionary<string, string>(), new Dictionary<string, string>(),
            digest.Sha256Hex);
        _drive.WriteMeta(staging, record);
        _drive.Commit(bucket, key, record, staging);
    }

    private static byte[] RandomBytes(int len, int seed = 21)
    {
        var buf = new byte[len];
        new Random(seed).NextBytes(buf);
        return buf;
    }

    [Fact]
    public async Task Delete_RemovesOnAllDisks_Quorum()
    {
        // Arrange
        await PutAsync("b", "del", RandomBytes(10_000));
        var meta = _drive.TryReadMeta("b", "del").Throw().Value is XlMetaLookup.Found f ? f.Record
            : throw new InvalidOperationException("запись не найдена после Put");

        // Act
        _drive.Delete("b", "del", meta);

        // Assert: каталога объекта нет ни на одном диске сета
        var setIndex = _pool.PickSetIndex("b", "del");
        foreach (var disk in _pool.Sets[setIndex])
            Directory.Exists(Path.Combine(disk.Root, "b", "del")).Should().BeFalse();
        _drive.TryReadMeta("b", "del").Should().Match<Result<XlMetaLookup>>(
            r => r.IsSuccess && r.Value is XlMetaLookup.Missing);
    }

    [Fact]
    public async Task Delete_BelowWriteQuorum_Throws503()
    {
        // Arrange: 2 диска сета ключа offline (успехов ≤ 2 < writeQuorum 3)
        await PutAsync("b", "delq", RandomBytes(1000));
        var meta = _drive.TryReadMeta("b", "delq").Throw().Value is XlMetaLookup.Found fq ? fq.Record
            : throw new InvalidOperationException("запись не найдена после Put");
        var setIndex = _pool.PickSetIndex("b", "delq");
        var offline = _pool.Sets[setIndex].Take(2).ToList();
        foreach (var disk in offline)
            Directory.Delete(disk.Root, recursive: true);
        _pool.RefreshOnline();

        // Act
        var result = _drive.Delete("b", "delq", meta);

        // Assert — кворумный отказ: Failed с кодом 503-семантики
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.InsufficientWriteQuorum);
    }

    [Fact]
    public async Task CreateBucket_DirsAndMetaOnEveryDiskOfAllSets()
    {
        // Arrange: 2 сета — 8 путей, setSize 4

        // Act
        _drive.CreateBucket("allsets");

        // Assert: каталог бакета + bucket.json на каждом диске обоих сетов
        foreach (var disk in _pool.Disks)
        {
            Directory.Exists(Path.Combine(disk.Root, "allsets")).Should().BeTrue();
            File.Exists(Path.Combine(disk.Root, ".owns3.sys", "buckets", "allsets", "bucket.json"))
                .Should().BeTrue();
        }
    }

    [Fact]
    public async Task CreateBucket_OneSetDegradedBeyondWrite_Throws()
    {
        // Arrange: сет 1 без кворума записи (2 онлайн < writeQuorum 3)
        foreach (var disk in _pool.Sets[1].Take(2))
            Directory.Delete(disk.Root, recursive: true);
        _pool.RefreshOnline();

        // Act
        var result = _drive.CreateBucket("degr");

        // Assert — кворумный отказ: Failed с кодом 503-семантики
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.InsufficientWriteQuorum);
    }

    [Fact]
    public async Task DeleteBucket_NotEmpty_ThrowsBucketNotEmpty()
    {
        // Arrange: объект в ОДНОМ из сетов (SIPMOD ключа)
        _drive.CreateBucket("nb");
        await PutAsync("nb", "obj", RandomBytes(100));

        // Act
        var result = _drive.DeleteBucket("nb");

        // Assert — доменный отказ: Failed с кодом
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.BucketNotEmpty);
    }

    [Fact]
    public async Task ListBuckets_ConsensusFromQuorumSet()
    {
        // Arrange
        _drive.CreateBucket("alpha");
        _drive.CreateBucket("beta");

        // Act
        var buckets = _drive.ListBuckets().Throw().Value;

        // Assert: сортировка Utf8ByteOrder
        buckets.Select(b => b.Name).Should().Equal("alpha", "beta");
        buckets.All(b => b.CreationDate > DateTimeOffset.UnixEpoch).Should().BeTrue();
    }

    [Fact]
    public async Task EnumerateObjects_AllSetsRequired_ThrowsWhenAnySetBelowK()
    {
        // Arrange: сет 1 без read-кворума (2 онлайн < k=3)
        _drive.CreateBucket("l");
        foreach (var disk in _pool.Sets[1].Take(2))
            Directory.Delete(disk.Root, recursive: true);
        _pool.RefreshOnline();

        // Act
        var result = _drive.EnumerateObjects("l");

        // Assert — кворумный отказ: Failed (не бросок) с кодом 503-семантики
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.InsufficientReadQuorum);
    }

    [Fact]
    public async Task EnumerateObjects_UnionAcrossSets_DedupsKeys()
    {
        // Arrange: объекты распределяются по обоим сетам (SIPMOD); листинг — union
        _drive.CreateBucket("u");
        var expected = new List<string>();
        for (var i = 0; i < 40; i++)
        {
            var key = "key-" + i.ToString("D3");
            await PutAsync("u", key, RandomBytes(10, i));
            expected.Add(key);
        }

        expected.Sort((l, r) => Utf8ByteOrder.Compare(l, r));

        // Act
        var cursor = _drive.EnumerateObjects("u").Throw().Value;
        var listed = new List<string>();
        while (cursor.PeekKey is { } key)
        {
            listed.Add(key);
            cursor.MoveNext();
        }

        // Assert — один листинг, Utf8ByteOrder, дедуп по ключу
        listed.Should().Equal(expected);
        listed.Distinct().Should().HaveCount(listed.Count, "ключи не дублируются между сетами");
    }

    [Fact]
    public async Task Put_SetOfKeyFullyOffline_FailedInsufficientReadQuorum()
    {
        // Arrange: multi-set 2×4; бакет создан при обоих сетах онлайн, затем сет 1
        // целиком офлайн (сет 0 жив — бакет-глобальный read-кворум обеспечен)
        _drive.CreateBucket("besc");
        const int deadSet = 1;
        foreach (var disk in _pool.Sets[deadSet])
            Directory.Delete(disk.Root, recursive: true);
        _pool.RefreshOnline();
        // ключ, чей сет (SIPMOD) — мёртвый
        var key = Enumerable.Range(0, 64).Select(i => "esc-" + i)
            .First(k => _pool.PickSetIndex("besc", k) == deadSet);

        // Act (драйв): CreateStaging — ReadDistribution сета без онлайн-дисков
        var act = () => _drive.CreateStaging("besc", key);

        // Assert — кворумный отказ пробрасывается драйвом (не Failed: конвертация
        // на границе XlObjectStore), как и в main до миграции
        act.Should().Throw<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.InsufficientReadQuorum);

        // Act/Assert (стор): граничная конвертация → Failed с кодом (503 через
        // транслятор — как в main), а не 500 InternalError
        var store = new XlObjectStore(_drive, TimeProvider.System,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<XlObjectStore>.Instance);
        var result = await store.PutObjectAsync("besc", key, new MemoryStream(new byte[16]), 16,
            new ObjectUploadMetadata("text/plain", new Dictionary<string, string>()),
            TestContext.Current.CancellationToken);

        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.InsufficientReadQuorum);
    }

    [Fact]
    public async Task EnumerateObjects_BrokenMetaOnOneDisk_EmitsFromHealthyDisk()
    {
        // Arrange
        _drive.CreateBucket("bm");
        await PutAsync("bm", "k", RandomBytes(50));
        var meta = _drive.TryReadMeta("bm", "k").Throw().Value is XlMetaLookup.Found fm ? fm.Record
            : throw new InvalidOperationException("запись не найдена после Put");
        var setIndex = _pool.PickSetIndex("bm", "k");
        var disk = _pool.Sets[setIndex][0];
        var objectDir = Path.Combine(disk.Root, "bm", "k");
        File.WriteAllText(Path.Combine(objectDir, "xl.meta"), "broken");
        File.Delete(Path.Combine(objectDir, "xl.meta.bkp"));

        // Act
        var cursor = _drive.EnumerateObjects("bm").Throw().Value;
        var listed = new List<string>();
        while (cursor.PeekKey is { } key)
        {
            _ = cursor.ReadCurrentMeta();
            listed.Add(key);
            cursor.MoveNext();
        }

        // Assert — ключ эмитится, мета с живого диска
        listed.Should().Equal("k");
    }
}
