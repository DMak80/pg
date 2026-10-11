using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Storage;
using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.Storage;

// Тесты записи ErasureXlDrive (t40, план Task 8): топология 4/1, temp-корни.
public class ErasureXlDriveWriteTests : IDisposable
{
    private readonly List<string> _roots = [];
    private readonly ErasurePool _pool;
    private readonly ErasureXlDrive _drive;

    public ErasureXlDriveWriteTests()
    {
        for (var i = 0; i < 4; i++)
        {
            var root = Path.Combine(Path.GetTempPath(), "owns3-ecw-" + Guid.NewGuid().ToString("N"));
            _roots.Add(root);
        }

        _pool = new ErasurePool(_roots, new ErasureTopology(4, 4, 1), TimeProvider.System);
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

    private async Task<PutResult> PutAsync(string bucket, string key, byte[] body)
    {
        using var staging = _drive.CreateStaging(bucket, key);
        var digest = await _drive.WritePartAsync(staging, 1, new MemoryStream(body), body.LongLength,
            CancellationToken.None);
        var record = new XlMetaRecord(Guid.Empty, body.LongLength, DateTimeOffset.UnixEpoch,
            digest.ETagHex, "text/plain", new Dictionary<string, string>(), new Dictionary<string, string>(),
            digest.Sha256Hex);
        _drive.WriteMeta(staging, record);
        return _drive.Commit(bucket, key, record, staging);
    }

    private static byte[] RandomBytes(int len, int seed = 3)
    {
        var buf = new byte[len];
        new Random(seed).NextBytes(buf);
        return buf;
    }

    [Fact]
    public async Task Put_CommitAppearsOnAllOnlineDisks_MetaV2WithPerDiskEcIndex()
    {
        // Arrange: пул 4 диска initialized; тело 2.5 МиБ
        var body = RandomBytes((int)(2.5 * 1024 * 1024));

        // Act
        await PutAsync("b", "k", body);

        // Assert: bucket-каталог объекта есть на всех 4 дисках; xl.meta каждого
        // диска — recordVersion 2, Ec.EcIndex == позиции диска; part.1 каждого
        // диска — шардовый поток; полный 2D-набор bitrot-хэшей одинаков во всех копиях
        for (var i = 0; i < 4; i++)
        {
            var objectDir = Path.Combine(_roots[i], "b", "k");
            Directory.Exists(objectDir).Should().BeTrue($"диск {i}");
            var meta = XlMetaFile.Read(objectDir, out _);
            meta.Ec.Should().NotBeNull();
            meta.Ec!.EcIndex.Should().Be((byte)i, "EcIndex = позиция диска в сете");
            meta.Ec.EcM.Should().Be(3);
            meta.Ec.EcN.Should().Be(4);
            meta.Size.Should().Be(body.LongLength);
            meta.Ec.Parts.Should().HaveCount(1);
            meta.Ec.Parts[0].BlockHashes.Should().HaveCount(4, "полный 2D-набор ecN");
            meta.Ec.Parts[0].BlockHashes[0].Should().HaveCount(3, "blockCount = ceil(2.5МиБ/1МиБ)");
            var dataDir = Path.Combine(objectDir, meta.DataDirName);
            File.Exists(Path.Combine(dataDir, "part.1")).Should().BeTrue();
        }

        // 2D-наборы одинаковы во всех копиях (кроме EcIndex)
        var first = XlMetaFile.Read(Path.Combine(_roots[0], "b", "k"), out _).Ec!;
        for (var i = 1; i < 4; i++)
        {
            var other = XlMetaFile.Read(Path.Combine(_roots[i], "b", "k"), out _).Ec!;
            other.Parts[0].BlockHashes.Should().BeEquivalentTo(first.Parts[0].BlockHashes,
                options => options.WithStrictOrdering());
        }
    }

    [Fact]
    public async Task Put_WithOneOfflineDisk_CommitsWithQuorum()
    {
        // Arrange: диск 3 удалён (offline, Task 6-семантика)
        var body = RandomBytes(100_000);
        Directory.Delete(_roots[3], recursive: true);
        _pool.RefreshOnline();
        _drive.RefreshOnline();

        // Act: запись (3 успеха ≥ writeQuorum 3)
        var put = await PutAsync("b", "k2", body);

        // Assert: успех; данные на 3 дисках
        put.ETag.Should().StartWith("\"");
        for (var i = 0; i < 3; i++)
            Directory.Exists(Path.Combine(_roots[i], "b", "k2")).Should().BeTrue();
        Directory.Exists(Path.Combine(_roots[3], "b", "k2")).Should().BeFalse();
    }

    [Fact]
    public async Task Put_OnlineBelowWriteQuorum_ThrowsInsufficientWriteQuorum()
    {
        // Arrange: 2 диска offline (онлайн 2 < k=3)
        var body = RandomBytes(1000);
        Directory.Delete(_roots[2], recursive: true);
        Directory.Delete(_roots[3], recursive: true);
        _pool.RefreshOnline();
        _drive.RefreshOnline();

        // Act
        var act = () => PutAsync("b", "k3", body);

        // Assert: InsufficientWriteQuorum; staging в .trash не остался в tmp
        (await act.Should().ThrowAsync<ObjectStoreException>())
            .Which.Code.Should().Be(ObjectStoreErrorCode.InsufficientWriteQuorum);
        for (var i = 0; i < 2; i++)
            Directory.EnumerateDirectories(Path.Combine(_roots[i], ".owns3.sys", "tmp"))
                .Should().BeEmpty("staging откатан из tmp");
    }

    [Fact]
    public async Task Put_ZeroLength_EmptyShardFiles()
    {
        // Arrange/Act
        await PutAsync("b", "empty", Array.Empty<byte>());

        // Assert: part.1 нулевой длины на всех дисках, blockCount 0
        for (var i = 0; i < 4; i++)
        {
            var meta = XlMetaFile.Read(Path.Combine(_roots[i], "b", "empty"), out _);
            meta.Ec!.Parts[0].SourceSize.Should().Be(0);
            meta.Ec.Parts[0].BlockHashes[0].Should().BeEmpty("blockCount 0");
            var part = Path.Combine(_roots[i], "b", "empty", meta.DataDirName, "part.1");
            new FileInfo(part).Length.Should().Be(0, "шардовые файлы нулевой длины");
        }
    }

    [Fact]
    public async Task Put_TailBlock_PaddedOnlyInMemory()
    {
        // Arrange: тело 1 МиБ + 1 байт: хвостовой блок 1 байт
        var body = RandomBytes(1024 * 1024 + 1);

        // Act
        await PutAsync("b", "tail", body);

        // Assert: размеры шардовых файлов = ceil-раскладка хвоста (1 байт → 1 байт на шарде)
        for (var i = 0; i < 4; i++)
        {
            var meta = XlMetaFile.Read(Path.Combine(_roots[i], "b", "tail"), out _);
            var part = Path.Combine(_roots[i], "b", "tail", meta.DataDirName, "part.1");
            // полный блок: ceil(1МиБ/3) на шарде + хвост: ceil(1/3)=1
            var fullShard = (1024 * 1024 + 2) / 3; // ceil(1048576/3)
            new FileInfo(part).Length.Should().Be(fullShard + 1,
                "добивка хвоста только в памяти кодера");
        }
    }

    [Fact]
    public async Task WritePartAsync_DigestsAreOfOriginalStream()
    {
        // Arrange
        var body = RandomBytes(300_000);
        using var staging = _drive.CreateStaging("b", "digest");

        // Act
        var digest = await _drive.WritePartAsync(staging, 1, new MemoryStream(body), body.LongLength,
            CancellationToken.None);

        // Assert — ETag/SHA — исходного тела
        digest.ETagHex.Should().Be(Convert.ToHexString(System.Security.Cryptography.MD5.HashData(body))
            .ToLowerInvariant());
        digest.Sha256Hex.Should().Be(Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(body))
            .ToLowerInvariant());
        digest.Length.Should().Be(body.LongLength);
    }
}
