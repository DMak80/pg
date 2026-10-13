using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Storage;
using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.Storage;

// Тесты чтения ErasureXlDrive (t40, план Task 9): консенсус меты, реконструкция,
// bitrot, Range. Топология 4/1, temp-корни.
public class ErasureXlDriveReadTests : IDisposable
{
    private readonly List<string> _roots = [];
    private readonly ErasurePool _pool;
    private readonly ErasureXlDrive _drive;

    public ErasureXlDriveReadTests()
    {
        for (var i = 0; i < 4; i++)
        {
            var root = Path.Combine(Path.GetTempPath(), "owns3-ecr-" + Guid.NewGuid().ToString("N"));
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

    private async Task<XlMetaRecord> PutAsync(string bucket, string key, byte[] body)
    {
        using var staging = _drive.CreateStaging(bucket, key);
        var digest = (await _drive.WritePartAsync(staging, 1, new MemoryStream(body), body.LongLength,
            CancellationToken.None)).Throw().Value;
        var record = new XlMetaRecord(Guid.Empty, body.LongLength, DateTimeOffset.UnixEpoch,
            digest.ETagHex, "text/plain", new Dictionary<string, string>(), new Dictionary<string, string>(),
            digest.Sha256Hex);
        _drive.WriteMeta(staging, record);
        _drive.Commit(bucket, key, record, staging);
        return _drive.TryReadMeta(bucket, key).Throw().Value is XlMetaLookup.Found f
            ? f.Record
            : throw new InvalidOperationException("запись не найдена после Put");
    }

    private static byte[] ReadAll(Stream stream)
    {
        using var target = new MemoryStream();
        stream.CopyTo(target);
        return target.ToArray();
    }

    private static byte[] RandomBytes(int len, int seed = 11)
    {
        var buf = new byte[len];
        new Random(seed).NextBytes(buf);
        return buf;
    }

    [Fact]
    public async Task Get_FullObject_EqualsOriginal()
    {
        // Arrange: 3.5 МиБ (несколько блоков + хвост)
        var body = RandomBytes((int)(3.5 * 1024 * 1024));
        var meta = await PutAsync("b", "full", body);

        // Act
        using var stream = _drive.OpenData("b", "full", meta, 0, meta.Size).Throw().Value;

        // Assert
        ReadAll(stream).Should().Equal(body);
    }

    [Fact]
    public async Task Get_Range_ReturnsSlice()
    {
        // Arrange
        var body = RandomBytes((int)(2.2 * 1024 * 1024));
        var meta = await PutAsync("b", "range", body);

        // Act — срез через границу блока
        var offset = 1024 * 1024 - 100;
        var length = 1024 * 1024 + 200;
        using var stream = _drive.OpenData("b", "range", meta, offset, length).Throw().Value;

        // Assert
        var slice = ReadAll(stream);
        slice.Should().Equal(body[(int)offset..(int)(offset + length)]);
    }

    [Fact]
    public async Task Get_OneDiskOffline_Reconstructs()
    {
        // Arrange
        var body = RandomBytes(700_000);
        var meta = await PutAsync("b", "rec", body);
        Directory.Delete(_roots[1], recursive: true);
        _pool.RefreshOnline();

        // Act
        using var stream = _drive.OpenData("b", "rec", meta, 0, meta.Size).Throw().Value;

        // Assert — реконструкция из k=3 оставшихся
        ReadAll(stream).Should().Equal(body);
    }

    [Fact]
    public async Task Get_BitrotCorruption_ReadsAndFlagsShard()
    {
        // Arrange: порча байта part.1 на диске, держащем data-шард 0 (EcDist записи)
        var body = RandomBytes(500_000);
        var meta = await PutAsync("b", "bitrot", body);
        var dataShardDisk = _pool.Sets[_pool.PickSetIndex("b", "bitrot")]
            .Single(d => d.PositionInSet == meta.Ec!.EcDist[0]);
        var partPath = Path.Combine(dataShardDisk.Root, "b", "bitrot", meta.DataDirName, "part.1");
        var bytes = File.ReadAllBytes(partPath);
        bytes[777] ^= 0xFF;
        File.WriteAllBytes(partPath, bytes);

        // Act
        using var stream = _drive.OpenData("b", "bitrot", meta, 0, meta.Size).Throw().Value;
        var read = ReadAll(stream);

        // Assert — чтение сходится (реконструкция), шард помечен
        read.Should().Equal(body);
        ((ErasureObjectStream)stream).CorruptedShards.Should().NotBeEmpty("битый шард зафиксирован для heal");
    }

    [Fact]
    public void TryReadMeta_ОтсутствующаяЗапись_ТерминальныйSuccessMissing()
    {
        // Act
        var lookup = _drive.TryReadMeta("b", "k");
        // Assert — отсутствие записи — терминальный Success (конвенция 01 §4)
        lookup.IsSuccess.Should().BeTrue();
        lookup.Value.Should().BeOfType<XlMetaLookup.Missing>();
    }

    [Fact]
    public void TryReadMeta_OnlineBelowReadQuorum_FailedInsufficientReadQuorum()
    {
        // Arrange: 2 диска offline (онлайн 2 < k=3)
        Directory.Delete(_roots[0], recursive: true);
        Directory.Delete(_roots[1], recursive: true);
        _pool.RefreshOnline();

        // Act
        var result = _drive.TryReadMeta("b", "any");

        // Assert — кворумный отказ: Failed с кодом 503-семантики
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.InsufficientReadQuorum);
    }

    [Fact]
    public async Task VerifyFullChecksum_Match_Passes()
    {
        // Arrange
        var body = RandomBytes(120_000);
        var meta = await PutAsync("b", "chk", body);

        // Act/Assert — сверка полного прохода (реконструкция не меняет исходный поток)
        var act = () => _drive.VerifyFullChecksum("b", "chk", meta);
        act.Should().NotThrow();
    }

    [Fact]
    public async Task VerifyFullChecksum_Mismatch_Throws()
    {
        // Arrange: порча записи — ContentSha256 не совпадает с хранимым потоком:
        // два битых шарда (data+parity) — восстановить нельзя, но сверка падает
        // сразу на несовпадении bitrot... вместо этого ломаем саму мету
        var body = RandomBytes(120_000);
        var meta = await PutAsync("b", "bad", body);
        var broken = meta with { ContentSha256 = new string('0', 64) };

        // Act
        var act = () => _drive.VerifyFullChecksum("b", "bad", broken);

        // Assert
        act.Should().Throw<XlIntegrityException>();
    }

    [Fact]
    public async Task EnumerateParts_ReturnsSourceSizes()
    {
        // Arrange: многочастевой объект через EC multipart (Task 11)
        const int partSize = 6 * 1024 * 1024;
        var part1 = RandomBytes(partSize, 41);
        var part2 = RandomBytes(partSize / 2, 42);
        var uploadId = Guid.NewGuid().ToString("N");
        _drive.CreateUploadDir("b", "parts", uploadId, new XlMetaRecord(Guid.Parse(uploadId), 0,
            DateTimeOffset.UnixEpoch, "", "text/plain", new Dictionary<string, string>(),
            new Dictionary<string, string>(), ""));
        _drive.WriteUploads("b", "parts",
        [
            new MultipartJournals.UploadJournalEntry(uploadId, "b", "parts",
                DateTimeOffset.UnixEpoch.ToUnixTimeMilliseconds(), "root"),
        ]);
        foreach (var (number, body) in new[] { (1, part1), (2, part2) })
        {
            using var staging = _drive.CreateUploadStaging("b", "parts", uploadId);
            var digest = (await _drive.WritePartAsync(staging, number, new MemoryStream(body), body.LongLength,
                CancellationToken.None)).Throw().Value;
            _drive.CommitUploadPart(staging, number, digest.ETagHex, digest.Length, DateTimeOffset.UnixEpoch);
        }

        var etag1 = Convert.ToHexString(System.Security.Cryptography.MD5.HashData(part1)).ToLowerInvariant();
        var etag2 = Convert.ToHexString(System.Security.Cryptography.MD5.HashData(part2)).ToLowerInvariant();
        _drive.CommitMultipart("b", "parts", uploadId,
        [
            new PartEtag(1, '"' + etag1 + '"'),
            new PartEtag(2, '"' + etag2 + '"'),
        ], new XlMetaRecord(Guid.Empty, 0, DateTimeOffset.UnixEpoch, "", "text/plain",
            new Dictionary<string, string>(), new Dictionary<string, string>(), ""));
        var meta = _drive.TryReadMeta("b", "parts").Throw().Value is XlMetaLookup.Found fp ? fp.Record
            : throw new InvalidOperationException("запись не найдена после Complete");

        // Act
        var parts = _drive.EnumerateParts("b", "parts", meta).Throw().Value;

        // Assert — исходные размеры частей (GetObjectAttributes без декодирования)
        parts.Should().Equal((1, (long)partSize), (2, (long)(partSize / 2)));
    }
}
