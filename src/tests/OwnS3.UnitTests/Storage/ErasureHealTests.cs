using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Storage;
using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.Storage;

// Тесты heal-прохода и статус-провайдера EC (t40, план Task 13).
public class ErasureHealTests : IDisposable
{
    private readonly List<string> _roots = [];
    private readonly ErasurePool _pool;
    private readonly ErasureXlDrive _drive;
    private readonly ErasureHealService _heal;
    private readonly ErasureStatusProvider _status;

    public ErasureHealTests()
    {
        for (var i = 0; i < 4; i++)
        {
            var root = Path.Combine(Path.GetTempPath(), "owns3-ech-" + Guid.NewGuid().ToString("N"));
            _roots.Add(root);
        }

        _pool = new ErasurePool(_roots, new ErasureTopology(4, 4, 1), TimeProvider.System);
        _pool.Initialize();
        _drive = new ErasureXlDrive(_pool, defaultParity: 1, TimeProvider.System,
            NullLogger<ErasureXlDrive>.Instance);
        _drive.CreateBucket("b");
        _status = new ErasureStatusProvider(_pool);
        _heal = new ErasureHealService(_pool, _drive, _status, TimeProvider.System,
            NullLogger<ErasureHealService>.Instance);
    }

    public void Dispose()
    {
        // Teardown: temp-корни удаляются при любом исходе
        _pool.Dispose();
        foreach (var root in _roots)
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
    }

    private async Task<XlMetaRecord> PutAsync(string key, int len, int seed = 3)
    {
        var body = new byte[len];
        new Random(seed).NextBytes(body);
        using var staging = _drive.CreateStaging("b", key);
        var digest = await _drive.WritePartAsync(staging, 1, new MemoryStream(body), body.LongLength,
            CancellationToken.None);
        var record = new XlMetaRecord(Guid.Empty, body.LongLength, DateTimeOffset.UnixEpoch,
            digest.ETagHex, "text/plain", new Dictionary<string, string>(), new Dictionary<string, string>(),
            digest.Sha256Hex);
        _drive.WriteMeta(staging, record);
        _drive.Commit("b", key, record, staging);
        return _drive.TryReadMeta("b", key)!;
    }

    private static byte[] ReadAll(Stream stream)
    {
        using var target = new MemoryStream();
        stream.CopyTo(target);
        return target.ToArray();
    }

    [Fact]
    public async Task Heal_MissingShardOnOneDisk_Restored()
    {
        // Arrange: удалён part-файл одного диска
        var body = new byte[700_000];
        new Random(5).NextBytes(body);
        var meta = await PutAsync("missing", body.Length, 5);
        var setIndex = _pool.PickSetIndex("b", "missing");
        var disk = _pool.Sets[setIndex][1];
        var partPath = Path.Combine(disk.Root, "b", "missing", meta.DataDirName, "part.1");
        File.Delete(partPath);

        // Act
        await _heal.RunPassAsync(CancellationToken.None);

        // Assert: файл восстановлен, содержимое идентично (чтение без реконструкции)
        File.Exists(partPath).Should().BeTrue("шард восстановлен heal");
        using var stream = _drive.OpenData("b", "missing", meta, 0, meta.Size);
        ReadAll(stream).Should().Equal(body);
    }

    [Fact]
    public async Task Heal_BitrotShard_Rewritten()
    {
        // Arrange: порча байта в шард-файле
        var body = new byte[600_000];
        new Random(6).NextBytes(body);
        var meta = await PutAsync("bitrot", body.Length, 6);
        var setIndex = _pool.PickSetIndex("b", "bitrot");
        var disk = _pool.Sets[setIndex][2];
        var partPath = Path.Combine(disk.Root, "b", "bitrot", meta.DataDirName, "part.1");
        var bytes = File.ReadAllBytes(partPath);
        bytes[42] ^= 0xFF;
        File.WriteAllBytes(partPath, bytes);

        // Act
        await _heal.RunPassAsync(CancellationToken.None);

        // Assert: хэш совпал (файл перезаписан реконструированным)
        var restored = File.ReadAllBytes(partPath);
        restored[42].Should().Be((byte)(bytes[42] ^ 0xFF), "битый байт перезаписан исходным значением");
        using var stream = _drive.OpenData("b", "bitrot", meta, 0, meta.Size);
        ReadAll(stream).Should().Equal(body);
    }

    [Fact]
    public async Task Heal_BrokenMeta_RestoredFromConsensus()
    {
        // Arrange: обрыв xl.meta на одном диске
        var meta = await PutAsync("bmeta", 1000, 7);
        var setIndex = _pool.PickSetIndex("b", "bmeta");
        var disk = _pool.Sets[setIndex][0];
        var objectDir = Path.Combine(disk.Root, "b", "bmeta");
        File.WriteAllText(Path.Combine(objectDir, "xl.meta"), "broken");
        File.Delete(Path.Combine(objectDir, "xl.meta.bkp"));

        // Act
        await _heal.RunPassAsync(CancellationToken.None);

        // Assert: мета читается, EcIndex корректен
        var healed = XlMetaFile.Read(objectDir, out _);
        healed.VersionId.Should().Be(meta.VersionId);
        healed.Ec!.EcIndex.Should().Be((byte)disk.PositionInSet, "копия консенсусной записи с EcIndex диска");
    }

    [Fact]
    public async Task Heal_OfflineDiskSkipped_NoError()
    {
        // Arrange: один диск offline (каталог удалён)
        await PutAsync("off", 1000, 8);
        Directory.Delete(_roots[3], recursive: true);
        _pool.RefreshOnline();

        // Act — проход не падает
        var act = async () => await _heal.RunPassAsync(CancellationToken.None);

        // Assert
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Heal_MissingJournalEntries_Reconstructed()
    {
        // Arrange: parts.json отсутствует на одном диске сета загрузки
        var uploadId = Guid.NewGuid().ToString("N");
        _drive.CreateUploadDir("b", "jrnl", uploadId, new XlMetaRecord(Guid.Parse(uploadId), 0,
            DateTimeOffset.UnixEpoch, "", "text/plain", new Dictionary<string, string>(),
            new Dictionary<string, string>(), ""));
        _drive.WriteUploads("b", "jrnl",
        [
            new MultipartJournals.UploadJournalEntry(uploadId, "b", "jrnl",
                DateTimeOffset.UnixEpoch.ToUnixTimeMilliseconds(), "root"),
        ]);
        using (var staging = _drive.CreateUploadStaging("b", "jrnl", uploadId))
        {
            var body = new byte[5 * 1024 * 1024];
            new Random(9).NextBytes(body);
            var digest = await _drive.WritePartAsync(staging, 1, new MemoryStream(body), body.LongLength,
                CancellationToken.None);
            _drive.CommitUploadPart(staging, 1, digest.ETagHex, digest.Length, DateTimeOffset.UnixEpoch);
        }

        var setIndex = _pool.PickSetIndex("b", "jrnl");
        var disk = _pool.Sets[setIndex][2];
        var partsJson = Directory.EnumerateFiles(
            Path.Combine(disk.Root, ".owns3.sys", "multipart"), "parts.json", SearchOption.AllDirectories).Single();
        File.Delete(partsJson);

        // Act
        await _heal.RunPassAsync(CancellationToken.None);

        // Assert: журнал восстановлен копией
        _drive.ReadParts("b", "jrnl", uploadId).Should().ContainSingle().Which.PartNumber.Should().Be(1);
    }

    [Fact]
    public async Task RequestPass_DuringRunningPass_DoesNotDuplicate()
    {
        // Arrange/Act: идущий проход + RequestPass — параллельный не создаётся
        var running = _heal.RunPassAsync(CancellationToken.None);
        _heal.RequestPass();
        await running;

        // Assert — IsRunning сброшен после завершения, второй проход не дублировал
        _heal.IsRunning.Should().BeFalse();
    }

    [Fact]
    public async Task Heal_EmptyObject_Restored()
    {
        // Arrange: size 0 — шардовые файлы нулевые, blockCount 0
        var meta = await PutAsync("empty", 0, 10);
        var setIndex = _pool.PickSetIndex("b", "empty");
        var disk = _pool.Sets[setIndex][0];
        var partPath = Path.Combine(disk.Root, "b", "empty", meta.DataDirName, "part.1");
        File.Delete(partPath);

        // Act
        await _heal.RunPassAsync(CancellationToken.None);

        // Assert
        File.Exists(partPath).Should().BeTrue("пустой шард-файл восстановлен");
        new FileInfo(partPath).Length.Should().Be(0);
    }

    [Fact]
    public async Task Status_ReflectsDegradation()
    {
        // Arrange: 1 диск offline
        await PutAsync("st", 100, 11);
        Directory.Delete(_roots[2], recursive: true);
        _pool.RefreshOnline();

        // Act
        var status = _status.GetStatus();

        // Assert: сет деградирован, но читаем и записываем (онлайн 3 ≥ k=3/writeQuorum 3)
        status.Mode.Should().Be("XL Erasure");
        var set = status.Sets.Should().ContainSingle().Subject;
        set.Readable.Should().BeTrue();
        set.Writable.Should().BeTrue();
        set.Drives.Single(d => d.Position == 2).Online.Should().BeFalse("деградация видна в статусе");
    }
}
