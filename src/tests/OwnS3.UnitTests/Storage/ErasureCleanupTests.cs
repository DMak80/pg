using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Storage;
using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.Storage;

// Тесты per-disk чисток EC (t40, план Task 12): пороги канона 04 §6/06 §10
// (включая фантом-записи правки Task 0). Топология 4/1, temp-корни,
// FixedTimeProvider для состаривания.
public class ErasureCleanupTests : IDisposable
{
    private static readonly DateTimeOffset BaseTime = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private readonly List<string> _roots = [];
    private readonly ErasurePool _pool;
    private readonly ErasureXlDrive _drive;
    private readonly FixedTimeProvider _time = new(BaseTime);

    public ErasureCleanupTests()
    {
        for (var i = 0; i < 4; i++)
        {
            var root = Path.Combine(Path.GetTempPath(), "owns3-ecc-" + Guid.NewGuid().ToString("N"));
            _roots.Add(root);
        }

        _pool = new ErasurePool(_roots, new ErasureTopology(4, 4, 1), _time);
        _pool.Initialize();
        _drive = new ErasureXlDrive(_pool, defaultParity: 1, _time, NullLogger<ErasureXlDrive>.Instance);
        _drive.CreateBucket("b");
    }

    public void Dispose()
    {
        // Teardown: temp-корни удаляются при любом исходе
        _pool.Dispose();
        foreach (var root in _roots)
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
    }

    private async Task<byte[]> PutAsync(string key, int len, int seed = 3)
    {
        var body = new byte[len];
        new Random(seed).NextBytes(body);
        using var staging = _drive.CreateStaging("b", key);
        var digest = (await _drive.WritePartAsync(staging, 1, new MemoryStream(body), body.LongLength,
            CancellationToken.None)).Throw().Value;
        var record = new XlMetaRecord(Guid.Empty, body.LongLength, DateTimeOffset.UnixEpoch,
            digest.ETagHex, "text/plain", new Dictionary<string, string>(), new Dictionary<string, string>(),
            digest.Sha256Hex);
        _drive.WriteMeta(staging, record);
        _drive.Commit("b", key, record, staging);
        return body;
    }

    private static void AgePath(string path, TimeSpan age) =>
        File.SetLastWriteTimeUtc(path, BaseTime.UtcDateTime - age);

    private static void AgeTree(string path, TimeSpan age)
    {
        // SetLastWriteTimeUtc работает и для файлов, и для каталогов
        AgePath(path, age);
        if (Directory.Exists(path))
            foreach (var entry in Directory.EnumerateFileSystemEntries(path))
                AgeTree(entry, age);
    }

    [Fact]
    public async Task Startup_WipesTmpOnEveryDisk()
    {
        // Arrange: мусор в tmp каждого диска
        foreach (var root in _roots)
        {
            var junkDir = Path.Combine(root, ".owns3.sys", "tmp", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(junkDir);
            File.WriteAllText(Path.Combine(junkDir, "junk"), "x");
        }

        // Act — рестарт пула: Initialize стирает tmp безусловно
        var pool2 = new ErasurePool(_roots, new ErasureTopology(4, 4, 1), _time);
        pool2.Initialize();
        pool2.Dispose();

        // Assert
        foreach (var root in _roots)
            Directory.EnumerateFileSystemEntries(Path.Combine(root, ".owns3.sys", "tmp"))
                .Should().BeEmpty($"tmp диска {root} вычищен стартом");
    }

    [Fact]
    public async Task AgedTrashRemoved_PerDisk()
    {
        // Arrange: aged-запись в .trash диска 1
        var trashDir = Path.Combine(_roots[1], ".owns3.sys", ".trash");
        var trashed = Path.Combine(trashDir, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(trashed);
        File.WriteAllText(Path.Combine(trashed, "x"), "x");
        AgeTree(trashed, TimeSpan.FromHours(2));

        // Act
        await _drive.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Assert
        Directory.Exists(trashed).Should().BeFalse("aged .trash удалён per-disk");
    }

    [Fact]
    public async Task OrphanDataDir_RemovedWhenNotInLocalMeta()
    {
        // Arrange: Guid-каталог без упоминания в локальной xl.meta, старше 1 ч
        await PutAsync("orphan-key", 500);
        var objectDir = Path.Combine(_roots[0], "b", "orphan-key");
        var meta = XlMetaFile.Read(objectDir, out _);
        var orphan = Path.Combine(objectDir, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(orphan);
        File.WriteAllText(Path.Combine(orphan, "part.1"), "x");
        AgeTree(orphan, TimeSpan.FromHours(2));

        // Act
        await _drive.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Assert
        Directory.Exists(orphan).Should().BeFalse("orphan-dataDir удалён (локальная сверка)");
        Directory.Exists(Path.Combine(objectDir, meta.DataDirName)).Should().BeTrue("живой dataDir не тронут");
    }

    [Fact]
    public async Task OrphanReferencedByLocalMeta_Kept()
    {
        // Arrange: dataDir, упомянутый локальной xl.meta (живой)
        await PutAsync("live-key", 500);
        var objectDir = Path.Combine(_roots[0], "b", "live-key");
        var meta = XlMetaFile.Read(objectDir, out _);
        AgeTree(objectDir, TimeSpan.FromHours(2)); // состарить всё — живой dataDir защищён метой

        // Act
        await _drive.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Assert
        Directory.Exists(Path.Combine(objectDir, meta.DataDirName)).Should().BeTrue(
            "dataDir, упомянутый xl.meta, не чистится");
    }

    [Fact]
    public async Task AbandonedUploads_24h_PerDisk()
    {
        // Arrange: каталог загрузки без записи в журнале, старше 24 ч (mtime-прокси)
        var multipartRoot = Path.Combine(_roots[2], ".owns3.sys", "multipart");
        var keyDir = Path.Combine(multipartRoot, "abandonedsha");
        var uploadDir = Path.Combine(keyDir, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(uploadDir);
        File.WriteAllText(MultipartJournals.UploadsJsonPath(keyDir), "[]");
        AgeTree(uploadDir, TimeSpan.FromHours(25));

        // Act
        await _drive.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Assert
        Directory.Exists(uploadDir).Should().BeFalse("брошенная загрузка (24 ч) удалена per-disk");
    }

    [Fact]
    public async Task PhantomRecord_AbsentQuorumOnSet_Removed()
    {
        // Arrange: ключ с валидной локальной метой на диске 0; на дисках 1–3 записи
        // нет, все онлайн — кворум отсутствия 3 ≥ k; mtime состарен
        await PutAsync("phantom", 500);
        var setIndex = _pool.PickSetIndex("b", "phantom");
        var disks = _pool.Sets[setIndex];
        // оставить запись ТОЛЬКО на первом диске сета
        foreach (var disk in disks.Skip(1))
        {
            var objectDir = Path.Combine(disk.Root, "b", "phantom");
            if (Directory.Exists(objectDir))
                Directory.Delete(objectDir, recursive: true);
        }

        AgeTree(Path.Combine(disks[0].Root, "b", "phantom"), TimeSpan.FromHours(2));

        // Act
        await _drive.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Assert: чистка устранила каталог ключа с диска
        Directory.Exists(Path.Combine(disks[0].Root, "b", "phantom")).Should().BeFalse(
            "фантом удалён локальной кворумной сверкой отсутствия");
    }

    [Fact]
    public async Task PhantomRecord_PresentInConsensus_Kept()
    {
        // Arrange: запись есть на 3 других дисках сета — ключ жив
        await PutAsync("alive", 500);

        // Act
        await _drive.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Assert: ключ не тронут ни на одном диске
        var setIndex = _pool.PickSetIndex("b", "alive");
        foreach (var disk in _pool.Sets[setIndex])
            Directory.Exists(Path.Combine(disk.Root, "b", "alive")).Should().BeTrue(
                $"живой в консенсусе ключ не чистится (диск {disk.Position})");
    }

    [Fact]
    public async Task PhantomRecord_NoAbsenceQuorum_Kept()
    {
        // Arrange: онлайн-дисков без записи < k (2 диска offline) — консервативно сохранить
        await PutAsync("keepme", 500);
        var setIndex = _pool.PickSetIndex("b", "keepme");
        var disks = _pool.Sets[setIndex];
        foreach (var disk in disks.Skip(1))
        {
            var objectDir = Path.Combine(disk.Root, "b", "keepme");
            if (Directory.Exists(objectDir))
                Directory.Delete(objectDir, recursive: true);
        }

        // 2 из 3 оставшихся peer-дисков offline → сверять не с кем: absent-peer'ов 0 < k
        Directory.Delete(disks[1].Root, recursive: true);
        Directory.Delete(disks[2].Root, recursive: true);
        _pool.RefreshOnline();
        AgeTree(Path.Combine(disks[0].Root, "b", "keepme"), TimeSpan.FromHours(2));

        // Act
        await _drive.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Assert: ключ консервативно сохранён (кворум отсутствия недостижим)
        Directory.Exists(Path.Combine(disks[0].Root, "b", "keepme")).Should().BeTrue(
            "кворум отсутствия недостижим — ключ сохранён до ближайшей сверки с кворумом");
    }
}
