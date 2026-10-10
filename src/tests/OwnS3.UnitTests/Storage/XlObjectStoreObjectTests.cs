using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Фикстура: temp-том + фиксированный TimeProvider + store (полный teardown);
// общий бакет «b» создаётся один раз (том на класс, ограничение изоляции 8).
public sealed class StoreFixture : IAsyncLifetime
{
    public string Root { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public XlVolume Volume { get; }
    public XlObjectStore Store { get; }

    public StoreFixture()
    {
        Volume = new XlVolume(Root, TimeProvider.System);
        Volume.Initialize();
        Store = new XlObjectStore(Volume, new FixedTimeProvider(TestVectors.FixedTime),
            NullLogger<XlObjectStore>.Instance);
    }

    public async ValueTask InitializeAsync() => await Store.CreateBucketAsync("b", CancellationToken.None);

    public ValueTask DisposeAsync()
    {
        // Teardown: temp-том удаляется при любом исходе
        if (Directory.Exists(Root))
            Directory.Delete(Root, recursive: true);
        return ValueTask.CompletedTask;
    }
}

// PutObject/DeleteObject(s): коммит-цикл, атомарность, раскладка канона 04,
// идемпотентность, спецключи (Get-ассерты дописаны в Task 7).
public class XlObjectStoreObjectTests(StoreFixture fixture) : IClassFixture<StoreFixture>
{
    private static readonly ObjectUploadMetadata Meta =
        new("text/plain", new Dictionary<string, string> { ["k"] = "v" });

    private XlObjectStore Store => fixture.Store;
    private XlVolume Volume => fixture.Volume;
    private string Root => fixture.Root;

    private static Task<PutResult> PutAsync(XlObjectStore store, string bucket, string key, string content) =>
        store.PutObjectAsync(bucket, key, new MemoryStream(Encoding.UTF8.GetBytes(content)),
            Encoding.UTF8.GetByteCount(content), Meta, CancellationToken.None);

    [Fact]
    public async Task PutGet_Roundtrip_ContentMetadataEtag()
    {
        // Arrange / Act
        var result = await PutAsync(Store, "b", "roundtrip", "hello");

        // Assert: ETag = md5("hello") hex В КАВЫЧКАХ (P8)
        result.ETag.Should().Be("\"5d41402abc4b2a76b9719d911017c592\"");
    }

    [Fact]
    public async Task Put_DiskLayout_CanonicalPaths()
    {
        // Arrange / Act
        await PutAsync(Store, "b", "layout", "hello");

        // Assert: раскладка канона 04 §1 (критерий спеки §10.3)
        var objectDir = Path.Combine(Root, "b", "layout");
        File.Exists(Path.Combine(objectDir, "xl.meta")).Should().BeTrue();
        Directory.GetDirectories(objectDir).Should().ContainSingle();
        File.Exists(Path.Combine(objectDir, Directory.GetDirectories(objectDir)[0], "part.1"))
            .Should().BeTrue();
        foreach (var dir in new[] { "tmp", "multipart", ".trash", "buckets", "config" })
            Directory.Exists(Path.Combine(Root, ".owns3.sys", dir)).Should().BeTrue();
        File.Exists(Path.Combine(Root, ".owns3.sys", "volume.json")).Should().BeTrue();
    }

    [Fact]
    public async Task Put_MissingBucket_NoSuchBucket()
    {
        // Arrange / Act
        var act = async () => await PutAsync(Store, "missing", "k", "x");

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.NoSuchBucket);
    }

    [Fact]
    public async Task Put_LengthMismatch_ThrowsIntegrity_StagingRemoved()
    {
        // Arrange: заявлено 5, фактически 3 байта
        var body = new MemoryStream(new byte[3]);

        // Act
        var act = async () => await Store.PutObjectAsync("b", "len-mismatch", body, 5, Meta, CancellationToken.None);

        // Assert: остаточный случай — XlIntegrityException; staging удалён
        await act.Should().ThrowAsync<XlIntegrityException>();
        Directory.EnumerateFileSystemEntries(Volume.TmpDir).Should().BeEmpty();
        Directory.Exists(Path.Combine(Root, "b", "len-mismatch")).Should().BeFalse();
    }

    private sealed class ThrowingStream : Stream
    {
        private int _reads;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => 0; set => throw new NotSupportedException(); }

        public override int Read(byte[] buffer, int offset, int count) =>
            _reads++ == 0 ? 3 : throw new IOException("разрыв соединения");

        public override void Flush() { }
        public override Task FlushAsync(CancellationToken ct) => Task.CompletedTask;
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    [Fact]
    public async Task Put_BodyThrowsMidway_ObjectInvisible_TmpEmpty()
    {
        // Arrange: тело рвётся на втором чтении

        // Act
        var act = async () => await Store.PutObjectAsync("b", "body-throws", new ThrowingStream(), 100,
            Meta, CancellationToken.None);

        // Assert: прерванный PUT невидим и не оставляет staging (критерий §10.4)
        await act.Should().ThrowAsync<IOException>();
        Directory.EnumerateFileSystemEntries(Volume.TmpDir).Should().BeEmpty();
        Directory.Exists(Path.Combine(Root, "b", "body-throws")).Should().BeFalse();
    }

    [Fact]
    public async Task Put_CommitVisibility_Immediate()
    {
        // Arrange / Act
        await PutAsync(Store, "b", "commit", "hello");

        // Assert: после успешного ответа xl.meta и dataDir на месте (критерий §10.4)
        var meta = XlMetaFile.Read(Path.Combine(Root, "b", "commit"), out _);
        Directory.Exists(Path.Combine(Root, "b", "commit", meta.DataDirName)).Should().BeTrue();
    }

    [Fact]
    public async Task Put_Overwrite_LastContentWins_OldDataDirOrphaned()
    {
        // Arrange: две версии ключа — два uuid-каталога
        await PutAsync(Store, "b", "overwrite", "v1");
        await PutAsync(Store, "b", "overwrite", "v2");
        var objectDir = Path.Combine(Root, "b", "overwrite");

        // Act: старый dataDir стареет на 2 ч — фоновая чистка забирает orphan
        var current = XlMetaFile.Read(objectDir, out _).DataDirName;
        foreach (var dir in Directory.GetDirectories(objectDir).Where(d => Path.GetFileName(d) != current))
            File.SetLastWriteTimeUtc(dir, DateTime.UtcNow.AddHours(-2));
        await Volume.RunCleanupAsync(CancellationToken.None);

        // Assert: остался ровно один dataDir — текущий (критерий §10.6)
        Directory.GetDirectories(objectDir).Should().ContainSingle(Path.Combine(objectDir, current));
    }

    [Fact]
    public async Task Put_NewKeyUnderLivePrefix_OverwriteScheme()
    {
        // Arrange: живой каталог-префикс a/b; ключ a пишется схемой перезаписи
        await PutAsync(Store, "b", "a/b", "nested");
        await PutAsync(Store, "b", "a", "flat");

        // Act / Assert: оба живы — xl.meta у a и у a/b (канон 04 §4 п.3)
        XlMetaFile.Read(Path.Combine(Root, "b", "a"), out _).Size.Should().Be(4);
        XlMetaFile.Read(Path.Combine(Root, "b", "a", "b"), out _).Size.Should().Be(6);
    }

    [Fact]
    public async Task Delete_MissingKey_IdempotentSuccess()
    {
        // Arrange: бакет есть, объекта нет

        // Act / Assert: отсутствие объекта — успех (204-семантика канона 02 §1)
        await Store.DeleteObjectAsync("b", "missing-key", CancellationToken.None);
        await Store.DeleteObjectAsync("b", "missing-key", CancellationToken.None);
    }

    [Fact]
    public async Task Delete_RemovesObject()
    {
        // Arrange
        await PutAsync(Store, "b", "removed", "hello");

        // Act
        await Store.DeleteObjectAsync("b", "removed", CancellationToken.None);

        // Assert: ни xl.meta, ни каталога объекта
        Directory.Exists(Path.Combine(Root, "b", "removed")).Should().BeFalse();
    }

    [Fact]
    public async Task Delete_KeepsNestedSibling()
    {
        // Arrange: ключи a и a/b делят каталог (канон 04 §4 п.5)
        await PutAsync(Store, "b", "a", "flat");
        await PutAsync(Store, "b", "a/b", "nested");

        // Act
        await Store.DeleteObjectAsync("b", "a", CancellationToken.None);

        // Assert: удаление a не затрагивает a/b (критерий §10.5)
        XlMetaFile.Read(Path.Combine(Root, "b", "a", "b"), out _).Size.Should().Be(6);
        Directory.Exists(Path.Combine(Root, "b", "a")).Should().BeTrue();
    }

    [Fact]
    public async Task Delete_DirKey()
    {
        // Arrange: ключ-каталог «dir/» — полноценный ключ
        await PutAsync(Store, "b", "dir/", "x");

        // Act
        await Store.DeleteObjectAsync("b", "dir/", CancellationToken.None);

        // Assert
        Directory.Exists(Path.Combine(Root, "b", "dir__XLDIR__")).Should().BeFalse();
    }

    [Fact]
    public async Task Delete_LeavesNoXlMeta_DataDirCleanedByVolume()
    {
        // Arrange: краш-имитация — xl.meta удалён вручную, dataDir остался
        await PutAsync(Store, "b", "orphan", "hello");
        var objectDir = Path.Combine(Root, "b", "orphan");
        File.Delete(Path.Combine(objectDir, "xl.meta"));
        var dataDir = Directory.GetDirectories(objectDir)[0];
        File.SetLastWriteTimeUtc(dataDir, DateTime.UtcNow.AddHours(-2));

        // Act
        await Volume.RunCleanupAsync(CancellationToken.None);

        // Assert: orphan без xl.meta убирается чисткой
        Directory.Exists(dataDir).Should().BeFalse();
    }

    [Fact]
    public async Task DeleteObjects_Mixed_PerKeyResults()
    {
        // Arrange
        await PutAsync(Store, "b", "mix-1", "v1");

        // Act
        var results = await Store.DeleteObjectsAsync("b", ["mix-1", "mix-2"], quiet: false, CancellationToken.None);

        // Assert: per-key исходы; несуществующий ключ — успех
        results.Should().HaveCount(2);
        results.Should().OnlyContain(r => r.Deleted && r.ErrorCode == null);
    }

    [Fact]
    public async Task DeleteObjects_MissingBucket_NoSuchBucket()
    {
        // Arrange / Act
        var act = async () => await Store.DeleteObjectsAsync("missing", ["k"], false, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.NoSuchBucket);
    }

    [Fact]
    public async Task ParallelPut_SameKey_EveryCommitAtomic()
    {
        // Arrange: 4 конкурентных PUT одного ключа (last-writer-wins, §2.3)
        var puts = Enumerable.Range(0, 4)
            .Select(i => Task.Run(() => PutAsync(Store, "b", "parallel", "v" + i)))
            .ToList();

        // Act
        var outcomes = await Task.WhenAll(puts);

        // Assert: раскладка консистентна — xl.meta читается, указывает ровно на
        // один dataDir (orphan-каталоги допустимы, чистятся по возрасту)
        var objectDir = Path.Combine(Root, "b", "parallel");
        var meta = XlMetaFile.Read(objectDir, out _);
        meta.Size.Should().Be(2);
        Directory.Exists(Path.Combine(objectDir, meta.DataDirName)).Should().BeTrue();
        foreach (var (outcome, i) in outcomes.Select((o, idx) => (o, idx)))
            outcome.ETag.Should().Be("\"" + TestHashes.Md5Hex("v" + i) + "\""); // ETag каждого = md5 его тела
    }

    [Fact]
    public async Task Put_KeyTooLongAfterEncoding_ThrowsInvalidArgument()
    {
        // Arrange: сегмент 300 ASCII-символов — за лимитом 255 байт

        // Act
        var act = async () => await PutAsync(Store, "b", new string('a', 300), "x");

        // Assert: доменный 400-исход Storage (App маппит в InvalidArgument)
        await act.Should().ThrowAsync<XlInvalidArgumentException>()
            .WithMessage("Key too long after encoding");
    }

    [Theory]
    [InlineData("a//b")]
    [InlineData(".")]
    [InlineData("100%")]
    [InlineData("lit%2E")]
    [InlineData("x__XLDIR__")]
    [InlineData("dir/")]
    public async Task Put_SpecialKeys_Roundtrip(string key)
    {
        // Arrange / Act
        await PutAsync(Store, "b", key, "x");

        // Assert: раскладка по кодированному пути канона 04 §1
        var encoded = XlPathEncoder.EncodePath(key).Split('/');
        var dir = Path.Combine([Root, "b", .. encoded]);
        XlMetaFile.Read(dir, out _).Size.Should().Be(1);
    }
}


// Вспомогательные вычисления эталонных хэшей (сверка с реализацией записи).
file static class TestHashes
{
    public static string Md5Hex(string value) =>
        Convert.ToHexString(MD5.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
}
