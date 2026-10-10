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

    public async ValueTask InitializeAsync() => await Store.CreateBucketAsync("b", TestContext.Current.CancellationToken);

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
            Encoding.UTF8.GetByteCount(content), Meta, TestContext.Current.CancellationToken);

    [Fact]
    public async Task PutGet_Roundtrip_ContentMetadataEtag()
    {
        // Arrange / Act
        var result = await PutAsync(Store, "b", "roundtrip", "hello");
        var content = await Store.GetObjectAsync("b", "roundtrip", new ObjectReadOptions(null, null, null),
            TestContext.Current.CancellationToken);
        using var reader = new StreamReader(content.Body);

        // Assert: ETag = md5("hello") hex В КАВЫЧКАХ (P8); тело/метаданные на месте
        result.ETag.Should().Be("\"5d41402abc4b2a76b9719d911017c592\"");
        result.LastModified.Should().Be(TestVectors.FixedTime);
        (await reader.ReadToEndAsync(TestContext.Current.CancellationToken)).Should().Be("hello");
        content.Metadata.ETag.Should().Be("\"5d41402abc4b2a76b9719d911017c592\"");
        content.Metadata.Size.Should().Be(5);
        content.Metadata.ContentType.Should().Be("text/plain");
        content.Metadata.UserMetadata.Should().ContainKey("k").WhoseValue.Should().Be("v");
        content.Metadata.LastModified.Should().Be(TestVectors.FixedTime);
        content.NotModified.Should().BeFalse();
        content.Range.Should().BeNull();
    }

    [Fact]
    public async Task Get_Missing_NoSuchKey()
    {
        // Arrange / Act
        var act = async () => await Store.GetObjectAsync("b", "missing-key",
            new ObjectReadOptions(null, null, null), TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.NoSuchKey);
    }

    [Fact]
    public async Task Get_Range206_ReturnsSlice_TotalIsFullSize()
    {
        // Arrange
        await PutAsync(Store, "b", "range-206", "abcdef");

        // Act
        var content = await Store.GetObjectAsync("b", "range-206",
            new ObjectReadOptions(null, new ByteRange(2, 3), null), TestContext.Current.CancellationToken);
        using var reader = new StreamReader(content.Body);
        var body = await reader.ReadToEndAsync(TestContext.Current.CancellationToken);

        // Assert: тело — срез, Range несёт полный размер объекта
        body.Should().Be("cd");
        content.Range.Should().Be(new AppliedByteRange(2, 3, 6));
    }

    private async Task<string> TamperAndGetAsync(string key, byte tamperByte, ByteRange? range)
    {
        // Arrange: порча одного байта part.1 — checksum-сверка обязана упасть
        // ДО передачи каких-либо байтов клиенту (канон 04 §2)
        await PutAsync(Store, "b", key, "abcdef");
        var objectDir = Path.Combine(Root, "b", XlPathEncoder.EncodePath(key));
        var meta = XlMetaFile.Read(objectDir, out _);
        var part = Path.Combine(objectDir, meta.DataDirName, "part.1");
        var bytes = File.ReadAllBytes(part);
        bytes[tamperByte] ^= 0xFF;
        File.WriteAllBytes(part, bytes);

        // Act
        var act = async () => await Store.GetObjectAsync("b", key, new ObjectReadOptions(null, range, null),
            TestContext.Current.CancellationToken);
        var message = (await act.Should().ThrowAsync<XlIntegrityException>()).Which.Message;
        return message;
    }

    [Fact]
    public async Task Get_TamperedData_ThrowsIntegrityBeforeBody()
    {
        // Arrange / Act / Assert: порча внутри запрошенного диапазона
        await TamperAndGetAsync("tamper-inside", tamperByte: 2, range: null);
    }

    [Fact]
    public async Task Get_RangeChecksummed_FullObjectHash_TamperedOutsideRange_ThrowsIntegrity()
    {
        // Arrange / Act / Assert: хэш — ВСЕГО объекта: порча байта 0 вне Range 4-5
        // всё равно роняет GET (критерий §10.2)
        await TamperAndGetAsync("tamper-outside", tamperByte: 0, range: new ByteRange(4, 5));
    }

    [Fact]
    public async Task Get_Conditional_IfMatch_Ok()
    {
        // Arrange
        await PutAsync(Store, "b", "cond-ifmatch", "hello");
        var etag = "\"5d41402abc4b2a76b9719d911017c592\"";

        // Act: If-Match совпал → полный объект
        var content = await Store.GetObjectAsync("b", "cond-ifmatch",
            new ObjectReadOptions(new ObjectConditions(etag, null, null, null), null, null),
            TestContext.Current.CancellationToken);

        // Assert
        content.NotModified.Should().BeFalse();
    }

    [Fact]
    public async Task Get_Conditional_IfMatch_Failed412()
    {
        // Arrange
        await PutAsync(Store, "b", "cond-412", "hello");

        // Act
        var act = async () => await Store.GetObjectAsync("b", "cond-412",
            new ObjectReadOptions(new ObjectConditions("\"other\"", null, null, null), null, null),
            TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.PreconditionFailed);
    }

    [Fact]
    public async Task Get_Conditional_IfNoneMatch_Match_NotModifiedFlag()
    {
        // Arrange
        await PutAsync(Store, "b", "cond-inm", "hello");
        var etag = "\"5d41402abc4b2a76b9719d911017c592\"";

        // Act
        var content = await Store.GetObjectAsync("b", "cond-inm",
            new ObjectReadOptions(new ObjectConditions(null, etag, null, null), null, null),
            TestContext.Current.CancellationToken);
        using (content.Body)
        {
            // Assert: флаг NotModified, Body = Stream.Null, Metadata заполнен для 304
            content.NotModified.Should().BeTrue();
            content.Body.Should().BeSameAs(Stream.Null);
            content.Metadata.ETag.Should().Be(etag);
            content.Metadata.LastModified.Should().Be(TestVectors.FixedTime);
        }
    }

    [Fact]
    public async Task Get_Conditional_IfModifiedSince_NotModified()
    {
        // Arrange: объект не менялся после даты клиента (modTime обрезан до секунд)
        await PutAsync(Store, "b", "cond-ims", "hello");
        var since = TestVectors.FixedTime; // == modTime (обрезка мс)

        // Act
        var content = await Store.GetObjectAsync("b", "cond-ims",
            new ObjectReadOptions(new ObjectConditions(null, null, since, null), null, null),
            TestContext.Current.CancellationToken);
        using (content.Body)
        {
            // Assert
            content.NotModified.Should().BeTrue();
        }
    }

    [Fact]
    public async Task Get_Conditional_IfUnmodifiedSince_Failed412()
    {
        // Arrange: объект менялся ПОСЛЕ границы клиента
        await PutAsync(Store, "b", "cond-ius", "hello");
        var since = TestVectors.FixedTime.AddSeconds(-10);

        // Act
        var act = async () => await Store.GetObjectAsync("b", "cond-ius",
            new ObjectReadOptions(new ObjectConditions(null, null, null, since), null, null),
            TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.PreconditionFailed);
    }

    [Fact]
    public async Task Get_Conditional_IfMatch_PriorityOverIfNoneMatch()
    {
        // Arrange: оба заданы и совпали — If-Match решает единолично (Proceed)
        await PutAsync(Store, "b", "cond-prio", "hello");
        var etag = "\"5d41402abc4b2a76b9719d911017c592\"";

        // Act
        var content = await Store.GetObjectAsync("b", "cond-prio",
            new ObjectReadOptions(new ObjectConditions(etag, etag, null, null), null, null),
            TestContext.Current.CancellationToken);

        // Assert: не 304 — условие If-None-Match не применилось
        content.NotModified.Should().BeFalse();
    }

    [Theory]
    [InlineData(null, -2L, "ef", 4L, 5L, 6L)]           // суффикс bytes=-2
    [InlineData(2L, null, "cdef", 2L, 5L, 6L)]          // bytes=2- (открытый конец)
    [InlineData(2L, 100L, "cdef", 2L, 5L, 6L)]           // end за размером обрезан
    public async Task Get_Range_Forms(long? start, long? end, string expected,
        long expStart, long expEnd, long total)
    {
        // Arrange
        await PutAsync(Store, "b", "range-forms", "abcdef");

        // Act
        var content = await Store.GetObjectAsync("b", "range-forms",
            new ObjectReadOptions(null, new ByteRange(start, end), null), TestContext.Current.CancellationToken);
        using var reader = new StreamReader(content.Body);
        var body = await reader.ReadToEndAsync(TestContext.Current.CancellationToken);

        // Assert
        body.Should().Be(expected);
        content.Range.Should().Be(new AppliedByteRange(expStart, expEnd, total));
    }

    [Fact]
    public async Task Get_Range_StartOutOfBounds_416()
    {
        // Arrange
        await PutAsync(Store, "b", "range-416", "abcdef");

        // Act
        var act = async () => await Store.GetObjectAsync("b", "range-416",
            new ObjectReadOptions(null, new ByteRange(6, null), null), TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.InvalidRange);
    }

    [Fact]
    public async Task Get_Range_EmptyObject_416()
    {
        // Arrange: пустой объект + Range — нет валидных диапазонов
        await PutAsync(Store, "b", "range-empty", "");

        // Act
        var act = async () => await Store.GetObjectAsync("b", "range-empty",
            new ObjectReadOptions(null, new ByteRange(0, null), null), TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.InvalidRange);
    }

    [Fact]
    public async Task Get_EmptyObject_Full_200()
    {
        // Arrange / Act
        await PutAsync(Store, "b", "range-emptyfull", "");
        var content = await Store.GetObjectAsync("b", "range-emptyfull",
            new ObjectReadOptions(null, null, null), TestContext.Current.CancellationToken);
        using var reader = new StreamReader(content.Body);

        // Assert: без Range пустой объект читается (200)
        content.Range.Should().BeNull();
        (await reader.ReadToEndAsync(TestContext.Current.CancellationToken)).Should().Be("");
    }

    [Fact]
    public async Task Get_IfRange_Match206()
    {
        // Arrange
        await PutAsync(Store, "b", "ifrange-match", "abcdef");
        var etag = "\"" + TestHashes.Md5Hex("abcdef") + "\"";

        // Act
        var content = await Store.GetObjectAsync("b", "ifrange-match",
            new ObjectReadOptions(null, new ByteRange(2, 3), etag), TestContext.Current.CancellationToken);
        using var reader = new StreamReader(content.Body);
        var body = await reader.ReadToEndAsync(TestContext.Current.CancellationToken);

        // Assert: сильный ETag совпал — Range применён
        body.Should().Be("cd");
        content.Range.Should().Be(new AppliedByteRange(2, 3, 6));
    }

    [Fact]
    public async Task Get_IfRange_Mismatch200()
    {
        // Arrange
        await PutAsync(Store, "b", "ifrange-miss", "abcdef");

        // Act
        var content = await Store.GetObjectAsync("b", "ifrange-miss",
            new ObjectReadOptions(null, new ByteRange(2, 3), "\"other\""), TestContext.Current.CancellationToken);
        using var reader = new StreamReader(content.Body);
        var body = await reader.ReadToEndAsync(TestContext.Current.CancellationToken);

        // Assert: несовпал — полный объект (200)
        body.Should().Be("abcdef");
        content.Range.Should().BeNull();
    }

    [Fact]
    public async Task Get_IfRange_DateForm200()
    {
        // Arrange: дата-форма If-Range — трактуется как несовпадение
        await PutAsync(Store, "b", "ifrange-date", "abcdef");
        var ifRangeDate = TestVectors.FixedTime.ToString("R");

        // Act
        var content = await Store.GetObjectAsync("b", "ifrange-date",
            new ObjectReadOptions(null, new ByteRange(2, 3), ifRangeDate), TestContext.Current.CancellationToken);
        using var reader = new StreamReader(content.Body);
        var body = await reader.ReadToEndAsync(TestContext.Current.CancellationToken);

        // Assert
        body.Should().Be("abcdef");
        content.Range.Should().BeNull();
    }

    [Fact]
    public async Task Head_ReturnsMetadata_WithoutReadingData()
    {
        // Arrange: порча part.1 — HEAD данных не читает, сверка не выполняется
        await PutAsync(Store, "b", "head-meta", "hello");
        var objectDir = Path.Combine(Root, "b", "head-meta");
        var meta = XlMetaFile.Read(objectDir, out _);
        var part = Path.Combine(objectDir, meta.DataDirName, "part.1");
        var bytes = File.ReadAllBytes(part);
        bytes[0] ^= 0xFF;
        File.WriteAllBytes(part, bytes);

        // Act
        var content = await Store.HeadObjectAsync("b", "head-meta", null, TestContext.Current.CancellationToken);
        using (content.Body)
        {
            // Assert
            content.Metadata.Size.Should().Be(5);
            content.NotModified.Should().BeFalse();
        }
    }

    [Fact]
    public async Task Head_Range_AppliedByteRange()
    {
        // Arrange
        await PutAsync(Store, "b", "head-range", "abcdef");

        // Act
        var content = await Store.HeadObjectAsync("b", "head-range",
            new ObjectReadOptions(null, new ByteRange(2, 3), null), TestContext.Current.CancellationToken);
        using (content.Body)
        {
            // Assert: 206-семантика — диапазон применён, тела нет
            content.Range.Should().Be(new AppliedByteRange(2, 3, 6));
            content.Body.Should().BeSameAs(Stream.Null);
        }
    }

    [Fact]
    public async Task Head_IfNoneMatch_NotModified()
    {
        // Arrange
        await PutAsync(Store, "b", "head-inm", "hello");
        var etag = "\"5d41402abc4b2a76b9719d911017c592\"";

        // Act
        var content = await Store.HeadObjectAsync("b", "head-inm",
            new ObjectReadOptions(new ObjectConditions(null, etag, null, null), null, null),
            TestContext.Current.CancellationToken);

        // Assert
        content.NotModified.Should().BeTrue();
    }

    [Theory]
    [InlineData("a//b")]
    [InlineData(".")]
    [InlineData("100%")]
    [InlineData("lit%2E")]
    [InlineData("x__XLDIR__")]
    [InlineData("dir/")]
    [InlineData("café/юникод/ключ")]
    public async Task Get_SpecialKeys_Roundtrip(string key)
    {
        // Arrange
        await PutAsync(Store, "b", key, "payload-" + key.Length);

        // Act
        var content = await Store.GetObjectAsync("b", key, new ObjectReadOptions(null, null, null),
            TestContext.Current.CancellationToken);
        using var reader = new StreamReader(content.Body);

        // Assert: Get по исходному ключу возвращает записанное
        (await reader.ReadToEndAsync(TestContext.Current.CancellationToken)).Should().Be("payload-" + key.Length);
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
        var act = async () => await Store.PutObjectAsync("b", "len-mismatch", body, 5, Meta, TestContext.Current.CancellationToken);

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
            Meta, TestContext.Current.CancellationToken);

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
        await Volume.RunCleanupAsync(TestContext.Current.CancellationToken);

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
        await Store.DeleteObjectAsync("b", "missing-key", TestContext.Current.CancellationToken);
        await Store.DeleteObjectAsync("b", "missing-key", TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Delete_RemovesObject()
    {
        // Arrange
        await PutAsync(Store, "b", "removed", "hello");

        // Act
        await Store.DeleteObjectAsync("b", "removed", TestContext.Current.CancellationToken);

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
        await Store.DeleteObjectAsync("b", "a", TestContext.Current.CancellationToken);

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
        await Store.DeleteObjectAsync("b", "dir/", TestContext.Current.CancellationToken);

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
        await Volume.RunCleanupAsync(TestContext.Current.CancellationToken);

        // Assert: orphan без xl.meta убирается чисткой
        Directory.Exists(dataDir).Should().BeFalse();
    }

    [Fact]
    public async Task DeleteObjects_Mixed_PerKeyResults()
    {
        // Arrange
        await PutAsync(Store, "b", "mix-1", "v1");

        // Act
        var results = await Store.DeleteObjectsAsync("b", ["mix-1", "mix-2"], quiet: false, TestContext.Current.CancellationToken);

        // Assert: per-key исходы; несуществующий ключ — успех
        results.Should().HaveCount(2);
        results.Should().OnlyContain(r => r.Deleted && r.ErrorCode == null);
    }

    [Fact]
    public async Task DeleteObjects_MissingBucket_NoSuchBucket()
    {
        // Arrange / Act
        var act = async () => await Store.DeleteObjectsAsync("missing", ["k"], false, TestContext.Current.CancellationToken);

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
