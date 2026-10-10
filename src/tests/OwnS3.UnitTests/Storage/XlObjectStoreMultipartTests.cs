using System.Text;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Multipart-цикл XlObjectStore (канон 02 §5, 04 §5): раскладка, исходы,
// last-writer-wins, идемпотентность Complete/Abort, многочастевое чтение.
public class XlObjectStoreMultipartTests(StoreFixture fixture) : IClassFixture<StoreFixture>
{
    private static readonly ObjectUploadMetadata Meta =
        new("text/plain", new Dictionary<string, string> { ["k"] = "v" });

    private XlObjectStore Store => fixture.Store;
    private XlVolume Volume => fixture.Volume;
    private string Root => fixture.Root;

    // Минимальный валидный манифест Complete: N-1 частей по 5 МиБ + последняя произвольная.
    internal static byte[] PartBytes(int size, byte fill) =>
        Enumerable.Repeat(fill, size).ToArray();

    [Fact]
    public async Task Create_WritesLayoutAndJournal()
    {
        // Act
        var uploadId = await Store.CreateMultipartUploadAsync("b", "mp-key", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Assert: раскладка канона 04 §5 (uploadId — UUID N; xl.meta загрузки; запись журнала)
        Guid.TryParseExact(uploadId, "N", out _).Should().BeTrue();
        var keyDir = MultipartJournals.KeyDir(Volume.MultipartDir, "b", "mp-key");
        Directory.Exists(MultipartJournals.UploadDirPath(keyDir, uploadId)).Should().BeTrue();
        File.Exists(Path.Combine(MultipartJournals.UploadDirPath(keyDir, uploadId), "xl.meta")).Should().BeTrue();
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir))
            .Should().ContainSingle().Which.Should().Match<MultipartJournals.UploadJournalEntry>(e =>
                e.UploadId == uploadId && e.Bucket == "b" && e.Key == "mp-key"
                && e.AccessKey == "writer");
        // xl.meta загрузки: метаданные будущего объекта, Size/ETag/Sha — пустые (М4)
        var record = XlMetaFile.Read(MultipartJournals.UploadDirPath(keyDir, uploadId), out _);
        record.ContentType.Should().Be("text/plain");
        record.UserMetadata.Should().ContainKey("k").WhoseValue.Should().Be("v");
        record.Size.Should().Be(0);
        record.ETag.Should().BeEmpty();
        record.ContentSha256.Should().BeEmpty();
        record.ModTime.Should().Be(TestVectors.FixedTime);
    }

    [Fact]
    public async Task Create_MissingBucket_NoSuchBucket()
    {
        // Act
        var act = async () => await Store.CreateMultipartUploadAsync("nob", "k", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchBucket);
    }

    [Fact]
    public async Task Abort_TrashesDirAndRecord_SecondAbort_NoSuchUpload()
    {
        // Arrange
        var uploadId = await Store.CreateMultipartUploadAsync("b", "abort-key", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Act
        await Store.AbortMultipartUploadAsync("b", "abort-key", uploadId, TestContext.Current.CancellationToken);
        var second = async () => await Store.AbortMultipartUploadAsync("b", "abort-key", uploadId,
            TestContext.Current.CancellationToken);

        // Assert: повторный Abort — NoSuchUpload (канон 02 §5); журнал пуст; каталог в .trash
        await second.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchUpload);
        var keyDir = MultipartJournals.KeyDir(Volume.MultipartDir, "b", "abort-key");
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir)).Should().BeEmpty();
        Directory.Exists(MultipartJournals.UploadDirPath(keyDir, uploadId)).Should().BeFalse();
    }

    [Fact]
    public async Task Abort_UnknownUpload_NoSuchUpload()
    {
        // Arrange / Act
        var act = async () => await Store.AbortMultipartUploadAsync("b", "k", "no-such-upload",
            TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task Create_TwoUploadsSameKey_BothListedInJournal()
    {
        // Act: две параллельные загрузки одного ключа
        var first = await Store.CreateMultipartUploadAsync("b", "two-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var second = await Store.CreateMultipartUploadAsync("b", "two-key", Meta, "other-writer",
            TestContext.Current.CancellationToken);

        // Assert: обе записи в журнале, обе с владельцами
        var keyDir = MultipartJournals.KeyDir(Volume.MultipartDir, "b", "two-key");
        var uploads = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));
        uploads.Should().HaveCount(2);
        uploads.Select(e => e.UploadId).Should().Contain(first).And.Contain(second);
        uploads.Single(e => e.UploadId == first).AccessKey.Should().Be("writer");
        uploads.Single(e => e.UploadId == second).AccessKey.Should().Be("other-writer");
    }

    [Fact]
    public async Task BrokenUploadsJson_CreateNotAffected_JournalBitAfterCreate()
    {
        // Arrange: живая загрузка, затем журнал ключа перезаписан мусором
        var uploadId = await Store.CreateMultipartUploadAsync("b", "broken-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var keyDir = MultipartJournals.KeyDir(Volume.MultipartDir, "b", "broken-key");
        File.WriteAllText(MultipartJournals.UploadsJsonPath(keyDir), "{broken");

        // Act: Create по ДРУГОМУ ключу не затронут битым журналом соседа
        var fresh = async () => await Store.CreateMultipartUploadAsync("b", "other-key", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Assert: создание живёт; загрузка с битым журналом разрешается как
        // NoSuchUpload (М2) — полный кейс UploadPart закрыт в Task 4
        await fresh.Should().NotThrowAsync();
        var resolve = async () => await Store.AbortMultipartUploadAsync("b", "broken-key", uploadId,
            TestContext.Current.CancellationToken);
        await resolve.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task UploadPart_WritesPartFileAndJournal_EtagQuoted()
    {
        // Arrange
        var uploadId = await Store.CreateMultipartUploadAsync("b", "up-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var payload = Encoding.UTF8.GetBytes("part-one");

        // Act
        var result = await Store.UploadPartAsync("b", "up-key", uploadId, 1,
            new MemoryStream(payload), payload.Length, TestContext.Current.CancellationToken);

        // Assert: ETag = md5(payload) hex В КАВЫЧКАХ (P8); part.1 на диске; запись parts.json
        var md5 = Convert.ToHexString(System.Security.Cryptography.MD5.HashData(payload)).ToLowerInvariant();
        result.ETag.Should().Be("\"" + md5 + "\"");
        result.LastModified.Should().Be(TestVectors.FixedTime);
        var uploadDir = MultipartJournals.UploadDirPath(
            MultipartJournals.KeyDir(Volume.MultipartDir, "b", "up-key"), uploadId);
        File.Exists(Path.Combine(uploadDir, "part.1")).Should().BeTrue();
        MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(uploadDir))
            .Should().ContainSingle().Which.Should().Match<MultipartJournals.PartJournalEntry>(p =>
                p.PartNumber == 1 && p.ETag == md5 && p.Size == payload.Length);
        // tmp-файл не остался
        File.Exists(Path.Combine(uploadDir, MultipartJournals.PartTmpFileName(1))).Should().BeFalse();
    }

    [Fact]
    public async Task UploadPart_SamePartNumber_LastWriterWins()
    {
        // Arrange
        var uploadId = await Store.CreateMultipartUploadAsync("b", "lw-key", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Act: две записи одного номера (первая больше)
        var first = Encoding.UTF8.GetBytes("first-version-of-part");
        var second = Encoding.UTF8.GetBytes("2nd");
        await Store.UploadPartAsync("b", "lw-key", uploadId, 1, new MemoryStream(first),
            first.Length, TestContext.Current.CancellationToken);
        await Store.UploadPartAsync("b", "lw-key", uploadId, 1, new MemoryStream(second),
            second.Length, TestContext.Current.CancellationToken);

        // Assert: файл и журнал — от последней записи (согласованная пара)
        var uploadDir = MultipartJournals.UploadDirPath(
            MultipartJournals.KeyDir(Volume.MultipartDir, "b", "lw-key"), uploadId);
        new FileInfo(Path.Combine(uploadDir, "part.1")).Length.Should().Be(second.Length);
        MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(uploadDir))
            .Should().ContainSingle().Which.Size.Should().Be(second.Length);
    }

    [Fact]
    public async Task UploadPart_UnknownUpload_NoSuchUpload()
    {
        // Arrange / Act
        var act = async () => await Store.UploadPartAsync("b", "k", "no-such", 1,
            new MemoryStream([1]), 1, TestContext.Current.CancellationToken);

        // Assert: несуществующий uploadId — NoSuchUpload
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task UploadPart_AfterAbort_NoSuchUpload()
    {
        // Arrange
        var uploadId = await Store.CreateMultipartUploadAsync("b", "ab-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        await Store.AbortMultipartUploadAsync("b", "ab-key", uploadId, TestContext.Current.CancellationToken);

        // Act
        var act = async () => await Store.UploadPartAsync("b", "ab-key", uploadId, 1,
            new MemoryStream([1]), 1, TestContext.Current.CancellationToken);

        // Assert: после Abort загрузка мертва (канон 02 §5)
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task UploadPart_LengthMismatch_XlIntegrity()
    {
        // Arrange
        var uploadId = await Store.CreateMultipartUploadAsync("b", "len-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var uploadDir = MultipartJournals.UploadDirPath(
            MultipartJournals.KeyDir(Volume.MultipartDir, "b", "len-key"), uploadId);

        // Act: заявлено 10, тело 3 байта
        var act = async () => await Store.UploadPartAsync("b", "len-key", uploadId, 1,
            new MemoryStream([1, 2, 3]), 10, TestContext.Current.CancellationToken);

        // Assert: XlIntegrityException; ни part.1, ни записи журнала
        await act.Should().ThrowAsync<XlIntegrityException>();
        File.Exists(Path.Combine(uploadDir, "part.1")).Should().BeFalse();
        MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(uploadDir)).Should().BeEmpty();
    }

    [Fact]
    public async Task UploadPart_TwoParts_JournalSortedByNumber()
    {
        // Arrange
        var uploadId = await Store.CreateMultipartUploadAsync("b", "sort-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var uploadDir = MultipartJournals.UploadDirPath(
            MultipartJournals.KeyDir(Volume.MultipartDir, "b", "sort-key"), uploadId);

        // Act: загрузка №2, затем №1
        await Store.UploadPartAsync("b", "sort-key", uploadId, 2,
            new MemoryStream([2]), 1, TestContext.Current.CancellationToken);
        await Store.UploadPartAsync("b", "sort-key", uploadId, 1,
            new MemoryStream([1]), 1, TestContext.Current.CancellationToken);

        // Assert: журнал по возрастанию номеров [1, 2]
        MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(uploadDir))
            .Select(p => p.PartNumber).Should().Equal(1, 2);
    }

    [Fact]
    public async Task UploadPart_BrokenPartsJson_NoSuchUpload()
    {
        // Arrange: parts.json перезаписан мусором при живой записи uploads.json
        var uploadId = await Store.CreateMultipartUploadAsync("b", "bp-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        await Store.UploadPartAsync("b", "bp-key", uploadId, 1,
            new MemoryStream([1]), 1, TestContext.Current.CancellationToken);
        var uploadDir = MultipartJournals.UploadDirPath(
            MultipartJournals.KeyDir(Volume.MultipartDir, "b", "bp-key"), uploadId);
        File.WriteAllText(MultipartJournals.PartsJsonPath(uploadDir), "{broken");

        // Act: вторая часть в загрузку с битым журналом частей
        var act = async () => await Store.UploadPartAsync("b", "bp-key", uploadId, 2,
            new MemoryStream([2]), 1, TestContext.Current.CancellationToken);

        // Assert: NoSuchUpload — единообразно с Complete/ListParts (спека §4.1)
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchUpload);
    }
}
