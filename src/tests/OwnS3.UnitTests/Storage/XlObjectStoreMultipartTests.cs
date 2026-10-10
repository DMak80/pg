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

    [Fact]
    public async Task ListParts_PaginationAndMarker()
    {
        // Arrange: части 1,2,3 — журнал по возрастанию
        var uploadId = await Store.CreateMultipartUploadAsync("b", "lp-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        for (var n = 1; n <= 3; n++)
        {
            var bytes = Encoding.UTF8.GetBytes("p" + n);
            await Store.UploadPartAsync("b", "lp-key", uploadId, n, new MemoryStream(bytes),
                bytes.Length, TestContext.Current.CancellationToken);
        }

        // Act: страница maxParts=2, затем продолжение за маркером
        var page1 = await Store.ListPartsAsync("b", "lp-key", uploadId, 2, null,
            UploadVisibility.AllUploads, TestContext.Current.CancellationToken);
        var page2 = await Store.ListPartsAsync("b", "lp-key", uploadId, 2, page1.NextPartNumberMarker,
            UploadVisibility.AllUploads, TestContext.Current.CancellationToken);

        // Assert: усечение, маркер = последний выданный номер; ETag в кавычках; порядок номеров
        page1.Parts.Select(p => p.PartNumber).Should().Equal(1, 2);
        page1.IsTruncated.Should().BeTrue();
        page1.NextPartNumberMarker.Should().Be(2);
        page2.Parts.Select(p => p.PartNumber).Should().Equal(3);
        page2.IsTruncated.Should().BeFalse();
        page1.Parts[0].ETag.Should().StartWith("\"").And.EndWith("\"");
        page1.Parts[0].LastModified.Should().Be(TestVectors.FixedTime);
    }

    [Fact]
    public async Task ListParts_UnknownUpload_NoSuchUpload()
    {
        // Arrange / Act
        var act = async () => await Store.ListPartsAsync("b", "k", "no-such", null, null,
            UploadVisibility.AllUploads, TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task ListParts_ForeignUploadForReadOnly_NoSuchUpload()
    {
        // Arrange: загрузка владельца writer
        var uploadId = await Store.CreateMultipartUploadAsync("b", "ro-key", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Act: read-only чужой доступ
        var act = async () => await Store.ListPartsAsync("b", "ro-key", uploadId, null, null,
            UploadVisibility.OwnedBy("reader"), TestContext.Current.CancellationToken);

        // Assert: неотличимо от несуществующей (канон 05 §3, Q2 спеки)
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task ListParts_BrokenPartsJson_NoSuchUpload()
    {
        // Arrange: parts.json перезаписан мусором при живой записи
        var uploadId = await Store.CreateMultipartUploadAsync("b", "blp-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        await Store.UploadPartAsync("b", "blp-key", uploadId, 1, new MemoryStream([1]), 1,
            TestContext.Current.CancellationToken);
        var uploadDir = MultipartJournals.UploadDirPath(
            MultipartJournals.KeyDir(Volume.MultipartDir, "b", "blp-key"), uploadId);
        File.WriteAllText(MultipartJournals.PartsJsonPath(uploadDir), "{broken");

        // Act
        var act = async () => await Store.ListPartsAsync("b", "blp-key", uploadId, null, null,
            UploadVisibility.AllUploads, TestContext.Current.CancellationToken);

        // Assert: битый журнал частей — загрузка недоступна (спека §4.1)
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task ListMultipartUploads_FiltersMarkersAndVisibility()
    {
        // Arrange: ИЗОЛИРОВАННЫЙ бакет кейса (листинг видит весь бакет — чужие
        // загрузки общего "b" фикстуры мешали бы); ключи a/x (ДВЕ загрузки writer),
        // a/y (writer + чужая admin), zzz; чужой бакет «other» — не попадает
        await Store.CreateBucketAsync("b-lmu-filters", TestContext.Current.CancellationToken);
        await Store.CreateBucketAsync("other", TestContext.Current.CancellationToken);
        var u1 = await Store.CreateMultipartUploadAsync("b-lmu-filters", "a/x", Meta, "writer",
            TestContext.Current.CancellationToken);
        var u1b = await Store.CreateMultipartUploadAsync("b-lmu-filters", "a/x", Meta, "writer",
            TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("b-lmu-filters", "a/y", Meta, "writer",
            TestContext.Current.CancellationToken);
        var foreign = await Store.CreateMultipartUploadAsync("b-lmu-filters", "a/y", Meta, "admin",
            TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("b-lmu-filters", "zzz", Meta, "writer",
            TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("other", "a/x", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Act 1: prefix "a/" + read-only(writer) видит только свои
        var page = await Store.ListMultipartUploadsAsync("b-lmu-filters", new UploadsQuery("a/", null, null, null,
            null, null, UploadVisibility.OwnedBy("writer")), TestContext.Current.CancellationToken);

        // Assert 1: свои загрузки своих ключей; чужая (admin) отфильтрована; порядок ключей байтовый.
        // Выдача — ТРИ записи: обе загрузки a/x (ключ повторяется) + a/y. Порядок ключей
        // детерминирован (М10: байтовая сортировка); НЕ детерминирован только порядок
        // u1/u1b внутри a/x (равные initiatedMs при FixedTimeProvider, доопределение по
        // uploadId) — их ассерт множеством, без зависимости от порядка
        page.Uploads.Select(u => u.Key).Should().Equal("a/x", "a/x", "a/y");
        page.Uploads.Where(u => u.Key == "a/x").Select(u => u.UploadId)
            .Should().Contain(u1).And.Contain(u1b);
        page.Uploads.Select(u => u.UploadId).Should().NotContain(foreign); // фильтр видимости (М9)
        page.CommonPrefixes.Should().BeEmpty();

        // Act 2: та же выборка под read-write/admin — ВСЕ загрузки бакета (§9.9, позитивная ветка)
        var allPage = await Store.ListMultipartUploadsAsync("b-lmu-filters", new UploadsQuery("a/", null, null, null,
            null, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken);

        // Assert 2: четыре записи — обе a/x, обе a/y (writer + чужая foreign); порядок ключей
        // детерминирован, порядок одно-ключевых загрузок — нет (см. Assert 1)
        allPage.Uploads.Should().HaveCount(4);
        allPage.Uploads.Select(u => u.Key).Should().Equal("a/x", "a/x", "a/y", "a/y");
        allPage.Uploads.Select(u => u.UploadId).Should().Contain(foreign); // admin-загрузка видна (§9.9)
    }

    [Fact]
    public async Task ListMultipartUploads_KeyMarkerAndUploadIdMarker_ContinuesAfterPair()
    {
        // Arrange: ключи k1 (две загрузки), k2 — маркер-пара внутри k1
        await Store.CreateBucketAsync("b-lmu-markers", TestContext.Current.CancellationToken);
        var first = await Store.CreateMultipartUploadAsync("b-lmu-markers", "k1", Meta, "writer",
            TestContext.Current.CancellationToken);
        var second = await Store.CreateMultipartUploadAsync("b-lmu-markers", "k1", Meta, "writer",
            TestContext.Current.CancellationToken);
        var third = await Store.CreateMultipartUploadAsync("b-lmu-markers", "k2", Meta, "writer",
            TestContext.Current.CancellationToken);
        var sorted = new[] { first, second }.OrderBy(id => id, StringComparer.Ordinal).ToList(); // доопределение М10

        // Act: продолжение строго после пары (k1, ПЕРВАЯ по порядку выдачи)
        var page = await Store.ListMultipartUploadsAsync("b-lmu-markers", new UploadsQuery(null, null, "k1",
            sorted[0], null, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken);

        // Assert: остаток k1 (вторая по порядку) + k2 — БЕЗ повторов и пропусков
        page.Uploads.Select(u => u.UploadId).Should().Equal([sorted[1], third]);

        // Act 2: маркер по ключу без uploadId — все ключи строго после k1
        var afterKey = await Store.ListMultipartUploadsAsync("b-lmu-markers", new UploadsQuery(null, null, "k1",
            null, null, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken);

        // Assert 2: только k2
        afterKey.Uploads.Select(u => u.Key).Should().Equal("k2");
    }

    [Fact]
    public async Task ListMultipartUploads_Delimiter_CommonPrefixesCountedInMax()
    {
        // Arrange: изолированный бакет кейса; ключи a/1, a/2, c (delimiter="/")
        await Store.CreateBucketAsync("b-lmu-delim", TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("b-lmu-delim", "a/1", Meta, "writer", TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("b-lmu-delim", "a/2", Meta, "writer", TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("b-lmu-delim", "c", Meta, "writer", TestContext.Current.CancellationToken);

        // Act
        var page = await Store.ListMultipartUploadsAsync("b-lmu-delim", new UploadsQuery(null, "/", null, null,
            2, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken);

        // Assert: CP "a/" + ключ "c" = 2 позиции; усечения нет
        page.CommonPrefixes.Select(p => p.Prefix).Should().Equal("a/");
        page.Uploads.Select(u => u.Key).Should().Equal("c");
        page.IsTruncated.Should().BeFalse();
    }

    [Fact]
    public async Task ListMultipartUploads_CpMarkerPagination_NoDuplicatePrefixes()
    {
        // Arrange: изолированный бакет кейса; ключи a/1, a/2, c; delimiter "/";
        // страница из ОДНОЙ позиции — первая отдаёт только CP "a/" и усекается
        // с NextKeyMarker = сам префикс
        await Store.CreateBucketAsync("b-lmu-cpm", TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("b-lmu-cpm", "a/1", Meta, "writer", TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("b-lmu-cpm", "a/2", Meta, "writer", TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("b-lmu-cpm", "c", Meta, "writer", TestContext.Current.CancellationToken);

        // Act: страница 1 → продолжение по маркеру-CP (без upload-id-marker)
        var page1 = await Store.ListMultipartUploadsAsync("b-lmu-cpm", new UploadsQuery(null, "/", null, null,
            1, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken);
        var page2 = await Store.ListMultipartUploadsAsync("b-lmu-cpm", new UploadsQuery(null, "/",
            page1.NextKeyMarker, page1.NextUploadIdMarker, 1, null, UploadVisibility.AllUploads),
            TestContext.Current.CancellationToken);

        // Assert: страница 1 = CP "a/" + усечение, NextKeyMarker = "a/"; страница 2 —
        // БЕЗ дубля CP (ключи префикса "a/" пропущены префикс-фильтром М10), продолжение
        // ключом "c", полное
        page1.CommonPrefixes.Select(p => p.Prefix).Should().Equal("a/");
        page1.Uploads.Should().BeEmpty();
        page1.IsTruncated.Should().BeTrue();
        page1.NextKeyMarker.Should().Be("a/");
        page2.CommonPrefixes.Should().BeEmpty();
        page2.Uploads.Select(u => u.Key).Should().Equal("c");
        page2.IsTruncated.Should().BeFalse();
    }

    [Fact]
    public async Task ListMultipartUploads_BrokenJournal_SkippedWithWarning()
    {
        // Arrange: изолированный бакет кейса; два ключа; журнал одного бит
        await Store.CreateBucketAsync("b-lmu-broken", TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("b-lmu-broken", "good-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var badUploadId = await Store.CreateMultipartUploadAsync("b-lmu-broken", "bad-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var badKeyDir = MultipartJournals.KeyDir(Volume.MultipartDir, "b-lmu-broken", "bad-key");
        File.WriteAllText(MultipartJournals.UploadsJsonPath(badKeyDir), "{broken");

        // Act
        var page = await Store.ListMultipartUploadsAsync("b-lmu-broken", new UploadsQuery(null, null, null, null,
            null, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken);

        // Assert: битый ключ пропущен, здоровый листится (спека §4.1)
        page.Uploads.Select(u => u.UploadId).Should().NotContain(badUploadId);
        page.Uploads.Select(u => u.Key).Should().Equal("good-key");
    }

    [Fact]
    public async Task DeleteBucket_LiveUpload_BucketNotEmpty_AfterAbort_Ok()
    {
        // Arrange
        await Store.CreateBucketAsync("bb", TestContext.Current.CancellationToken);
        var uploadId = await Store.CreateMultipartUploadAsync("bb", "k", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Act / Assert: живая загрузка блокирует удаление
        var del = async () => await Store.DeleteBucketAsync("bb", TestContext.Current.CancellationToken);
        await del.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.BucketNotEmpty);
        await Store.AbortMultipartUploadAsync("bb", "k", uploadId, TestContext.Current.CancellationToken);
        await del.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Complete_WrongEtag_InvalidPart()
    {
        // Arrange: одна часть загружена, манифест с чужим ETag
        var uploadId = await Store.CreateMultipartUploadAsync("b", "ci-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var bytes = Encoding.UTF8.GetBytes("part");
        await Store.UploadPartAsync("b", "ci-key", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken);

        // Act
        var act = async () => await Store.CompleteMultipartUploadAsync("b", "ci-key", uploadId,
            [new PartEtag(1, "\"deadbeef\"")], TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.InvalidPart);
    }

    [Fact]
    public async Task Complete_MissingPartNumber_InvalidPart()
    {
        // Arrange: загружена только часть 1; манифест ссылается на номер 2
        var uploadId = await Store.CreateMultipartUploadAsync("b", "mn-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var bytes = Encoding.UTF8.GetBytes("part");
        await Store.UploadPartAsync("b", "mn-key", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken);

        // Act
        var act = async () => await Store.CompleteMultipartUploadAsync("b", "mn-key", uploadId,
            [new PartEtag(2, "\"whatever\"")], TestContext.Current.CancellationToken);

        // Assert: несуществующая часть манифеста — InvalidPart
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.InvalidPart);
    }

    [Fact]
    public async Task Complete_SmallPartNotLast_InvalidPart()
    {
        // Arrange: две части по 3 байта — первая < 5 МиБ и НЕ последняя
        var uploadId = await Store.CreateMultipartUploadAsync("b", "sp-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var first = Encoding.UTF8.GetBytes("aaa");
        var second = Encoding.UTF8.GetBytes("bbb");
        var e1 = await Store.UploadPartAsync("b", "sp-key", uploadId, 1, new MemoryStream(first),
            first.Length, TestContext.Current.CancellationToken);
        var e2 = await Store.UploadPartAsync("b", "sp-key", uploadId, 2, new MemoryStream(second),
            second.Length, TestContext.Current.CancellationToken);

        // Act
        var act = async () => await Store.CompleteMultipartUploadAsync("b", "sp-key", uploadId,
            [new PartEtag(1, e1.ETag), new PartEtag(2, e2.ETag)], TestContext.Current.CancellationToken);

        // Assert: первая часть манифеста < MinPartSize — InvalidPart (канон 02 §1)
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.InvalidPart);
    }

    [Fact]
    public async Task Complete_EtagWithoutQuotesAndUpperHex_Matches()
    {
        // Arrange: манифест без кавычек / в UPPER — нормализация сверки (канон 02 §5)
        var uploadId = await Store.CreateMultipartUploadAsync("b", "nq-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var bytes = Encoding.UTF8.GetBytes("one-small-part");
        var put = await Store.UploadPartAsync("b", "nq-key", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken);
        var normalized = put.ETag.Trim('"').ToUpperInvariant();

        // Act: Complete с ненормализованным ETag — нормализация, успешная сборка
        var result = await Store.CompleteMultipartUploadAsync("b", "nq-key", uploadId,
            [new PartEtag(1, normalized)], TestContext.Current.CancellationToken);

        // Assert: НЕ InvalidPart (нормализация сработала); составной ETag от реального etag
        var composite = "1-" + Convert.ToHexString(System.Security.Cryptography.MD5.HashData(
            Encoding.ASCII.GetBytes(put.ETag.Trim('"')))).ToLowerInvariant();
        result.ETag.Should().Be("\"" + composite + "\"");
        var head = await Store.HeadObjectAsync("b", "nq-key", null, TestContext.Current.CancellationToken);
        head.Metadata.Size.Should().Be(bytes.Length);
    }

    [Fact]
    public async Task Complete_UnknownUpload_NoSuchUpload()
    {
        // Arrange / Act
        var act = async () => await Store.CompleteMultipartUploadAsync("b", "k", "no-such",
            [new PartEtag(1, "\"x\"")], TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchUpload);
    }

    // Загрузка большой части (5 МиБ) с возвратом etag hex без кавычек.
    internal static async Task<string> UploadBigPartAsync(XlObjectStore store, string bucket, string key,
        string uploadId, int partNumber, int size, byte fill)
    {
        var bytes = PartBytes(size, fill);
        var result = await store.UploadPartAsync(bucket, key, uploadId, partNumber,
            new MemoryStream(bytes), bytes.Length, TestContext.Current.CancellationToken);
        return result.ETag.Trim('"');
    }

    [Fact]
    public async Task Complete_CommitsObject_CompositeEtagAndLayout()
    {
        // Arrange: 2 части: 5 МиБ + 3 байта
        var uploadId = await Store.CreateMultipartUploadAsync("b", "cc-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var etag1 = await UploadBigPartAsync(Store, "b", "cc-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
        var etag2 = await UploadBigPartAsync(Store, "b", "cc-key", uploadId, 2, 3, 0xBB);
        var manifest = new List<PartEtag> { new(1, etag1), new(2, etag2) };

        // Act
        var result = await Store.CompleteMultipartUploadAsync("b", "cc-key", uploadId, manifest,
            TestContext.Current.CancellationToken);

        // Assert: составной ETag «2-md5(concat)» В КАВЫЧКАХ; объект виден Get/Head/List;
        // xl.meta: Size/ContentType/UserMetadata; загрузка зачищена
        var concat = etag1 + etag2;
        var composite = "2-" + Convert.ToHexString(
            System.Security.Cryptography.MD5.HashData(Encoding.ASCII.GetBytes(concat))).ToLowerInvariant();
        result.ETag.Should().Be("\"" + composite + "\"");
        var head = await Store.HeadObjectAsync("b", "cc-key", null, TestContext.Current.CancellationToken);
        head.Metadata.ETag.Should().Be("\"" + composite + "\"");
        head.Metadata.Size.Should().Be(5 * 1024 * 1024 + 3);
        head.Metadata.ContentType.Should().Be("text/plain");
        head.Metadata.UserMetadata.Should().ContainKey("k").WhoseValue.Should().Be("v");
        var listed = await Store.ListObjectsAsync("b", new ListQuery(null, null, null, null, null, null, null, false, ListVariant.V1),
            TestContext.Current.CancellationToken);
        listed.Contents.Any(e => e.Key == "cc-key").Should().BeTrue();
        var keyDir = MultipartJournals.KeyDir(Volume.MultipartDir, "b", "cc-key");
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir)).Should().BeEmpty();
        Directory.Exists(MultipartJournals.UploadDirPath(keyDir, uploadId)).Should().BeFalse();
    }

    [Fact]
    public async Task Complete_CrashBeforeCommit_IdempotentRetryReusesParts()
    {
        // Arrange: загрузка 5 МиБ + 3 байта; хук сбоя после переноса, до коммита
        var uploadId = await Store.CreateMultipartUploadAsync("b", "cr-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var etag1 = await UploadBigPartAsync(Store, "b", "cr-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
        var etag2 = await UploadBigPartAsync(Store, "b", "cr-key", uploadId, 2, 3, 0xBB);
        var manifest = new List<PartEtag> { new(1, etag1), new(2, etag2) };
        Store.CompletePreCommitProbe = () => throw new IOException("crash before commit");

        // Act: сбойная попытка
        var crashed = async () => await Store.CompleteMultipartUploadAsync("b", "cr-key", uploadId,
            manifest, TestContext.Current.CancellationToken);
        await crashed.Should().ThrowAsync<IOException>();
        Store.CompletePreCommitProbe = null;

        // Assert до повтора: объект НЕ виден; загрузка жива (ListParts работает)
        var missing = async () => await Store.GetObjectAsync("b", "cr-key",
            new ObjectReadOptions(null, null, null), TestContext.Current.CancellationToken);
        await missing.Should().ThrowAsync<ObjectStoreException>().Where(e => e.Code == ObjectStoreErrorCode.NoSuchKey);
        var parts = await Store.ListPartsAsync("b", "cr-key", uploadId, null, null,
            UploadVisibility.AllUploads, TestContext.Current.CancellationToken);
        parts.Parts.Should().HaveCount(2);

        // Act: повторный Complete — идемпотентен (доиспользование переноса, тот же dataDir)
        var result = await Store.CompleteMultipartUploadAsync("b", "cr-key", uploadId, manifest,
            TestContext.Current.CancellationToken);
        var head = await Store.HeadObjectAsync("b", "cr-key", null, TestContext.Current.CancellationToken);
        head.Metadata.ETag.Should().Be(result.ETag);
        // dataDir попытки переиспользован: в каталоге объекта ровно один Guid-dataDir
        var objectDir = Path.Combine(Root, "b", XlPathEncoder.EncodePath("cr-key"));
        Directory.EnumerateDirectories(objectDir).Count().Should().Be(1);
    }

    [Fact]
    public async Task Complete_AfterSuccess_SecondComplete_NoSuchUpload()
    {
        // Arrange: успешная сборка из одной малой части (единственная — без лимита 5 МиБ)
        var uploadId = await Store.CreateMultipartUploadAsync("b", "sc-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var bytes = Encoding.UTF8.GetBytes("single");
        var put = await Store.UploadPartAsync("b", "sc-key", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken);
        var manifest = new List<PartEtag> { new(1, put.ETag) };
        await Store.CompleteMultipartUploadAsync("b", "sc-key", uploadId, manifest,
            TestContext.Current.CancellationToken);

        // Act: повторный Complete завершённой загрузки
        var second = async () => await Store.CompleteMultipartUploadAsync("b", "sc-key", uploadId, manifest,
            TestContext.Current.CancellationToken);

        // Assert: NoSuchUpload (канон 02 §5); объект жив
        await second.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.NoSuchUpload);
        var head = await Store.HeadObjectAsync("b", "sc-key", null, TestContext.Current.CancellationToken);
        head.Metadata.Size.Should().Be(bytes.Length);
    }

    [Fact]
    public async Task Complete_ExtraPartsNotInManifest_TrashedWithUploadDir()
    {
        // Arrange: части 1,2 (валидный манифест) + лишняя часть 3
        var uploadId = await Store.CreateMultipartUploadAsync("b", "ex-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var etag1 = await UploadBigPartAsync(Store, "b", "ex-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
        var etag2 = await UploadBigPartAsync(Store, "b", "ex-key", uploadId, 2, 3, 0xBB);
        await UploadBigPartAsync(Store, "b", "ex-key", uploadId, 3, 4, 0xCC);
        var manifest = new List<PartEtag> { new(1, etag1), new(2, etag2) };

        // Act
        await Store.CompleteMultipartUploadAsync("b", "ex-key", uploadId, manifest,
            TestContext.Current.CancellationToken);

        // Assert: объект = 2 части (лишняя часть 3 ушла в .trash с каталогом загрузки)
        var objectDir = Path.Combine(Root, "b", XlPathEncoder.EncodePath("ex-key"));
        var dataDir = Directory.EnumerateDirectories(objectDir).Single();
        Directory.EnumerateFiles(dataDir).Select(Path.GetFileName).OrderBy(n => n)
            .Should().Equal(["part.1", "part.2"]);
        var head = await Store.HeadObjectAsync("b", "ex-key", null, TestContext.Current.CancellationToken);
        head.Metadata.Size.Should().Be(5 * 1024 * 1024 + 3);
    }

    [Fact]
    public async Task Complete_NewObjectDir_PrefixesCreated()
    {
        // Arrange: ключ с вложенными сегментами «deep/nested/key»
        var uploadId = await Store.CreateMultipartUploadAsync("b", "deep/nested/key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var bytes = Encoding.UTF8.GetBytes("deep");
        var put = await Store.UploadPartAsync("b", "deep/nested/key", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken);

        // Act
        await Store.CompleteMultipartUploadAsync("b", "deep/nested/key", uploadId,
            [new PartEtag(1, put.ETag)], TestContext.Current.CancellationToken);

        // Assert: каталоги-префиксы созданы, объект читается
        var head = await Store.HeadObjectAsync("b", "deep/nested/key", null, TestContext.Current.CancellationToken);
        head.Metadata.Size.Should().Be(bytes.Length);
    }

    [Fact]
    public async Task Complete_OverwriteExistingObject_OldDataDirTrashed()
    {
        // Arrange: простой PUT до Complete поверх
        await Store.PutObjectAsync("b", "ov-key", new MemoryStream(Encoding.UTF8.GetBytes("old")),
            3, Meta, TestContext.Current.CancellationToken);
        var objectDir = Path.Combine(Root, "b", XlPathEncoder.EncodePath("ov-key"));
        var oldDataDir = Directory.EnumerateDirectories(objectDir).Single();
        var uploadId = await Store.CreateMultipartUploadAsync("b", "ov-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var bytes = Encoding.UTF8.GetBytes("new-object");
        var put = await Store.UploadPartAsync("b", "ov-key", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken);

        // Act: Complete перезаписывает существующий объект
        await Store.CompleteMultipartUploadAsync("b", "ov-key", uploadId,
            [new PartEtag(1, put.ETag)], TestContext.Current.CancellationToken);

        // Assert: старый dataDir в .trash (в каталоге объекта один новый dataDir);
        // xl.meta новый; содержимое — новое
        Directory.EnumerateDirectories(objectDir).Should().ContainSingle();
        Directory.Exists(oldDataDir).Should().BeFalse();
        var head = await Store.HeadObjectAsync("b", "ov-key", null, TestContext.Current.CancellationToken);
        head.Metadata.Size.Should().Be(bytes.Length);
    }

    [Fact]
    public async Task Complete_DoseUploadAfterCrash_Allowed()
    {
        // Arrange: сбойный Complete после переноса части 1; часть 2 ещё не загружена
        var uploadId = await Store.CreateMultipartUploadAsync("b", "du-key", Meta, "writer",
            TestContext.Current.CancellationToken);
        var etag1 = await UploadBigPartAsync(Store, "b", "du-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
        Store.CompletePreCommitProbe = () => throw new IOException("crash");
        var crashed = async () => await Store.CompleteMultipartUploadAsync("b", "du-key", uploadId,
            [new PartEtag(1, etag1)], TestContext.Current.CancellationToken);
        await crashed.Should().ThrowAsync<IOException>();
        Store.CompletePreCommitProbe = null;

        // Act: дозагрузка части 2 заново и успешная сборка
        var etag2 = await UploadBigPartAsync(Store, "b", "du-key", uploadId, 2, 3, 0xBB);
        var result = await Store.CompleteMultipartUploadAsync("b", "du-key", uploadId,
            new List<PartEtag> { new(1, etag1), new(2, etag2) }, TestContext.Current.CancellationToken);

        // Assert: объект собран (спека §2.3 — дозагрузка после сбоя разрешена)
        var head = await Store.HeadObjectAsync("b", "du-key", null, TestContext.Current.CancellationToken);
        head.Metadata.ETag.Should().Be(result.ETag);
        head.Metadata.Size.Should().Be(5 * 1024 * 1024 + 3);
    }
}
