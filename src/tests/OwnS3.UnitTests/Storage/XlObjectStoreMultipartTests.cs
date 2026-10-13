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
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "mp-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;

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
        var actResult = (await Store.CreateMultipartUploadAsync("nob", "k", Meta, "writer",
            TestContext.Current.CancellationToken));
        // Assert — доменный отказ NoSuchBucket: Failed с кодом (конвенция 01 §4)
        (!actResult.IsSuccess).Should().BeTrue();
        actResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchBucket);
    }

    [Fact]
    public async Task Abort_TrashesDirAndRecord_SecondAbort_NoSuchUpload()
    {
        // Arrange
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "abort-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;

        // Act
        await Store.AbortMultipartUploadAsync("b", "abort-key", uploadId, TestContext.Current.CancellationToken);
        var second = async () => await Store.AbortMultipartUploadAsync("b", "abort-key", uploadId,
            TestContext.Current.CancellationToken);

        // Assert: повторный Abort — NoSuchUpload (канон 02 §5); журнал пуст; каталог в .trash
        var secondResult = (await Store.AbortMultipartUploadAsync("b", "abort-key", uploadId,
            TestContext.Current.CancellationToken));
        // Assert — доменный отказ NoSuchUpload: Failed с кодом (конвенция 01 §4)
        (!secondResult.IsSuccess).Should().BeTrue();
        secondResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchUpload);
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
        var actResult = (await Store.AbortMultipartUploadAsync("b", "k", "no-such-upload",
            TestContext.Current.CancellationToken));
        // Assert — доменный отказ NoSuchUpload: Failed с кодом (конвенция 01 §4)
        (!actResult.IsSuccess).Should().BeTrue();
        actResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task Create_TwoUploadsSameKey_BothListedInJournal()
    {
        // Act: две параллельные загрузки одного ключа
        var first = (await Store.CreateMultipartUploadAsync("b", "two-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var second = (await Store.CreateMultipartUploadAsync("b", "two-key", Meta, "other-writer",
            TestContext.Current.CancellationToken)).Throw().Value;

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
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "broken-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var keyDir = MultipartJournals.KeyDir(Volume.MultipartDir, "b", "broken-key");
        File.WriteAllText(MultipartJournals.UploadsJsonPath(keyDir), "{broken");

        // Act: Create по ДРУГОМУ ключу не затронут битым журналом соседа
        var fresh = async () => await Store.CreateMultipartUploadAsync("b", "other-key", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Assert: создание живёт; загрузка с битым журналом разрешается как
        // NoSuchUpload (М2) — полный кейс UploadPart закрыт в Task 4
        await fresh.Should().NotThrowAsync();
        var resolveResult = await Store.AbortMultipartUploadAsync("b", "broken-key", uploadId,
            TestContext.Current.CancellationToken);
        // Assert — доменный отказ NoSuchUpload: Failed с кодом (конвенция 01 §4)
        (!resolveResult.IsSuccess).Should().BeTrue();
        resolveResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task UploadPart_WritesPartFileAndJournal_EtagQuoted()
    {
        // Arrange
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "up-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var payload = Encoding.UTF8.GetBytes("part-one");

        // Act
        var result = (await Store.UploadPartAsync("b", "up-key", uploadId, 1,
            new MemoryStream(payload), payload.Length, TestContext.Current.CancellationToken)).Throw().Value;

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
        // tmp-файлов не осталось (уникальные part.1.<guid>.tmp переименованы)
        Directory.EnumerateFiles(uploadDir, "part.1*")
            .Where(f => Path.GetFileName(f) != "part.1").Should().BeEmpty();
    }

    [Fact]
    public async Task UploadPart_SamePartNumber_LastWriterWins()
    {
        // Arrange
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "lw-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;

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
        var actResult = (await Store.UploadPartAsync("b", "k", "no-such", 1,
            new MemoryStream([1]), 1, TestContext.Current.CancellationToken));
        // Assert — доменный отказ NoSuchUpload: Failed с кодом (конвенция 01 §4)
        (!actResult.IsSuccess).Should().BeTrue();
        actResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task UploadPart_AfterAbort_NoSuchUpload()
    {
        // Arrange
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "ab-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        await Store.AbortMultipartUploadAsync("b", "ab-key", uploadId, TestContext.Current.CancellationToken);

        // Act
        var act = async () => await Store.UploadPartAsync("b", "ab-key", uploadId, 1,
            new MemoryStream([1]), 1, TestContext.Current.CancellationToken);

        // Assert: после Abort загрузка мертва (канон 02 §5)
        var actResult = (await Store.UploadPartAsync("b", "ab-key", uploadId, 1,
            new MemoryStream([1]), 1, TestContext.Current.CancellationToken));
        // Assert — доменный отказ NoSuchUpload: Failed с кодом (конвенция 01 §4)
        (!actResult.IsSuccess).Should().BeTrue();
        actResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task UploadPart_LengthMismatch_XlIntegrity()
    {
        // Arrange
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "len-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
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
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "sort-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
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
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "bp-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        await Store.UploadPartAsync("b", "bp-key", uploadId, 1,
            new MemoryStream([1]), 1, TestContext.Current.CancellationToken);
        var uploadDir = MultipartJournals.UploadDirPath(
            MultipartJournals.KeyDir(Volume.MultipartDir, "b", "bp-key"), uploadId);
        File.WriteAllText(MultipartJournals.PartsJsonPath(uploadDir), "{broken");

        // Act: вторая часть в загрузку с битым журналом частей
        var act = async () => await Store.UploadPartAsync("b", "bp-key", uploadId, 2,
            new MemoryStream([2]), 1, TestContext.Current.CancellationToken);

        // Assert: NoSuchUpload — единообразно с Complete/ListParts (спека §4.1)
        var actResult = (await Store.UploadPartAsync("b", "bp-key", uploadId, 2,
            new MemoryStream([2]), 1, TestContext.Current.CancellationToken));
        // Assert — доменный отказ NoSuchUpload: Failed с кодом (конвенция 01 §4)
        (!actResult.IsSuccess).Should().BeTrue();
        actResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task ListParts_PaginationAndMarker()
    {
        // Arrange: части 1,2,3 — журнал по возрастанию
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "lp-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        for (var n = 1; n <= 3; n++)
        {
            var bytes = Encoding.UTF8.GetBytes("p" + n);
            await Store.UploadPartAsync("b", "lp-key", uploadId, n, new MemoryStream(bytes),
                bytes.Length, TestContext.Current.CancellationToken);
        }

        // Act: страница maxParts=2, затем продолжение за маркером
        var page1 = (await Store.ListPartsAsync("b", "lp-key", uploadId, 2, null,
            UploadVisibility.AllUploads, TestContext.Current.CancellationToken)).Throw().Value;
        var page2 = (await Store.ListPartsAsync("b", "lp-key", uploadId, 2, page1.NextPartNumberMarker,
            UploadVisibility.AllUploads, TestContext.Current.CancellationToken)).Throw().Value;

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
        var actResult = (await Store.ListPartsAsync("b", "k", "no-such", null, null,
            UploadVisibility.AllUploads, TestContext.Current.CancellationToken));
        // Assert — доменный отказ NoSuchUpload: Failed с кодом (конвенция 01 §4)
        (!actResult.IsSuccess).Should().BeTrue();
        actResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task ListParts_ForeignUploadForReadOnly_NoSuchUpload()
    {
        // Arrange: загрузка владельца writer
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "ro-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;

        // Act: read-only чужой доступ
        var act = async () => await Store.ListPartsAsync("b", "ro-key", uploadId, null, null,
            UploadVisibility.OwnedBy("reader"), TestContext.Current.CancellationToken);

        // Assert: неотличимо от несуществующей (канон 05 §3, Q2 спеки)
        var actResult = (await Store.ListPartsAsync("b", "ro-key", uploadId, null, null,
            UploadVisibility.OwnedBy("reader"), TestContext.Current.CancellationToken));
        // Assert — доменный отказ NoSuchUpload: Failed с кодом (конвенция 01 §4)
        (!actResult.IsSuccess).Should().BeTrue();
        actResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task ListParts_BrokenPartsJson_NoSuchUpload()
    {
        // Arrange: parts.json перезаписан мусором при живой записи
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "blp-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        await Store.UploadPartAsync("b", "blp-key", uploadId, 1, new MemoryStream([1]), 1,
            TestContext.Current.CancellationToken);
        var uploadDir = MultipartJournals.UploadDirPath(
            MultipartJournals.KeyDir(Volume.MultipartDir, "b", "blp-key"), uploadId);
        File.WriteAllText(MultipartJournals.PartsJsonPath(uploadDir), "{broken");

        // Act
        var act = async () => await Store.ListPartsAsync("b", "blp-key", uploadId, null, null,
            UploadVisibility.AllUploads, TestContext.Current.CancellationToken);

        // Assert: битый журнал частей — загрузка недоступна (спека §4.1)
        var actResult = (await Store.ListPartsAsync("b", "blp-key", uploadId, null, null,
            UploadVisibility.AllUploads, TestContext.Current.CancellationToken));
        // Assert — доменный отказ NoSuchUpload: Failed с кодом (конвенция 01 §4)
        (!actResult.IsSuccess).Should().BeTrue();
        actResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task ListMultipartUploads_FiltersMarkersAndVisibility()
    {
        // Arrange: ИЗОЛИРОВАННЫЙ бакет кейса (листинг видит весь бакет — чужие
        // загрузки общего "b" фикстуры мешали бы); ключи a/x (ДВЕ загрузки writer),
        // a/y (writer + чужая admin), zzz; чужой бакет «other» — не попадает
        await Store.CreateBucketAsync("b-lmu-filters", TestContext.Current.CancellationToken);
        await Store.CreateBucketAsync("other", TestContext.Current.CancellationToken);
        var u1 = (await Store.CreateMultipartUploadAsync("b-lmu-filters", "a/x", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var u1b = (await Store.CreateMultipartUploadAsync("b-lmu-filters", "a/x", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        await Store.CreateMultipartUploadAsync("b-lmu-filters", "a/y", Meta, "writer",
            TestContext.Current.CancellationToken);
        var foreign = (await Store.CreateMultipartUploadAsync("b-lmu-filters", "a/y", Meta, "admin",
            TestContext.Current.CancellationToken)).Throw().Value;
        await Store.CreateMultipartUploadAsync("b-lmu-filters", "zzz", Meta, "writer",
            TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("other", "a/x", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Act 1: prefix "a/" + read-only(writer) видит только свои
        var page = (await Store.ListMultipartUploadsAsync("b-lmu-filters", new UploadsQuery("a/", null, null, null,
            null, null, UploadVisibility.OwnedBy("writer")), TestContext.Current.CancellationToken)).Throw().Value;

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
        var allPage = (await Store.ListMultipartUploadsAsync("b-lmu-filters", new UploadsQuery("a/", null, null, null,
            null, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken)).Throw().Value;

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
        var first = (await Store.CreateMultipartUploadAsync("b-lmu-markers", "k1", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var second = (await Store.CreateMultipartUploadAsync("b-lmu-markers", "k1", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var third = (await Store.CreateMultipartUploadAsync("b-lmu-markers", "k2", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var sorted = new[] { first, second }.OrderBy(id => id, StringComparer.Ordinal).ToList(); // доопределение М10

        // Act: продолжение строго после пары (k1, ПЕРВАЯ по порядку выдачи)
        var page = (await Store.ListMultipartUploadsAsync("b-lmu-markers", new UploadsQuery(null, null, "k1",
            sorted[0], null, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken)).Throw().Value;

        // Assert: остаток k1 (вторая по порядку) + k2 — БЕЗ повторов и пропусков
        page.Uploads.Select(u => u.UploadId).Should().Equal([sorted[1], third]);

        // Act 2: маркер по ключу без uploadId — все ключи строго после k1
        var afterKey = (await Store.ListMultipartUploadsAsync("b-lmu-markers", new UploadsQuery(null, null, "k1",
            null, null, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken)).Throw().Value;

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
        var page = (await Store.ListMultipartUploadsAsync("b-lmu-delim", new UploadsQuery(null, "/", null, null,
            2, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken)).Throw().Value;

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
        var page1 = (await Store.ListMultipartUploadsAsync("b-lmu-cpm", new UploadsQuery(null, "/", null, null,
            1, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken)).Throw().Value;
        var page2 = (await Store.ListMultipartUploadsAsync("b-lmu-cpm", new UploadsQuery(null, "/",
            page1.NextKeyMarker, page1.NextUploadIdMarker, 1, null, UploadVisibility.AllUploads),
            TestContext.Current.CancellationToken)).Throw().Value;

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
        var badUploadId = (await Store.CreateMultipartUploadAsync("b-lmu-broken", "bad-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var badKeyDir = MultipartJournals.KeyDir(Volume.MultipartDir, "b-lmu-broken", "bad-key");
        File.WriteAllText(MultipartJournals.UploadsJsonPath(badKeyDir), "{broken");

        // Act
        var page = (await Store.ListMultipartUploadsAsync("b-lmu-broken", new UploadsQuery(null, null, null, null,
            null, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken)).Throw().Value;

        // Assert: битый ключ пропущен, здоровый листится (спека §4.1)
        page.Uploads.Select(u => u.UploadId).Should().NotContain(badUploadId);
        page.Uploads.Select(u => u.Key).Should().Equal("good-key");
    }

    [Fact]
    public async Task DeleteBucket_LiveUpload_BucketNotEmpty_AfterAbort_Ok()
    {
        // Arrange
        await Store.CreateBucketAsync("bb", TestContext.Current.CancellationToken);
        var uploadId = (await Store.CreateMultipartUploadAsync("bb", "k", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;

        // Act / Assert: живая загрузка блокирует удаление
        var delResult = await Store.DeleteBucketAsync("bb", TestContext.Current.CancellationToken);
        // Assert — доменный отказ BucketNotEmpty: Failed с кодом (конвенция 01 §4)
        (!delResult.IsSuccess).Should().BeTrue();
        delResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.BucketNotEmpty);
        await Store.AbortMultipartUploadAsync("bb", "k", uploadId, TestContext.Current.CancellationToken);
        // Assert — после Abort повторное удаление succeeds (Result.IsSuccess)
        var delRetry = (await Store.DeleteBucketAsync("bb", TestContext.Current.CancellationToken));
        delRetry.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task Complete_WrongEtag_InvalidPart()
    {
        // Arrange: одна часть загружена, манифест с чужим ETag
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "ci-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var bytes = Encoding.UTF8.GetBytes("part");
        await Store.UploadPartAsync("b", "ci-key", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken);

        // Act
        var act = async () => await Store.CompleteMultipartUploadAsync("b", "ci-key", uploadId,
            [new PartEtag(1, "\"deadbeef\"")], TestContext.Current.CancellationToken);

        // Assert
        var actResult = (await Store.CompleteMultipartUploadAsync("b", "ci-key", uploadId,
            [new PartEtag(1, "\"deadbeef\"")], TestContext.Current.CancellationToken));
        // Assert — доменный отказ InvalidPart: Failed с кодом (конвенция 01 §4)
        (!actResult.IsSuccess).Should().BeTrue();
        actResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.InvalidPart);
    }

    [Fact]
    public async Task Complete_MissingPartNumber_InvalidPart()
    {
        // Arrange: загружена только часть 1; манифест ссылается на номер 2
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "mn-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var bytes = Encoding.UTF8.GetBytes("part");
        await Store.UploadPartAsync("b", "mn-key", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken);

        // Act
        var act = async () => await Store.CompleteMultipartUploadAsync("b", "mn-key", uploadId,
            [new PartEtag(2, "\"whatever\"")], TestContext.Current.CancellationToken);

        // Assert: несуществующая часть манифеста — InvalidPart
        var actResult = (await Store.CompleteMultipartUploadAsync("b", "mn-key", uploadId,
            [new PartEtag(2, "\"whatever\"")], TestContext.Current.CancellationToken));
        // Assert — доменный отказ InvalidPart: Failed с кодом (конвенция 01 §4)
        (!actResult.IsSuccess).Should().BeTrue();
        actResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.InvalidPart);
    }

    [Fact]
    public async Task Complete_SmallPartNotLast_InvalidPart()
    {
        // Arrange: две части по 3 байта — первая < 5 МиБ и НЕ последняя
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "sp-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var first = Encoding.UTF8.GetBytes("aaa");
        var second = Encoding.UTF8.GetBytes("bbb");
        var e1 = (await Store.UploadPartAsync("b", "sp-key", uploadId, 1, new MemoryStream(first),
            first.Length, TestContext.Current.CancellationToken)).Throw().Value;
        var e2 = (await Store.UploadPartAsync("b", "sp-key", uploadId, 2, new MemoryStream(second),
            second.Length, TestContext.Current.CancellationToken)).Throw().Value;

        // Act
        var act = async () => await Store.CompleteMultipartUploadAsync("b", "sp-key", uploadId,
            [new PartEtag(1, e1.ETag), new PartEtag(2, e2.ETag)], TestContext.Current.CancellationToken);

        // Assert: первая часть манифеста < MinPartSize — InvalidPart (канон 02 §1)
        var actResult = (await Store.CompleteMultipartUploadAsync("b", "sp-key", uploadId,
            [new PartEtag(1, e1.ETag), new PartEtag(2, e2.ETag)], TestContext.Current.CancellationToken));
        // Assert — доменный отказ InvalidPart: Failed с кодом (конвенция 01 §4)
        (!actResult.IsSuccess).Should().BeTrue();
        actResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.InvalidPart);
    }

    [Fact]
    public async Task Complete_EtagWithoutQuotesAndUpperHex_Matches()
    {
        // Arrange: манифест без кавычек / в UPPER — нормализация сверки (канон 02 §5)
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "nq-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var bytes = Encoding.UTF8.GetBytes("one-small-part");
        var put = (await Store.UploadPartAsync("b", "nq-key", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken)).Throw().Value;
        var normalized = put.ETag.Trim('"').ToUpperInvariant();

        // Act: Complete с ненормализованным ETag — нормализация, успешная сборка
        var result = (await Store.CompleteMultipartUploadAsync("b", "nq-key", uploadId,
            [new PartEtag(1, normalized)], TestContext.Current.CancellationToken)).Throw().Value;

        // Assert: НЕ InvalidPart (нормализация сработала); составной ETag от реального etag
        var composite = "1-" + Convert.ToHexString(System.Security.Cryptography.MD5.HashData(
            Encoding.ASCII.GetBytes(put.ETag.Trim('"')))).ToLowerInvariant();
        result.ETag.Should().Be("\"" + composite + "\"");
        var head = (await Store.HeadObjectAsync("b", "nq-key", null, TestContext.Current.CancellationToken)).Throw().Value;
        head.Metadata.Size.Should().Be(bytes.Length);
    }

    [Fact]
    public async Task Complete_UnknownUpload_NoSuchUpload()
    {
        // Arrange / Act
        var act = async () => await Store.CompleteMultipartUploadAsync("b", "k", "no-such",
            [new PartEtag(1, "\"x\"")], TestContext.Current.CancellationToken);

        // Assert
        var actResult = (await Store.CompleteMultipartUploadAsync("b", "k", "no-such",
            [new PartEtag(1, "\"x\"")], TestContext.Current.CancellationToken));
        // Assert — доменный отказ NoSuchUpload: Failed с кодом (конвенция 01 §4)
        (!actResult.IsSuccess).Should().BeTrue();
        actResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchUpload);
    }

    // Загрузка большой части (5 МиБ) с возвратом etag hex без кавычек.
    internal static async Task<string> UploadBigPartAsync(XlObjectStore store, string bucket, string key,
        string uploadId, int partNumber, int size, byte fill)
    {
        var bytes = PartBytes(size, fill);
        var result = (await store.UploadPartAsync(bucket, key, uploadId, partNumber,
            new MemoryStream(bytes), bytes.Length, TestContext.Current.CancellationToken)).Throw().Value;
        return result.ETag.Trim('"');
    }

    [Fact]
    public async Task Complete_CommitsObject_CompositeEtagAndLayout()
    {
        // Arrange: 2 части: 5 МиБ + 3 байта
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "cc-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var etag1 = await UploadBigPartAsync(Store, "b", "cc-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
        var etag2 = await UploadBigPartAsync(Store, "b", "cc-key", uploadId, 2, 3, 0xBB);
        var manifest = new List<PartEtag> { new(1, etag1), new(2, etag2) };

        // Act
        var result = (await Store.CompleteMultipartUploadAsync("b", "cc-key", uploadId, manifest,
            TestContext.Current.CancellationToken)).Throw().Value;

        // Assert: составной ETag «2-md5(concat)» В КАВЫЧКАХ; объект виден Get/Head/List;
        // xl.meta: Size/ContentType/UserMetadata; загрузка зачищена
        var concat = etag1 + etag2;
        var composite = "2-" + Convert.ToHexString(
            System.Security.Cryptography.MD5.HashData(Encoding.ASCII.GetBytes(concat))).ToLowerInvariant();
        result.ETag.Should().Be("\"" + composite + "\"");
        var head = (await Store.HeadObjectAsync("b", "cc-key", null, TestContext.Current.CancellationToken)).Throw().Value;
        head.Metadata.ETag.Should().Be("\"" + composite + "\"");
        head.Metadata.Size.Should().Be(5 * 1024 * 1024 + 3);
        head.Metadata.ContentType.Should().Be("text/plain");
        head.Metadata.UserMetadata.Should().ContainKey("k").WhoseValue.Should().Be("v");
        var listed = (await Store.ListObjectsAsync("b", new ListQuery(null, null, null, null, null, null, null, false, ListVariant.V1),
            TestContext.Current.CancellationToken)).Throw().Value;
        listed.Contents.Any(e => e.Key == "cc-key").Should().BeTrue();
        var keyDir = MultipartJournals.KeyDir(Volume.MultipartDir, "b", "cc-key");
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir)).Should().BeEmpty();
        Directory.Exists(MultipartJournals.UploadDirPath(keyDir, uploadId)).Should().BeFalse();
    }

    [Fact]
    public async Task Complete_CrashBeforeCommit_IdempotentRetryReusesParts()
    {
        // Arrange: загрузка 5 МиБ + 3 байта; хук сбоя после переноса, до коммита
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "cr-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var etag1 = await UploadBigPartAsync(Store, "b", "cr-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
        var etag2 = await UploadBigPartAsync(Store, "b", "cr-key", uploadId, 2, 3, 0xBB);
        var manifest = new List<PartEtag> { new(1, etag1), new(2, etag2) };
        fixture.Drive.CompletePreCommitProbe = () => throw new IOException("crash before commit");

        // Act: сбойная попытка
        var crashed = async () => await Store.CompleteMultipartUploadAsync("b", "cr-key", uploadId,
            manifest, TestContext.Current.CancellationToken);
        await crashed.Should().ThrowAsync<IOException>();
        fixture.Drive.CompletePreCommitProbe = null;

        // Assert до повтора: объект НЕ виден; загрузка жива (ListParts работает)
        var missing = await Store.GetObjectAsync("b", "cr-key",
            new ObjectReadOptions(null, null), TestContext.Current.CancellationToken);
        (!missing.IsSuccess).Should().BeTrue();
        missing.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchKey);
        var parts = (await Store.ListPartsAsync("b", "cr-key", uploadId, null, null,
            UploadVisibility.AllUploads, TestContext.Current.CancellationToken)).Throw().Value;
        parts.Parts.Should().HaveCount(2);

        // Act: повторный Complete — идемпотентен (доиспользование переноса, тот же dataDir)
        var result = (await Store.CompleteMultipartUploadAsync("b", "cr-key", uploadId, manifest,
            TestContext.Current.CancellationToken)).Throw().Value;
        var head = (await Store.HeadObjectAsync("b", "cr-key", null, TestContext.Current.CancellationToken)).Throw().Value;
        head.Metadata.ETag.Should().Be(result.ETag);
        // dataDir попытки переиспользован: в каталоге объекта ровно один Guid-dataDir
        var objectDir = Path.Combine(Root, "b", XlPathEncoder.EncodePath("cr-key"));
        Directory.EnumerateDirectories(objectDir).Count().Should().Be(1);
    }

    [Fact]
    public async Task Complete_AfterSuccess_SecondComplete_NoSuchUpload()
    {
        // Arrange: успешная сборка из одной малой части (единственная — без лимита 5 МиБ)
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "sc-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var bytes = Encoding.UTF8.GetBytes("single");
        var put = (await Store.UploadPartAsync("b", "sc-key", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken)).Throw().Value;
        var manifest = new List<PartEtag> { new(1, put.ETag) };
        await Store.CompleteMultipartUploadAsync("b", "sc-key", uploadId, manifest,
            TestContext.Current.CancellationToken);

        // Act: повторный Complete завершённой загрузки
        var second = async () => await Store.CompleteMultipartUploadAsync("b", "sc-key", uploadId, manifest,
            TestContext.Current.CancellationToken);

        // Assert: NoSuchUpload (канон 02 §5); объект жив
        var secondResult = (await Store.CompleteMultipartUploadAsync("b", "sc-key", uploadId, manifest,
            TestContext.Current.CancellationToken));
        // Assert — доменный отказ NoSuchUpload: Failed с кодом (конвенция 01 §4)
        (!secondResult.IsSuccess).Should().BeTrue();
        secondResult.Error.Should().BeOfType<ObjectStoreException>()
            .Which.Code.Should().Be(ObjectStoreErrorCode.NoSuchUpload);
        var head = (await Store.HeadObjectAsync("b", "sc-key", null, TestContext.Current.CancellationToken)).Throw().Value;
        head.Metadata.Size.Should().Be(bytes.Length);
    }

    [Fact]
    public async Task Complete_ExtraPartsNotInManifest_TrashedWithUploadDir()
    {
        // Arrange: части 1,2 (валидный манифест) + лишняя часть 3
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "ex-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
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
        var head = (await Store.HeadObjectAsync("b", "ex-key", null, TestContext.Current.CancellationToken)).Throw().Value;
        head.Metadata.Size.Should().Be(5 * 1024 * 1024 + 3);
    }

    [Fact]
    public async Task Complete_NewObjectDir_PrefixesCreated()
    {
        // Arrange: ключ с вложенными сегментами «deep/nested/key»
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "deep/nested/key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var bytes = Encoding.UTF8.GetBytes("deep");
        var put = (await Store.UploadPartAsync("b", "deep/nested/key", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken)).Throw().Value;

        // Act
        await Store.CompleteMultipartUploadAsync("b", "deep/nested/key", uploadId,
            [new PartEtag(1, put.ETag)], TestContext.Current.CancellationToken);

        // Assert: каталоги-префиксы созданы, объект читается
        var head = (await Store.HeadObjectAsync("b", "deep/nested/key", null, TestContext.Current.CancellationToken)).Throw().Value;
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
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "ov-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var bytes = Encoding.UTF8.GetBytes("new-object");
        var put = (await Store.UploadPartAsync("b", "ov-key", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken)).Throw().Value;

        // Act: Complete перезаписывает существующий объект
        await Store.CompleteMultipartUploadAsync("b", "ov-key", uploadId,
            [new PartEtag(1, put.ETag)], TestContext.Current.CancellationToken);

        // Assert: старый dataDir в .trash (в каталоге объекта один новый dataDir);
        // xl.meta новый; содержимое — новое
        Directory.EnumerateDirectories(objectDir).Should().ContainSingle();
        Directory.Exists(oldDataDir).Should().BeFalse();
        var head = (await Store.HeadObjectAsync("b", "ov-key", null, TestContext.Current.CancellationToken)).Throw().Value;
        head.Metadata.Size.Should().Be(bytes.Length);
    }

    [Fact]
    public async Task Complete_DoseUploadAfterCrash_Allowed()
    {
        // Arrange: сбойный Complete после переноса части 1; часть 2 ещё не загружена
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "du-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var etag1 = await UploadBigPartAsync(Store, "b", "du-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
        fixture.Drive.CompletePreCommitProbe = () => throw new IOException("crash");
        var crashed = async () => await Store.CompleteMultipartUploadAsync("b", "du-key", uploadId,
            [new PartEtag(1, etag1)], TestContext.Current.CancellationToken);
        await crashed.Should().ThrowAsync<IOException>();
        fixture.Drive.CompletePreCommitProbe = null;

        // Act: дозагрузка части 2 заново и успешная сборка
        var etag2 = await UploadBigPartAsync(Store, "b", "du-key", uploadId, 2, 3, 0xBB);
        var result = (await Store.CompleteMultipartUploadAsync("b", "du-key", uploadId,
            new List<PartEtag> { new(1, etag1), new(2, etag2) }, TestContext.Current.CancellationToken)).Throw().Value;

        // Assert: объект собран (спека §2.3 — дозагрузка после сбоя разрешена)
        var head = (await Store.HeadObjectAsync("b", "du-key", null, TestContext.Current.CancellationToken)).Throw().Value;
        head.Metadata.ETag.Should().Be(result.ETag);
        head.Metadata.Size.Should().Be(5 * 1024 * 1024 + 3);
    }

    [Fact]
    public async Task Get_MultipartObject_BodyIsConcatOfParts()
    {
        // Arrange: объект из частей 5 МиБ (0xAA) + «ccc»
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "read-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var e1 = await UploadBigPartAsync(Store, "b", "read-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
        var e2 = await UploadBigPartAsync(Store, "b", "read-key", uploadId, 2, 3, (byte)'c');
        var result = (await Store.CompleteMultipartUploadAsync("b", "read-key", uploadId,
            [new PartEtag(1, e1), new PartEtag(2, e2)], TestContext.Current.CancellationToken)).Throw().Value;

        // Act
        var content = (await Store.GetObjectAsync("b", "read-key", new ObjectReadOptions(null, null),
            TestContext.Current.CancellationToken)).Throw().Value;
        using var body = content.Body;
        var bytes = new byte[body.Length];
        await body.ReadExactlyAsync(bytes, TestContext.Current.CancellationToken);

        // Assert: тело = конкатенация; хвост — «ccc»; conditional по составному ETag:
        // If-None-Match совпал → 304-семантика (NotModified-обёртка)
        bytes.Length.Should().Be(5 * 1024 * 1024 + 3);
        bytes[^3..].Should().Equal("ccc"u8.ToArray());
        var notModified = (await Store.GetObjectAsync("b", "read-key",
            new ObjectReadOptions(new ObjectConditions(null, result.ETag, null, null), null),
            TestContext.Current.CancellationToken)).Throw().Value;
        notModified.NotModified.Should().BeTrue();
    }

    [Fact]
    public async Task Get_MultipartObject_RangeAcrossPartBoundary_206()
    {
        // Arrange: части 5 МиБ + 3 байта; срез (5*1024*1024 - 2, 5*1024*1024 + 2) — через стык
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "rng-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var e1 = await UploadBigPartAsync(Store, "b", "rng-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
        var e2 = await UploadBigPartAsync(Store, "b", "rng-key", uploadId, 2, 3, (byte)'c');
        await Store.CompleteMultipartUploadAsync("b", "rng-key", uploadId,
            [new PartEtag(1, e1), new PartEtag(2, e2)], TestContext.Current.CancellationToken);

        // Act
        var start = 5 * 1024 * 1024 - 2;
        var content = (await Store.GetObjectAsync("b", "rng-key",
            new ObjectReadOptions(null, new ByteRange(start, start + 4)),
            TestContext.Current.CancellationToken)).Throw().Value;
        using var body = content.Body;
        var bytes = new byte[body.Length];
        await body.ReadExactlyAsync(bytes, TestContext.Current.CancellationToken);

        // Assert: 206, срез = 2 байта хвоста первой части + 3 второй; сверка пройдена ДО байтов
        content.Range.Should().NotBeNull();
        bytes.Should().Equal([(byte)0xAA, (byte)0xAA, (byte)'c', (byte)'c', (byte)'c']);
    }

    [Fact]
    public async Task Get_MultipartObject_RangeStartsExactlyAtPartBoundary_206()
    {
        // Arrange: части 5 МиБ + «ccc»; срез начинается РОВНО на границе частей —
        // регресс-кейс скетча MultipartBodyStream (skip == size части обязан
        // переводить старт в следующую часть с позиции 0, а не в первую часть)
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "bnd-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var e1 = await UploadBigPartAsync(Store, "b", "bnd-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
        var e2 = await UploadBigPartAsync(Store, "b", "bnd-key", uploadId, 2, 3, (byte)'c');
        await Store.CompleteMultipartUploadAsync("b", "bnd-key", uploadId,
            [new PartEtag(1, e1), new PartEtag(2, e2)], TestContext.Current.CancellationToken);

        // Act
        var start = 5 * 1024 * 1024;
        var content = (await Store.GetObjectAsync("b", "bnd-key",
            new ObjectReadOptions(null, new ByteRange(start, start + 2)),
            TestContext.Current.CancellationToken)).Throw().Value;
        using var body = content.Body;
        var bytes = new byte[body.Length];
        await body.ReadExactlyAsync(bytes, TestContext.Current.CancellationToken);

        // Assert: 206; срез = ТОЛЬКО вторая часть («ccc»), ни байта первой
        content.Range.Should().NotBeNull();
        bytes.Should().Equal("ccc"u8.ToArray());
    }

    [Fact]
    public async Task Get_MultipartObject_CorruptedPart_500BeforeBytes()
    {
        // Arrange: порча байта в part.1 закоммиченного объекта
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "cor-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var e1 = await UploadBigPartAsync(Store, "b", "cor-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
        var e2 = await UploadBigPartAsync(Store, "b", "cor-key", uploadId, 2, 3, (byte)'c');
        await Store.CompleteMultipartUploadAsync("b", "cor-key", uploadId,
            [new PartEtag(1, e1), new PartEtag(2, e2)], TestContext.Current.CancellationToken);
        var objectDir = Path.Combine(Root, "b", XlPathEncoder.EncodePath("cor-key"));
        var dataDir = Directory.EnumerateDirectories(objectDir).Single();
        var partPath = Path.Combine(objectDir, dataDir, "part.1");
        using (var fs = new FileStream(partPath, FileMode.Open, FileAccess.Write, FileShare.None))
        {
            fs.Seek(100, SeekOrigin.Begin);
            fs.WriteByte(0x00);
        }

        // Act / Assert: сверка по конкатенации ДО отдачи — XlIntegrityException (500), байтов нет
        var act = async () => await Store.GetObjectAsync("b", "cor-key",
            new ObjectReadOptions(null, null), TestContext.Current.CancellationToken);
        await act.Should().ThrowAsync<XlIntegrityException>();
    }

    [Fact]
    public async Task GetObjectAttributes_MultipartObject_RealPartsWithPagination()
    {
        // Arrange: 3 части (5 МиБ, 5 МиБ, 3 байта)
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "attr-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var e1 = await UploadBigPartAsync(Store, "b", "attr-key", uploadId, 1, 5 * 1024 * 1024, 0x01);
        var e2 = await UploadBigPartAsync(Store, "b", "attr-key", uploadId, 2, 5 * 1024 * 1024, 0x02);
        var e3 = await UploadBigPartAsync(Store, "b", "attr-key", uploadId, 3, 3, 0x03);
        var complete = (await Store.CompleteMultipartUploadAsync("b", "attr-key", uploadId,
            [new PartEtag(1, e1), new PartEtag(2, e2), new PartEtag(3, e3)], TestContext.Current.CancellationToken)).Throw().Value;

        // Act: страница maxParts=2, продолжение
        var page1 = (await Store.GetObjectAttributesAsync("b", "attr-key",
            [ObjectAttributeName.ObjectParts], 2, null, null, TestContext.Current.CancellationToken)).Throw().Value;
        var page1Attrs = ((GetAttributesOutcome.Full)page1).Attributes;
        var page2 = (await Store.GetObjectAttributesAsync("b", "attr-key",
            [ObjectAttributeName.ObjectParts], 2, page1Attrs.Parts!.NextPartNumberMarker, null,
            TestContext.Current.CancellationToken)).Throw().Value;
        var page2Attrs = ((GetAttributesOutcome.Full)page2).Attributes;

        // Assert: реальные части/размеры; PartsCount=3; составной ETag
        page1Attrs.Parts!.PartsCount.Should().Be(3);
        page1Attrs.Parts.IsTruncated.Should().BeTrue();
        page1Attrs.Parts.Parts.Should().Equal([(1, 5L * 1024 * 1024), (2, 5L * 1024 * 1024)]);
        page2Attrs.Parts!.Parts.Should().Equal([(3, 3L)]);
        page1Attrs.ETag.Should().Be(complete.ETag);
    }

    [Fact]
    public async Task GetObjectAttributes_SimplePut_SyntheticSinglePart()
    {
        // Arrange: простой PUT-объект — частный случай «ровно part.1» (М8)
        await Store.PutObjectAsync("b", "put-attr", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, Meta, TestContext.Current.CancellationToken);

        // Act
        var attributes = (await Store.GetObjectAttributesAsync("b", "put-attr",
            [ObjectAttributeName.ObjectParts], null, null, null, TestContext.Current.CancellationToken)).Throw().Value;

        // Assert: одна синтетическая часть (1, 5) — получена перечислением dataDir
        var putAttrs = ((GetAttributesOutcome.Full)attributes).Attributes;
        putAttrs.Parts!.PartsCount.Should().Be(1);
        putAttrs.Parts.Parts.Should().Equal([(1, 5L)]);
        putAttrs.ObjectSize.Should().Be(5);
    }

    [Fact]
    public async Task Copy_MultipartSource_InheritsCompositeEtagAndParts()
    {
        // Arrange: multipart-объект (5 МиБ + 3 байта)
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "cpsrc-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var e1 = await UploadBigPartAsync(Store, "b", "cpsrc-key", uploadId, 1, 5 * 1024 * 1024, 0xAA);
        var e2 = await UploadBigPartAsync(Store, "b", "cpsrc-key", uploadId, 2, 3, (byte)'c');
        var complete = (await Store.CompleteMultipartUploadAsync("b", "cpsrc-key", uploadId,
            [new PartEtag(1, e1), new PartEtag(2, e2)], TestContext.Current.CancellationToken)).Throw().Value;

        // Act
        var copy = (await Store.CopyObjectAsync(new CopyRequest("b", "cpsrc-key", "b", "cpdst-key",
            ReplaceMetadata: false, NewMetadata: null, SourceConditions: null),
            TestContext.Current.CancellationToken)).Throw().Value;

        // Assert: составной ETag/Size наследованы (arch-правка §3.5/Q3); тело копии читается;
        // dataDir копии содержит обе части
        copy.ETag.Should().Be(complete.ETag);
        var dst = (await Store.HeadObjectAsync("b", "cpdst-key", null, TestContext.Current.CancellationToken)).Throw().Value;
        dst.Metadata.ETag.Should().Be(complete.ETag);
        dst.Metadata.Size.Should().Be(5 * 1024 * 1024 + 3);
        var dstObjectDir = Path.Combine(Root, "b", XlPathEncoder.EncodePath("cpdst-key"));
        var dstDataDir = Directory.EnumerateDirectories(dstObjectDir).Single();
        Directory.EnumerateFiles(dstDataDir).Select(Path.GetFileName).OrderBy(n => n)
            .Should().Equal(["part.1", "part.2"]);
        var content = (await Store.GetObjectAsync("b", "cpdst-key",
            new ObjectReadOptions(null, new ByteRange(5 * 1024 * 1024, 5 * 1024 * 1024 + 2)),
            TestContext.Current.CancellationToken)).Throw().Value;
        using var body = content.Body;
        var tail = new byte[body.Length];
        await body.ReadExactlyAsync(tail, TestContext.Current.CancellationToken);
        tail.Should().Equal("ccc"u8.ToArray());
    }

    [Fact]
    public async Task ListParts_MaxPartsZero_EmptyPageNotTruncated()
    {
        // Arrange: загрузка с двумя частями
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "z0-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        await Store.UploadPartAsync("b", "z0-key", uploadId, 1, new MemoryStream([1]), 1,
            TestContext.Current.CancellationToken);
        await Store.UploadPartAsync("b", "z0-key", uploadId, 2, new MemoryStream([2]), 1,
            TestContext.Current.CancellationToken);

        // Act: max-parts=0 — валидный вход (App пропускает 0 в Storage)
        var page = (await Store.ListPartsAsync("b", "z0-key", uploadId, 0, null,
            UploadVisibility.AllUploads, TestContext.Current.CancellationToken)).Throw().Value;

        // Assert: пустая страница БЕЗ усечения — конвенция ListWalker t37
        page.Parts.Should().BeEmpty();
        page.IsTruncated.Should().BeFalse();
        page.NextPartNumberMarker.Should().BeNull();
    }

    [Fact]
    public async Task ListMultipartUploads_MaxUploadsZero_EmptyPageNotTruncated()
    {
        // Arrange: изолированный бакет с живой загрузкой
        await Store.CreateBucketAsync("b-z0-uploads", TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("b-z0-uploads", "k", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Act: max-uploads=0
        var page = (await Store.ListMultipartUploadsAsync("b-z0-uploads", new UploadsQuery(null, null,
            null, null, 0, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken)).Throw().Value;

        // Assert: пустая страница без усечения — нет вечного пагинационного цикла
        page.Uploads.Should().BeEmpty();
        page.CommonPrefixes.Should().BeEmpty();
        page.IsTruncated.Should().BeFalse();
        page.NextKeyMarker.Should().BeNull();
        page.NextUploadIdMarker.Should().BeNull();
    }

    [Fact]
    public async Task GetObjectAttributes_MaxPartsZero_EmptyPartsNotTruncated()
    {
        // Arrange: multipart-объект из одной малой части (единственная — без лимита 5 МиБ)
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "z0-attr", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var bytes = Encoding.UTF8.GetBytes("z");
        var put = (await Store.UploadPartAsync("b", "z0-attr", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken)).Throw().Value;
        await Store.CompleteMultipartUploadAsync("b", "z0-attr", uploadId,
            [new PartEtag(1, put.ETag)], TestContext.Current.CancellationToken);

        // Act: x-amz-max-parts=0
        var result = (await Store.GetObjectAttributesAsync("b", "z0-attr",
            [ObjectAttributeName.ObjectParts], 0, null, null, TestContext.Current.CancellationToken)).Throw().Value;

        // Assert: пустой список частей без усечения; PartsCount и ETag на месте
        var attrs = ((GetAttributesOutcome.Full)result).Attributes;
        attrs.Parts!.PartsCount.Should().Be(1);
        attrs.Parts.Parts.Should().BeEmpty();
        attrs.Parts.IsTruncated.Should().BeFalse();
        attrs.Parts.NextPartNumberMarker.Should().BeNull();
        attrs.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task ListMultipartUploads_KeyMarkerNotCpPrefix_KeepsStringPrefixedKeys()
    {
        // Arrange: изолированный бакет; ключи a/1, a/1x (строковый префикс «a/1»,
        // но НЕ CP-префикс «a/»), c; delimiter «/»
        await Store.CreateBucketAsync("b-km-cp", TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("b-km-cp", "a/1", Meta, "writer",
            TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("b-km-cp", "a/1x", Meta, "writer",
            TestContext.Current.CancellationToken);
        await Store.CreateMultipartUploadAsync("b-km-cp", "c", Meta, "writer",
            TestContext.Current.CancellationToken);

        // Act: key-marker «a/1» (не оканчивается на delimiter — НЕ маркер-CP):
        // выдача строго после пары, строковый префикс не выкидывает «a/1x»
        var page = (await Store.ListMultipartUploadsAsync("b-km-cp", new UploadsQuery(null, "/", "a/1",
            null, null, null, UploadVisibility.AllUploads), TestContext.Current.CancellationToken)).Throw().Value;

        // Assert: «a/1x» не потерян (свёрнут в CP «a/»), ключ «c» после; полнота 02 §5
        page.CommonPrefixes.Select(p => p.Prefix).Should().Equal("a/");
        page.Uploads.Select(u => u.Key).Should().Equal("c");
        page.IsTruncated.Should().BeFalse();
    }

    [Fact]
    public async Task UploadPart_ParallelSamePartNumber_BothSucceedConsistentJournal()
    {
        // Arrange: загрузка; две параллельные записи одного номера (уникальные tmp)
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "par-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var first = Encoding.UTF8.GetBytes("parallel-first-body");
        var second = Encoding.UTF8.GetBytes("2nd");
        var tasks = new[]
        {
            Store.UploadPartAsync("b", "par-key", uploadId, 1, new MemoryStream(first),
                first.Length, TestContext.Current.CancellationToken),
            Store.UploadPartAsync("b", "par-key", uploadId, 1, new MemoryStream(second),
                second.Length, TestContext.Current.CancellationToken),
        };

        // Act: обе завершаются — ни IOException от FileShare.None на общем tmp
        var results = (await Task.WhenAll(tasks)).Select(r => r.Throw().Value).ToArray();

        // Assert: обе PutResult валидны; журнал — согласованная пара ровно с ОДНОЙ
        // записью (last-writer-wins линеаризован rename'ом под _commitLock);
        // размер файла = размеру записи журнала; tmp-остатков нет
        var uploadDir = MultipartJournals.UploadDirPath(
            MultipartJournals.KeyDir(Volume.MultipartDir, "b", "par-key"), uploadId);
        var journal = MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(uploadDir));
        journal.Should().ContainSingle();
        var fileSize = new FileInfo(Path.Combine(uploadDir, "part.1")).Length;
        fileSize.Should().Be(journal[0].Size);
        new[] { first.Length, second.Length }.Should().Contain((int)fileSize);
        Directory.EnumerateFiles(uploadDir, "part.1*")
            .Where(f => Path.GetFileName(f) != "part.1").Should().BeEmpty();
        results.Should().OnlyContain(r => r.ETag.StartsWith("\""));
    }

    [Fact]
    public async Task Complete_BrokenAttemptMarker_NewAttemptRecovers()
    {
        // Arrange: живая загрузка с частью; attempt.json с НЕ-Guid dataDir (битый маркер)
        var uploadId = (await Store.CreateMultipartUploadAsync("b", "bam-key", Meta, "writer",
            TestContext.Current.CancellationToken)).Throw().Value;
        var bytes = Encoding.UTF8.GetBytes("bam");
        var put = (await Store.UploadPartAsync("b", "bam-key", uploadId, 1, new MemoryStream(bytes),
            bytes.Length, TestContext.Current.CancellationToken)).Throw().Value;
        var uploadDir = MultipartJournals.UploadDirPath(
            MultipartJournals.KeyDir(Volume.MultipartDir, "b", "bam-key"), uploadId);
        File.WriteAllText(MultipartJournals.AttemptJsonPath(uploadDir),
            """{"dataDir":"not-a-guid-at-all"}""");

        // Act: Complete обязан траковать битый маркер как отсутствующий (М7) —
        // новая попытка, а не перманентный 500 от Guid.Parse
        var result = (await Store.CompleteMultipartUploadAsync("b", "bam-key", uploadId,
            [new PartEtag(1, put.ETag)], TestContext.Current.CancellationToken)).Throw().Value;

        // Assert: объект собран новой попыткой; в каталоге объекта один Guid-dataDir
        var head = (await Store.HeadObjectAsync("b", "bam-key", null, TestContext.Current.CancellationToken)).Throw().Value;
        head.Metadata.ETag.Should().Be(result.ETag);
        head.Metadata.Size.Should().Be(bytes.Length);
        var objectDir = Path.Combine(Root, "b", XlPathEncoder.EncodePath("bam-key"));
        Directory.EnumerateDirectories(objectDir).Should().ContainSingle();
    }
}
