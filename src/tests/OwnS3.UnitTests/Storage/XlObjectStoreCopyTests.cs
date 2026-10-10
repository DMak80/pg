using System.Text;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// CopyObject (хардлинк+fallback, COPY/REPLACE, source-conditional 412,
// переживание удаления источника) + GetObjectAttributes (conditional 412/304,
// ETag quoted, синтетическая 1 часть).
public class XlObjectStoreCopyTests(StoreFixture fixture) : IClassFixture<StoreFixture>
{
    private static readonly ObjectUploadMetadata SourceMeta =
        new("text/csv", new Dictionary<string, string> { ["src"] = "yes" });
    private static readonly ObjectUploadMetadata ReplaceMeta =
        new("application/json", new Dictionary<string, string> { ["dst"] = "replaced" });

    private XlObjectStore Store => fixture.Store;
    private string Root => fixture.Root;

    private static readonly string HelloEtagQuoted = "\"5d41402abc4b2a76b9719d911017c592\"";

    private static ObjectConditions IfMatch(string value) => new(value, null, null, null);
    private static ObjectConditions IfNoneMatch(string value) => new(null, value, null, null);

    private async Task<string> ReadBodyAsync(string bucket, string key)
    {
        var content = await Store.GetObjectAsync(bucket, key, new ObjectReadOptions(null, null, null),
            TestContext.Current.CancellationToken);
        using var reader = new StreamReader(content.Body);
        return await reader.ReadToEndAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Copy_ContentAndInheritedMetadata_CopyDirective()
    {
        // Arrange
        await Store.PutObjectAsync("b", "copy-src", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);

        // Act
        var result = await Store.CopyObjectAsync(new CopyRequest("b", "copy-src", "b", "copy-dst",
            ReplaceMetadata: false, NewMetadata: null, SourceConditions: null),
            TestContext.Current.CancellationToken);

        // Assert: содержимое скопировано; метаданные источника; ETag наследован
        // (P8, в кавычках); LastModified — modTime новой записи
        (await ReadBodyAsync("b", "copy-dst")).Should().Be("hello");
        result.ETag.Should().Be(HelloEtagQuoted);
        result.LastModified.Should().Be(TestVectors.FixedTime);
        var dst = await Store.HeadObjectAsync("b", "copy-dst", null, TestContext.Current.CancellationToken);
        dst.Metadata.ContentType.Should().Be("text/csv");
        dst.Metadata.UserMetadata.Should().ContainKey("src").WhoseValue.Should().Be("yes");
    }

    [Fact]
    public async Task Copy_ReplaceDirective_UsesNewMetadata()
    {
        // Arrange
        await Store.PutObjectAsync("b", "repl-src", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);

        // Act
        await Store.CopyObjectAsync(new CopyRequest("b", "repl-src", "b", "repl-dst",
            ReplaceMetadata: true, NewMetadata: ReplaceMeta, SourceConditions: null),
            TestContext.Current.CancellationToken);

        // Assert
        var dst = await Store.HeadObjectAsync("b", "repl-dst", null, TestContext.Current.CancellationToken);
        dst.Metadata.ContentType.Should().Be("application/json");
        dst.Metadata.UserMetadata.Should().ContainKey("dst").WhoseValue.Should().Be("replaced");
    }

    [Fact]
    public async Task Copy_SurvivesSourceDeletion()
    {
        // Arrange: хардлинк — inode жив, пока есть ссылки (Q2)
        await Store.PutObjectAsync("b", "surv-src", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);
        await Store.CopyObjectAsync(new CopyRequest("b", "surv-src", "b", "surv-dst",
            false, null, null), TestContext.Current.CancellationToken);

        // Act
        await Store.DeleteObjectAsync("b", "surv-src", TestContext.Current.CancellationToken);

        // Assert: копия читается без ошибок целостности
        (await ReadBodyAsync("b", "surv-dst")).Should().Be("hello");
    }

    [Fact]
    public async Task Copy_FallbackToByteCopy_Works()
    {
        // Arrange: хук принудительно валит хардлинк — fallback-ветка
        await Store.PutObjectAsync("b", "fb-src", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);
        Store.HardLinkProbe = (_, _) => false;

        // Act
        try
        {
            await Store.CopyObjectAsync(new CopyRequest("b", "fb-src", "b", "fb-dst",
                false, null, null), TestContext.Current.CancellationToken);
        }
        finally
        {
            Store.HardLinkProbe = null;
        }

        // Assert
        (await ReadBodyAsync("b", "fb-dst")).Should().Be("hello");
    }

    [Fact]
    public async Task Copy_HardLinkProbeTrue_TakesLinkBranch()
    {
        // Arrange: хук имитирует УСПЕШНЫЙ линк (фиксация линк-ветвления для
        // net11): CopyPart обязан вернуться сразу, БЕЗ побайтового копирования
        await Store.PutObjectAsync("b", "link-src", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);
        Store.HardLinkProbe = (_, _) => true;

        // Act
        try
        {
            await Store.CopyObjectAsync(new CopyRequest("b", "link-src", "b", "link-dst",
                false, null, null), TestContext.Current.CancellationToken);
        }
        finally
        {
            Store.HardLinkProbe = null;
        }

        // Assert: копия закоммичена, но part.1 НЕ создан копированием —
        // fallback-ветка не выполнялась (линк-имитация не пишет байтов)
        var dst = await Store.HeadObjectAsync("b", "link-dst", null, TestContext.Current.CancellationToken);
        dst.Metadata.Size.Should().Be(5);
        var objectDir = Path.Combine(Root, "b", "link-dst");
        var dataDir = Path.Combine(objectDir, XlMetaFile.Read(objectDir, out _).DataDirName);
        File.Exists(Path.Combine(dataDir, "part.1")).Should().BeFalse();
    }

    [Fact]
    public async Task Copy_SourceConditions_Fail412()
    {
        // Arrange
        await Store.PutObjectAsync("b", "cond-src", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);

        // Act: If-Match не совпал
        var act = async () => await Store.CopyObjectAsync(new CopyRequest("b", "cond-src", "b", "cond-dst",
            false, null, IfMatch("\"other\"")), TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.PreconditionFailed);

        // Act: If-None-Match СОВПАЛ — copy не GET: 412, не 304
        var act2 = async () => await Store.CopyObjectAsync(new CopyRequest("b", "cond-src", "b", "cond-dst",
            false, null, IfNoneMatch(HelloEtagQuoted)), TestContext.Current.CancellationToken);

        // Assert
        await act2.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.PreconditionFailed);
    }

    [Fact]
    public async Task Copy_MissingSource_NoSuchKey()
    {
        // Arrange / Act
        var act = async () => await Store.CopyObjectAsync(new CopyRequest("b", "no-such-src", "b", "dst",
            false, null, null), TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.NoSuchKey);
    }

    [Fact]
    public async Task Copy_MissingDestBucket_NoSuchBucket()
    {
        // Arrange
        await Store.PutObjectAsync("b", "ok-src", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);

        // Act
        var act = async () => await Store.CopyObjectAsync(new CopyRequest("b", "ok-src", "missing", "dst",
            false, null, null), TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.NoSuchBucket);
    }

    [Fact]
    public async Task Copy_SourceTooLarge_EntityTooLarge()
    {
        // Arrange: поверх живого объекта пишется запись Size = 5 ГБ + 1
        await Store.PutObjectAsync("b", "huge-src", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);
        var objectDir = Path.Combine(Root, "b", "huge-src");
        var current = XlMetaFile.Read(objectDir, out _);
        XlMetaFile.Write(objectDir, current with { Size = 5L * 1024 * 1024 * 1024 + 1 });

        // Act
        var act = async () => await Store.CopyObjectAsync(new CopyRequest("b", "huge-src", "b", "huge-dst",
            false, null, null), TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.EntityTooLarge);
    }

    [Fact]
    public async Task Copy_SameKey_MetadataRewrite()
    {
        // Arrange: src == dest с REPLACE — коммит-перезапись самого себя
        await Store.PutObjectAsync("b", "self", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);

        // Act
        var result = await Store.CopyObjectAsync(new CopyRequest("b", "self", "b", "self",
            true, ReplaceMeta, null), TestContext.Current.CancellationToken);

        // Assert: содержимое то же, метаданные заменены
        (await ReadBodyAsync("b", "self")).Should().Be("hello");
        result.ETag.Should().Be(HelloEtagQuoted);
        var head = await Store.HeadObjectAsync("b", "self", null, TestContext.Current.CancellationToken);
        head.Metadata.ContentType.Should().Be("application/json");
    }

    // — GetObjectAttributes —

    private static readonly IReadOnlyList<ObjectAttributeName> AllAttributes =
        [ObjectAttributeName.ETag, ObjectAttributeName.ObjectSize,
         ObjectAttributeName.StorageClass, ObjectAttributeName.ObjectParts];

    [Fact]
    public async Task Attributes_ETagQuoted_SizeStorageClass_PartsSyntheticOne()
    {
        // Arrange
        await Store.PutObjectAsync("b", "attr", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);

        // Act
        var result = await Store.GetObjectAttributesAsync("b", "attr", AllAttributes,
            maxParts: null, partNumberMarker: null, conditions: null,
            TestContext.Current.CancellationToken);

        // Assert: unversioned-объект — ровно одна синтетическая часть (P8: ETag в кавычках)
        result.NotModifiedMetadata.Should().BeNull();
        var attrs = result.Attributes!;
        attrs.ETag.Should().Be(HelloEtagQuoted);
        attrs.ObjectSize.Should().Be(5);
        attrs.LastModified.Should().Be(TestVectors.FixedTime, "LastModified — modTime записи, не UtcNow");
        attrs.StorageClass.Should().Be("STANDARD");
        attrs.Parts!.PartsCount.Should().Be(1);
        attrs.Parts.MaxParts.Should().Be(1000);
        attrs.Parts.PartNumberMarker.Should().Be(0);
        attrs.Parts.IsTruncated.Should().BeFalse();
        attrs.Parts.Parts.Should().Equal((1, 5L));
    }

    [Fact]
    public async Task Attributes_MaxParts_PassedThrough()
    {
        // Arrange
        await Store.PutObjectAsync("b", "attr-mp", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);

        // Act
        var result = await Store.GetObjectAttributesAsync("b", "attr-mp", AllAttributes,
            maxParts: 7, partNumberMarker: 3, conditions: null, TestContext.Current.CancellationToken);

        // Assert
        result.Attributes!.Parts!.MaxParts.Should().Be(7);
        result.Attributes.Parts.PartNumberMarker.Should().Be(3);
    }

    [Fact]
    public async Task Attributes_Conditional_IfMatchFailed_Throws412()
    {
        // Arrange
        await Store.PutObjectAsync("b", "attr-412", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);

        // Act
        var act = async () => await Store.GetObjectAttributesAsync("b", "attr-412", AllAttributes,
            null, null, IfMatch("\"other\""), TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.PreconditionFailed);
    }

    [Fact]
    public async Task Attributes_Conditional_IfNoneMatchMatch_ReturnsNotModifiedMetadata()
    {
        // Arrange
        await Store.PutObjectAsync("b", "attr-304", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);

        // Act
        var result = await Store.GetObjectAttributesAsync("b", "attr-304", AllAttributes,
            null, null, IfNoneMatch(HelloEtagQuoted), TestContext.Current.CancellationToken);

        // Assert: 304-обёртка — Attributes пуст, Metadata несёт ETag в кавычках (P10)
        result.Attributes.Should().BeNull();
        result.NotModifiedMetadata!.ETag.Should().Be(HelloEtagQuoted);
        result.NotModifiedMetadata.LastModified.Should().Be(TestVectors.FixedTime);
    }

    [Fact]
    public async Task Attributes_Conditional_IfModifiedSince_NotModified()
    {
        // Arrange: объект не менялся после даты клиента
        await Store.PutObjectAsync("b", "attr-ims", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);

        // Act
        var result = await Store.GetObjectAttributesAsync("b", "attr-ims", AllAttributes,
            null, null, new ObjectConditions(null, null, TestVectors.FixedTime, null),
            TestContext.Current.CancellationToken);

        // Assert
        result.NotModifiedMetadata.Should().NotBeNull();
    }

    [Fact]
    public async Task Attributes_Conditional_IfUnmodifiedSince_Failed412()
    {
        // Arrange: объект менялся ПОСЛЕ границы клиента
        await Store.PutObjectAsync("b", "attr-ius", new MemoryStream(Encoding.UTF8.GetBytes("hello")),
            5, SourceMeta, TestContext.Current.CancellationToken);

        // Act
        var act = async () => await Store.GetObjectAttributesAsync("b", "attr-ius", AllAttributes,
            null, null, new ObjectConditions(null, null, null, TestVectors.FixedTime.AddSeconds(-10)),
            TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.PreconditionFailed);
    }

    [Fact]
    public async Task Attributes_Missing_NoSuchKey()
    {
        // Arrange / Act
        var act = async () => await Store.GetObjectAttributesAsync("b", "no-such-attr", AllAttributes,
            null, null, null, TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.NoSuchKey);
    }

    [Fact]
    public async Task Attributes_MissingBucket_NoSuchBucket()
    {
        // Arrange / Act
        var act = async () => await Store.GetObjectAttributesAsync("missing", "k", AllAttributes,
            null, null, null, TestContext.Current.CancellationToken);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.NoSuchBucket);
    }
}
