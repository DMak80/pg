using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Бакетные операции XlObjectStore + BucketMetaStore: CRUD, исходы
// BucketAlreadyOwnedByYou/BucketNotEmpty/NoSuchBucket, сортировка ListBuckets.
public class XlObjectStoreBucketTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    private readonly XlVolume _volume;
    private readonly XlObjectStore _store;

    public XlObjectStoreBucketTests()
    {
        _volume = new XlVolume(_root, TimeProvider.System);
        _volume.Initialize();
        _store = new XlObjectStore(_volume, new FixedTimeProvider(TestVectors.FixedTime),
            NullLogger<XlObjectStore>.Instance);
    }

    public void Dispose()
    {
        // Teardown: temp-том удаляется при любом исходе
        if (Directory.Exists(_root))
            Directory.Delete(_root, recursive: true);
    }

    [Fact]
    public async Task CreateBucket_CreatesDirAndMeta_CreationDateFromBucketJson()
    {
        // Arrange / Act
        await _store.CreateBucketAsync("b", CancellationToken.None);

        // Assert: каталог + bucket.json с createdAt фиксированного TimeProvider
        Directory.Exists(Path.Combine(_root, "b")).Should().BeTrue();
        File.Exists(Path.Combine(_root, ".owns3.sys", "buckets", "b", "bucket.json")).Should().BeTrue();
        var listed = await _store.ListBucketsAsync(CancellationToken.None);
        listed.Should().ContainSingle(e => e.Name == "b")
            .Which.CreationDate.Should().Be(TestVectors.FixedTime);
    }

    [Fact]
    public async Task CreateBucket_Existing_BucketAlreadyOwnedByYou()
    {
        // Arrange
        await _store.CreateBucketAsync("b", CancellationToken.None);

        // Act
        var act = async () => await _store.CreateBucketAsync("b", CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.BucketAlreadyOwnedByYou);
    }

    [Fact]
    public async Task DeleteBucket_Missing_NoSuchBucket()
    {
        // Arrange / Act
        var act = async () => await _store.DeleteBucketAsync("b", CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.NoSuchBucket);
    }

    [Fact]
    public async Task DeleteBucket_Empty_Succeeds_RemovesDirAndMeta()
    {
        // Arrange
        await _store.CreateBucketAsync("b", CancellationToken.None);

        // Act
        await _store.DeleteBucketAsync("b", CancellationToken.None);

        // Assert: и каталог бакета, и служебная мета удалены
        Directory.Exists(Path.Combine(_root, "b")).Should().BeFalse();
        Directory.Exists(Path.Combine(_root, ".owns3.sys", "buckets", "b")).Should().BeFalse();
    }

    [Fact]
    public async Task DeleteBucket_WithObject_BucketNotEmpty()
    {
        // Arrange: объект собирается руками — ObjectDir + xl.meta
        await _store.CreateBucketAsync("b", CancellationToken.None);
        var objectDir = _store.ObjectDir("b", "k");
        Directory.CreateDirectory(objectDir);
        XlMetaFile.Write(objectDir, new XlMetaRecord(Guid.NewGuid(), 1,
            DateTimeOffset.FromUnixTimeMilliseconds(0), "d41d8cd98f00b204e9800998ecf8427e",
            "text/plain", new Dictionary<string, string>(), new Dictionary<string, string>(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));

        // Act
        var act = async () => await _store.DeleteBucketAsync("b", CancellationToken.None);

        // Assert: в поддереве есть xl.meta — бакет непуст
        await act.Should().ThrowAsync<ObjectStoreException>()
            .Where(ex => ex.Code == ObjectStoreErrorCode.BucketNotEmpty);
    }

    [Fact]
    public async Task BucketExists_TrueForRealBucket_FalseForSysDirAndMissing()
    {
        // Arrange
        await _store.CreateBucketAsync("b", CancellationToken.None);

        // Act / Assert: служебный каталог тома бакетом не является
        (await _store.BucketExistsAsync("b", CancellationToken.None)).Should().BeTrue();
        (await _store.BucketExistsAsync(".owns3.sys", CancellationToken.None)).Should().BeFalse();
        (await _store.BucketExistsAsync("missing", CancellationToken.None)).Should().BeFalse();
    }

    [Fact]
    public async Task ListBuckets_ExcludesSysDir_SortedByName_ReturnsCreationDates()
    {
        // Arrange: создание в НЕ-алфавитном порядке (z первым, a вторым)
        await _store.CreateBucketAsync("z-bucket", CancellationToken.None);
        await _store.CreateBucketAsync("a-bucket", CancellationToken.None);

        // Act
        var buckets = await _store.ListBucketsAsync(CancellationToken.None);

        // Assert: сортировка по имени (Utf8ByteOrder), служебный каталог скрыт,
        // CreationDate каждого — из bucket.json (фиксированный TimeProvider)
        buckets.Select(b => b.Name).Should().Equal("a-bucket", "z-bucket");
        buckets.Should().OnlyContain(b => b.CreationDate == TestVectors.FixedTime);
    }
}
