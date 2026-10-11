using System.Security.Cryptography;
using System.Text;
using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Коммит-результаты несут метаданные события репликации (t35, глава 07 §3):
// PutResult/CompleteResult отдают Size/ContentSha256Hex/LastModified без
// повторного чтения xl-меты — состав события object-put хендлер собирает из
// результата коммит-поинта.
public class CommitResultEventMetadataTests(StoreFixture fixture) : IClassFixture<StoreFixture>
{
    private static readonly ObjectUploadMetadata Meta =
        new("text/plain", new Dictionary<string, string> { ["k"] = "v" });

    private XlObjectStore Store => fixture.Store;

    private static string Sha256HexOf(byte[] payload) =>
        Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant();

    [Fact]
    public async Task PutObject_ResultCarriesEventMetadata()
    {
        // Arrange — объектный слой на temp-томе (StoreFixture)
        var payload = "replication-meta"u8.ToArray();

        // Act
        var result = await Store.PutObjectAsync("b", "event-meta", new MemoryStream(payload), payload.Length,
            Meta, TestContext.Current.CancellationToken);

        // Assert — результат несёт всё для события object-put без повторного чтения меты
        result.ETag.Should().NotBeNullOrEmpty();
        result.LastModified.Should().Be(TestVectors.FixedTime);
        result.Size.Should().Be(payload.Length);
        result.ContentSha256Hex.Should().Be(Sha256HexOf(payload));
    }

    [Fact]
    public async Task CompleteMultipartUpload_ResultCarriesEventMetadata()
    {
        // Arrange — составной объект из двух частей: первая ≥ MinPartSize (канон 02 §5),
        // вторая (последняя) малая
        var uploadId = await Store.CreateMultipartUploadAsync("b", "event-mp", Meta, "writer",
            TestContext.Current.CancellationToken);
        var p1 = new byte[5 * 1024 * 1024];
        var p2 = "part-two-payload"u8.ToArray();
        var e1 = await Store.UploadPartAsync("b", "event-mp", uploadId, 1, new MemoryStream(p1),
            p1.Length, TestContext.Current.CancellationToken);
        var e2 = await Store.UploadPartAsync("b", "event-mp", uploadId, 2, new MemoryStream(p2),
            p2.Length, TestContext.Current.CancellationToken);

        // Act
        var result = await Store.CompleteMultipartUploadAsync("b", "event-mp", uploadId,
            [new PartEtag(1, e1.ETag), new PartEtag(2, e2.ETag)], TestContext.Current.CancellationToken);

        // Assert — CompleteResult несёт полные метаданные собранного объекта
        result.ETag.Should().NotBeNullOrEmpty();
        result.LastModified.Should().Be(TestVectors.FixedTime);
        result.Size.Should().Be(p1.Length + p2.Length);
        result.ContentSha256Hex.Should().Be(Sha256HexOf([.. p1, .. p2]));
    }

    [Fact]
    public async Task CopyObject_ResultCarriesEventMetadata()
    {
        // Arrange — источник для CopyObject (клиентский коммит той же природы)
        var payload = "copy-source"u8.ToArray();
        await Store.PutObjectAsync("b", "copy-src", new MemoryStream(payload), payload.Length,
            Meta, TestContext.Current.CancellationToken);

        // Act
        var result = await Store.CopyObjectAsync(new CopyRequest("b", "copy-src", "b", "copy-dst",
            ReplaceMetadata: false, NewMetadata: null, SourceConditions: null),
            TestContext.Current.CancellationToken);

        // Assert — PutResult копии несёт метаданные события нового объекта
        result.ETag.Should().NotBeNullOrEmpty();
        result.LastModified.Should().Be(TestVectors.FixedTime);
        result.Size.Should().Be(payload.Length);
        result.ContentSha256Hex.Should().Be(Sha256HexOf(payload));
    }
}
