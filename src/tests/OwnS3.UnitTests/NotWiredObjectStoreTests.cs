using OwnS3.Storage;

namespace OwnS3.UnitTests;

// Заглушка NotWiredObjectStore (spec §3.3): все методы без тела —
// ObjectStoreUnavailableException; методы с телом — drain до исключения.
public sealed class NotWiredObjectStoreTests
{
    private readonly NotWiredObjectStore _store = new();

    [Theory]
    [MemberData(nameof(BodylessMethods))]
    public async Task BodylessMethod_ThrowsUnavailable(Func<NotWiredObjectStore, Task> call)
    {
        // Arrange / Act
        var act = async () => await call(_store);

        // Assert: заглушка без хранилища — единый исход
        await act.Should().ThrowAsync<ObjectStoreUnavailableException>();
    }

    public static IEnumerable<object[]> BodylessMethods()
    {
        // 17 методов без тела (из 19 контракта; PutObject/UploadPart — с телом)
        yield return [new Func<NotWiredObjectStore, Task>(s => s.CreateBucketAsync("b", default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.DeleteBucketAsync("b", default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.BucketExistsAsync("b", default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.ListBucketsAsync(default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.GetObjectAsync("b", "k", null!, default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.HeadObjectAsync("b", "k", null!, default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.DeleteObjectAsync("b", "k", default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.DeleteObjectsAsync("b", ["k"], false, default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.CopyObjectAsync(null!, default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.GetObjectAttributesAsync("b", "k", [], null, null, null!, default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.ListObjectsAsync("b", null!, default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.CreateMultipartUploadAsync("b", "k", null!, default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.UploadPartCopyAsync(null!, default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.CompleteMultipartUploadAsync("b", "k", "u", [], default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.AbortMultipartUploadAsync("b", "k", "u", default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.ListPartsAsync("b", "k", "u", null, null, default))];
        yield return [new Func<NotWiredObjectStore, Task>(s => s.ListMultipartUploadsAsync("b", null!, default))];
    }

    [Fact]
    public async Task PutObject_DrainsBodyBeforeThrowing()
    {
        // Arrange: поток 3 МиБ под счётчиком прочитанных байт
        var body = new CountingStream(new MemoryStream(new byte[3 * 1024 * 1024]));

        // Act
        var act = async () => await _store.PutObjectAsync("b", "k", body, body.Length,
            new ObjectUploadMetadata("application/octet-stream", new Dictionary<string, string>()), default);

        // Assert: исключение заглушки, но тело дочитано (drain — сверка наблюдаема)
        await act.Should().ThrowAsync<ObjectStoreUnavailableException>();
        body.BytesRead.Should().Be(3 * 1024 * 1024);
    }

    [Fact]
    public async Task UploadPart_DrainsBodyBeforeThrowing()
    {
        // Arrange
        var body = new CountingStream(new MemoryStream(new byte[1024]));

        // Act
        var act = async () => await _store.UploadPartAsync("b", "k", "u", 1, body, body.Length, default);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreUnavailableException>();
        body.BytesRead.Should().Be(1024);
    }

    [Fact]
    public async Task PutObject_DrainToleratesEmptyStream()
    {
        // Arrange: пустой поток (Stream.Null) — drain не ломается
        // Act
        var act = async () => await _store.PutObjectAsync("b", "k", Stream.Null, 0,
            new ObjectUploadMetadata("application/octet-stream", new Dictionary<string, string>()), default);

        // Assert
        await act.Should().ThrowAsync<ObjectStoreUnavailableException>();
    }

    // Счётчик прочитанного — фиксация drain-семантики.
    private sealed class CountingStream(Stream inner) : Stream
    {
        public long BytesRead { get; private set; }

        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => inner.CanSeek;
        public override bool CanWrite => inner.CanWrite;
        public override long Length => inner.Length;
        public override long Position { get => inner.Position; set => inner.Position = value; }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var read = inner.Read(buffer, offset, count);
            BytesRead += read;
            return read;
        }

        public override void Flush() => inner.Flush();
        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);
        public override void SetLength(long value) => inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
    }
}
