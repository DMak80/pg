using System.Security.Cryptography;

namespace PgWorker.PgNode.Operations;

/// <summary>Прозрачная обёртка потока: каждый прочитанный байт уходит в
/// сквозной sha256 (verify сверяет его с etcd-полем `sha256`; restore/drill —
/// с полем команды до старта postgres). Только чтение; Dispose закрывает
/// внутренний поток.</summary>
internal sealed class HashingReadStream(Stream inner, IncrementalHash hash) : Stream
{
    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var read = inner.Read(buffer, offset, count);
        if (read > 0)
            hash.AppendData(buffer, offset, read);
        return read;
    }

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var read = await inner.ReadAsync(buffer, cancellationToken);
        if (read > 0)
            hash.AppendData(buffer.Span[..read]);
        return read;
    }

    public override void Flush()
    {
        // Только чтение.
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            inner.Dispose();
        base.Dispose(disposing);
    }

    public override ValueTask DisposeAsync() => inner.DisposeAsync();

    /// <summary>Дочитать поток до EOF (хвост tar-добивки входит в сквозной
    /// хэш объекта).</summary>
    public async Task DrainAsync(CancellationToken ct)
    {
        var buffer = new byte[64 * 1024];
        while (await ReadAsync(buffer, ct) > 0)
        {
            // Хвост — только в хэш.
        }
    }
}
