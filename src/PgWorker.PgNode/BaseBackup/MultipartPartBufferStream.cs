using PgWorker.Backups;
using Shared.Core;

namespace PgWorker.PgNode.BaseBackup;

/// <summary>Поток-цель tar-writer'а с нарезкой на части multipart (spec t01
/// §3.4 п.2): заполняемый буфер части (16 МиБ) уходит в фоновую заливку с
/// ограничением 4 частей в полёте; память ограничена буфером части + копиями
/// частей в полёте (≤5×part), backpressure — естественный (Write ждёт слота).
/// Промежуточные части — ровно partSize (выше S3-минимума 5 МиБ), короче 5 МиБ
/// бывает только финальный остаток, выгружаемый CompleteAsync. Ошибка любой
/// части запоминается первой и возвращается всеми последующими Write/Complete.</summary>
public sealed class MultipartPartBufferStream : Stream
{
    // Максимум частей в полёте (память ≤ 5×part, spec §3.4 п.2).
    private const int MaxPartsInFlight = 4;

    private readonly IMultipartUploadSession _session;
    private readonly int _partSize;
    private readonly byte[] _buffer;
    private readonly SemaphoreSlim _inFlight = new(MaxPartsInFlight);
    private readonly List<(int PartNumber, Task<Result<string>> Task)> _parts = [];

    private int _offset;
    private int _nextPartNumber = 1;
    private long _bytesDone;
    private long _partsDone;
    private Exception? _error;
    private bool _completed;

    public MultipartPartBufferStream(
        IMultipartUploadSession session, int partSizeBytes = 16 * 1024 * 1024)
    {
        _session = session;
        _partSize = partSizeBytes;
        _buffer = new byte[partSizeBytes];
    }

    /// <summary>Прочитано байт (прогресс операции: bytes_done).</summary>
    public long BytesDone => Interlocked.Read(ref _bytesDone);

    /// <summary>Успешно залитых частей (прогресс операции: parts_done).</summary>
    public long PartsDone => Interlocked.Read(ref _partsDone);

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
        => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count)
        => WriteAsync(buffer.AsMemory(offset, count), CancellationToken.None)
            .AsTask().GetAwaiter().GetResult();

    public override async ValueTask WriteAsync(
        ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ThrowIfFailed();
        while (buffer.Length > 0)
        {
            var count = Math.Min(_partSize - _offset, buffer.Length);
            buffer[..count].CopyTo(_buffer.AsMemory(_offset));
            _offset += count;
            Interlocked.Add(ref _bytesDone, count);
            buffer = buffer[count..];
            if (_offset == _partSize)
                await FlushPartAsync(_partSize, cancellationToken);
        }
    }

    public override void Flush()
    {
        // Байты уже в буфере/полёте — части заливаются по заполнению.
    }

    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>Дождаться частей в полёте → выгрузить финальный остаток (если
    /// есть) → CompleteMultipartUpload; наружу — Result объекта (ETag).</summary>
    public async Task<Result<string>> CompleteAsync(CancellationToken ct)
    {
        if (_completed)
            throw new InvalidOperationException("multipart upload is already completed");
        // Ошибка части — Complete возвращает её как отказ (Write — бросает).
        if (_error is { } earlier)
            return Result<string>.Failed(new ApplicationException(earlier.Message, earlier));
        if (_offset > 0)
        {
            var tail = _offset;
            _offset = 0;
            await FlushPartAsync(tail, ct);
        }

        var partETags = new Dictionary<int, string>();
        foreach (var (partNumber, task) in _parts)
        {
            var result = await task;
            if (!result.IsSuccess)
                return Result<string>.Failed(
                    new ApplicationException(result.Error!.Message));
            partETags[partNumber] = result.Value;
        }

        if (_error is { } failed)
            return Result<string>.Failed(new ApplicationException(failed.Message, failed));
        var completed = await _session.CompleteAsync(partETags, ct);
        if (completed.IsSuccess)
            _completed = true;
        return completed;
    }

    /// <summary>Best-effort Abort сессии (отказ endpoint'а/отмена): ошибки
    /// глотаются — операция всё равно уходит в FAILED своей причиной.</summary>
    public async Task AbortAsync()
    {
        try
        {
            await _session.AbortAsync(CancellationToken.None);
        }
        catch
        {
            // Отмена — уборка лучшем усилием: причина операции важнее.
        }
    }

    private async Task FlushPartAsync(int size, CancellationToken ct)
    {
        // Копия: буфер переиспользуется под следующую часть, а заливка
        // асинхронная (части в полёте живут независимо).
        var data = new byte[size];
        Buffer.BlockCopy(_buffer, 0, data, 0, size);
        _offset = 0;
        var partNumber = _nextPartNumber++;
        await _inFlight.WaitAsync(ct);
        _parts.Add((partNumber, UploadPartAsync(partNumber, data, ct)));
    }

    private async Task<Result<string>> UploadPartAsync(
        int partNumber, byte[] data, CancellationToken ct)
    {
        try
        {
            var result = await _session.UploadPartAsync(partNumber, data, ct);
            if (result.IsSuccess)
                Interlocked.Increment(ref _partsDone);
            else
                RememberError(new ApplicationException(result.Error!.Message));
            return result;
        }
        catch (Exception e)
        {
            RememberError(e);
            return Result<string>.Failed(new ApplicationException(e.Message, e));
        }
        finally
        {
            _inFlight.Release();
        }
    }

    private void RememberError(Exception error)
        => Interlocked.CompareExchange(ref _error, error, null);

    private void ThrowIfFailed()
    {
        if (_error is { } error)
            throw new ApplicationException($"multipart upload failed: {error.Message}", error);
    }
}
