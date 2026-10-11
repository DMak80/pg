using OwnS3.Storage.Erasure;

namespace OwnS3.Storage;

// Составной поток данных EC-объекта: последовательность окон частей (по одной
// ErasureBlockReader на пересечение запроса с частью), bitrot-сверка и
// реконструкция — внутри ридера; агрегирует CorruptedShards для heal.
public sealed class ErasureObjectStream : Stream
{
    private readonly IReadOnlyList<ErasureBlockReader> _windows;
    private int _current;
    private bool _windowExhausted;

    public ErasureObjectStream(IReadOnlyList<ErasureBlockReader> windows)
    {
        _windows = windows;
    }

    // Позиции с несовпавшим bitrot (для пометки heal).
    public IReadOnlyList<int> CorruptedShards => _windows.SelectMany(w => w.CorruptedShards).Distinct().ToList();

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        while (_current < _windows.Count)
        {
            if (!_windowExhausted)
            {
                var read = await _windows[_current].ReadAsync(buffer, offset, count, cancellationToken);
                if (read > 0)
                    return read;
                _windowExhausted = true;
            }

            _current++;
            _windowExhausted = false;
        }

        return 0;
    }

    public override int Read(byte[] buffer, int offset, int count) =>
        ReadAsync(buffer, offset, count, CancellationToken.None).GetAwaiter().GetResult();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            foreach (var window in _windows)
                window.Dispose();
        }

        base.Dispose(disposing);
    }
}
