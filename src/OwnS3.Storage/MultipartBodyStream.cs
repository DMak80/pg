namespace OwnS3.Storage;

// Составное тело данных (канон 04 §1: порядок part.N = порядок байтов): лениво
// открывает файлы частей (FileShare.Read), переходит между ними при чтении.
// Полный объект: startOffset=0, length=Size; срез Range — смещение/длина среза.
public sealed class MultipartBodyStream : Stream
{
    private readonly IReadOnlyList<string> _partPaths;
    private FileStream? _current;
    private int _currentIndex;      // стартовая часть — вычислена конструктором
    private int _openedIndex = -1;  // индекс открытого файла; -1 — пока не открывали
    private long _positionInCurrent;
    private long _remaining;

    public MultipartBodyStream(IReadOnlyList<string> partPaths, long startOffset, long length)
    {
        _partPaths = partPaths;
        _remaining = length;
        // Стартовая часть и смещение В ней: startOffset ∈ [0, суммарный размер].
        // ВАЖНО: skip == size части — старт со СЛЕДУЮЩЕЙ части с позиции 0
        // (диапазон, начинающийся ровно на границе частей).
        var skip = startOffset;
        var index = 0;
        while (index < partPaths.Count)
        {
            var size = new FileInfo(partPaths[index]).Length;
            if (skip < size)
                break;                        // старт внутри части index
            skip -= size;                     // старт за этой частью (вкл. ровно на границе)
            index++;
        }
        _currentIndex = index;                // первое чтение откроет ровно эту часть
        _positionInCurrent = skip;
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _remaining;
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (_remaining <= 0)
            return 0;
        EnsureCurrentOpen();
        var read = _current!.Read(buffer, offset, (int)Math.Min(count, _remaining));
        if (read == 0)
            throw new EndOfStreamException("часть данных короче ожидаемого (изменилась на диске?)");
        _remaining -= read;
        return read;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        if (_remaining <= 0)
            return 0;
        EnsureCurrentOpen();
        var read = await _current!.ReadAsync(buffer[..(int)Math.Min(buffer.Length, _remaining)], cancellationToken);
        if (read == 0)
            throw new EndOfStreamException("часть данных короче ожидаемого (изменилась на диске?)");
        _remaining -= read;
        return read;
    }

    // Ленивое открытие стартовой части и переход к следующей при исчерпании текущей.
    // _openedIndex = -1 → ничего не открыто: первое открытие — РОВНО _currentIndex,
    // вычисленный конструктором (без инкремента); далее — последовательный переход.
    private void EnsureCurrentOpen()
    {
        if (_current is not null && _current.Position < _current.Length)
            return;
        _current?.Dispose();
        var next = _openedIndex < 0 ? _currentIndex : _openedIndex + 1;
        if (next >= _partPaths.Count)
            throw new EndOfStreamException("данные объекта исчерпаны раньше длины");
        _current = new FileStream(_partPaths[next], FileMode.Open, FileAccess.Read, FileShare.Read);
        if (_positionInCurrent > 0)
        {
            _current.Seek(_positionInCurrent, SeekOrigin.Begin);
            _positionInCurrent = 0;
        }
        _openedIndex = next;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _current?.Dispose();
        base.Dispose(disposing);
    }
}
