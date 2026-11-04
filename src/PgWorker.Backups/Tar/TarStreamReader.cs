using System.Text;

namespace PgWorker.Backups.Tar;

/// <summary>Потоковое чтение tar-архива (ustar + GNU longname + PAX):
/// заголовок 512 байт → данные Size байт → добивка до кратности 512.
/// Разрешение имени: PAX-поле `path=` (и `size=`) переопределяет следующую
/// запись, GNU longname (`L`) — имя следующей записи; иначе ustar
/// `prefix + "/" + name`. Валидация checksum заголовка и typeflag'а —
/// нарушение формата бросает <see cref="TarFormatException"/> (битый архив
/// не превращается в «тихо пустой» набор).</summary>
public sealed class TarStreamReader
{
    private const int HeaderSize = 512;
    private const int CopyBufferSize = 64 * 1024;

    private readonly Stream _source;

    // Состояние текущей отданной записи: сколько данных и добивки ещё
    // не прочитано из источника (дренаж — при следующем ReadHeaderAsync или
    // в Dispose потока данных).
    private long _entryRemaining;
    private long _paddingRemaining;
    private bool _dataOpened;

    public TarStreamReader(Stream source) => _source = source;

    /// <summary>Заголовок следующей ЗНАЧИМОЙ записи; null — нулевой заголовок
    /// (конец архива). Служебные записи (longname/PAX) применяются к
    /// следующей и наружу не отдаются.</summary>
    public async Task<TarEntry?> ReadHeaderAsync(CancellationToken ct)
    {
        // Недопрочитанные данные/добивка предыдущей записи — дренаж до
        // заголовка (потребитель мог не дочитать поток данных).
        await DrainRestAsync(ct);

        string? gnuName = null;
        string? paxPath = null;
        long? paxSize = null;
        while (true)
        {
            var header = new byte[HeaderSize];
            await _source.ReadExactlyAsync(header, ct);
            if (IsAllZeros(header))
                return null;

            var entry = TryParseHeader(header, 0)
                ?? throw new TarFormatException("tar header checksum mismatch");
            EnsureSupportedType(entry.TypeFlag);
            _entryRemaining = entry.Size;
            _paddingRemaining = Padding(entry.Size);
            _dataOpened = false;

            switch (entry.TypeFlag)
            {
                case TarTypeFlags.GnuLongName:
                    gnuName = Encoding.ASCII.GetString(await ReadAllDataAsync(ct)).TrimEnd('\0');
                    continue;
                case TarTypeFlags.PaxHeader:
                    ParsePax(Encoding.ASCII.GetString(await ReadAllDataAsync(ct)), ref paxPath, ref paxSize);
                    continue;
                default:
                    var name = paxPath ?? gnuName ?? entry.Name;
                    var size = paxSize ?? entry.Size;
                    _entryRemaining = size;
                    _paddingRemaining = Padding(size);
                    return entry with { Name = name, Size = size };
            }
        }
    }

    /// <summary>Поток данных текущей записи (ровно Size байт; дальше — EOF).
    /// Dispose дочитывает остаток данных и добивку до кратности 512.</summary>
    public Stream OpenData()
    {
        if (_dataOpened)
            throw new InvalidOperationException("tar entry data stream is already open");
        _dataOpened = true;
        return new EntryStream(this, _entryRemaining);
    }

    /// <summary>Данные текущей записи целиком (мелкие записи: backup_label,
    /// служебные заголовки); добивка пропускается.</summary>
    public async Task<byte[]> ReadAllDataAsync(CancellationToken ct)
    {
        if (_dataOpened)
            throw new InvalidOperationException("tar entry data stream is already open");
        var size = _entryRemaining;
        if (size > Array.MaxLength)
            throw new TarFormatException($"tar entry is too large to buffer: {size} bytes");
        var data = new byte[size];
        await ReadExactlyOrThrowAsync(data, ct);
        _entryRemaining = 0;
        await DrainPaddingAsync(ct);
        return data;
    }

    private static long Padding(long size) => (HeaderSize - size % HeaderSize) % HeaderSize;

    private static bool IsAllZeros(ReadOnlySpan<byte> header)
    {
        foreach (var b in header)
        {
            if (b != 0)
                return false;
        }

        return true;
    }

    // Разбор 512-байт заголовка по смещению: null — checksum не сошёлся или
    // буфер короче записи (вызывающий отличает «мусор» от валидной записи).
    private static TarEntry? TryParseHeader(byte[] buffer, int offset)
    {
        if (offset < 0 || buffer.Length - offset < HeaderSize)
            return null;
        var header = buffer.AsSpan(offset, HeaderSize);
        long stored;
        try
        {
            stored = ParseOctal(header.Slice(148, 8));
        }
        catch (TarFormatException)
        {
            return null;
        }

        // Checksum: сумма всех байт заголовка, поле checksum считается пробелами.
        long sum = 0;
        for (var i = 0; i < HeaderSize; i++)
            sum += i is >= 148 and < 156 ? 0x20 : header[i];
        if (sum != stored)
            return null;

        var name = ReadString(header[..100]);
        var mode = (int)ParseOctal(header.Slice(100, 8));
        var size = ParseOctal(header.Slice(124, 12));
        var typeFlag = (char)header[156];
        var linkName = ReadString(header.Slice(157, 100));
        var prefix = ReadString(header.Slice(345, 155));
        if (prefix.Length > 0)
            name = prefix + "/" + name;
        return new TarEntry(name, typeFlag, size, mode, linkName);
    }

    private static void EnsureSupportedType(char typeFlag)
    {
        if (typeFlag is TarTypeFlags.File or TarTypeFlags.LegacyFile or TarTypeFlags.Dir
            or TarTypeFlags.Symlink or TarTypeFlags.GnuLongName or TarTypeFlags.PaxHeader)
            return;
        throw new TarFormatException(
            $"tar entry type '{typeFlag}' (0x{(int)typeFlag:X2}) is not supported");
    }

    /// <summary>Октальное число tar-поля: пробелы/NUL на краях отбрасываются,
    /// пустое поле — 0.</summary>
    private static long ParseOctal(ReadOnlySpan<byte> field)
    {
        var start = 0;
        var end = field.Length;
        while (start < end && (field[start] == 0 || field[start] == (byte)' '))
            start++;
        while (end > start && (field[end - 1] == 0 || field[end - 1] == (byte)' '))
            end--;
        long value = 0;
        for (var i = start; i < end; i++)
        {
            var b = field[i];
            if (b is < (byte)'0' or > (byte)'7')
                throw new TarFormatException($"tar header: invalid octal digit '{(char)b}'");
            value = (value << 3) + (b - (byte)'0');
        }

        return value;
    }

    // Строковое поле заголовка: до NUL, хвостовые пробелы срезаются.
    private static string ReadString(ReadOnlySpan<byte> field)
    {
        var end = field.IndexOf((byte)0);
        var span = end < 0 ? field : field[..end];
        while (span.Length > 0 && span[^1] == (byte)' ')
            span = span[..^1];
        return Encoding.ASCII.GetString(span);
    }

    // PAX-запись: подряд идущие записи "<len> <key>=<value>\n", len — полная
    // длина записи в байтах. Значимы path (имя) и size (размер) следующей.
    private static void ParsePax(string text, ref string? path, ref long? size)
    {
        var pos = 0;
        while (pos < text.Length)
        {
            var space = text.IndexOf(' ', pos);
            if (space < 0)
                break;
            if (!int.TryParse(text.AsSpan(pos, space - pos), out var length) || length <= 0)
                break;
            if (pos + length > text.Length)
                break;
            var record = text.Substring(space + 1, pos + length - space - 2);
            var eq = record.IndexOf('=');
            if (eq > 0)
            {
                var key = record[..eq];
                var value = record[(eq + 1)..];
                if (key == "path")
                    path = value;
                else if (key == "size" && long.TryParse(value, out var parsed))
                    size = parsed;
            }

            pos += length;
        }
    }

    private async Task ReadExactlyOrThrowAsync(Memory<byte> buffer, CancellationToken ct)
    {
        try
        {
            await _source.ReadExactlyAsync(buffer, ct);
        }
        catch (EndOfStreamException e)
        {
            throw new TarFormatException(
                $"tar data truncated: expected {buffer.Length} bytes, stream ended ({e.Message})");
        }
    }

    private async Task DrainPaddingAsync(CancellationToken ct)
    {
        if (_paddingRemaining <= 0)
            return;
        var padding = new byte[_paddingRemaining];
        await ReadExactlyOrThrowAsync(padding, ct);
        _paddingRemaining = 0;
    }

    private async Task DrainRestAsync(CancellationToken ct)
    {
        var skip = _entryRemaining + _paddingRemaining;
        if (skip <= 0)
            return;
        var buffer = new byte[(int)Math.Min(skip, CopyBufferSize)];
        while (skip > 0)
        {
            var read = await _source.ReadAsync(
                buffer.AsMemory(0, (int)Math.Min(skip, buffer.Length)), ct);
            if (read == 0)
                throw new TarFormatException("tar data truncated: unexpected end of stream");
            skip -= read;
        }

        _entryRemaining = 0;
        _paddingRemaining = 0;
    }

    // Best-effort синхронный дренаж из Dispose потока данных: наружу не
    // бросаем — битый архив проявится на следующем ReadHeaderAsync.
    private void DrainRestSync()
    {
        try
        {
            var skip = _entryRemaining + _paddingRemaining;
            if (skip <= 0)
                return;
            var buffer = new byte[(int)Math.Min(skip, CopyBufferSize)];
            while (skip > 0)
            {
                var read = _source.Read(buffer, 0, (int)Math.Min(skip, buffer.Length));
                if (read == 0)
                    return;
                skip -= read;
            }

            _entryRemaining = 0;
            _paddingRemaining = 0;
        }
        catch (IOException)
        {
            // Обрыв на добивке — диагностируется следующим заголовком.
        }
    }

    /// <summary>Поток данных одной записи: ровно Size байт, без seek/write.</summary>
    private sealed class EntryStream : Stream
    {
        private readonly TarStreamReader _reader;
        private readonly long _size;
        private long _remaining;
        private bool _disposed;

        public EntryStream(TarStreamReader reader, long size)
        {
            _reader = reader;
            _size = size;
            _remaining = size;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _size;

        public override long Position
        {
            get => _size - _remaining;
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
            => ReadAsync(buffer.AsMemory(offset, count), CancellationToken.None)
                .AsTask().GetAwaiter().GetResult();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            if (_remaining == 0)
                return 0;
            var chunk = buffer[..(int)Math.Min(buffer.Length, _remaining)];
            var read = await _reader._source.ReadAsync(chunk, ct);
            if (read == 0)
                throw new TarFormatException("tar data truncated: unexpected end of stream");
            _remaining -= read;
            _reader._entryRemaining -= read;
            return read;
        }

        public override void Flush()
        {
            // Только чтение — сброс нечего.
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (_disposed)
                return;
            _disposed = true;
            // Дочитываем остаток данных и добивку до кратности 512.
            if (disposing)
                _reader.DrainRestSync();
            base.Dispose(disposing);
        }
    }
}
