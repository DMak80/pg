using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace PgWorker.Backups.Tar;

/// <summary>Потоковая запись единого tar-архива (ustar + GNU longname/longlink
/// + PAX, spec t01 §3.2): заголовок ustar, имя длиннее 100 байт — GNU longname
/// (`L`), цель symlink длиннее 100 байт — GNU longlink (`K`), размер больше
/// 8 ГиБ−1 — PAX (`x`, поле size). Имена/цели кодируются UTF-8 (невалидная
/// последовательность — TarFormatException; тихое искажение имени недопустимо).
/// Ведёт счётчик записанных байт и сквозной sha256 выхода (для etcd-поля
/// `sha256` и dup-карты смещений). Sha256Hash вызывать строго после
/// <see cref="FinishAsync"/>.</summary>
public sealed class TarStreamWriter : IDisposable
{
    private const int HeaderSize = 512;
    private const int CopyBufferSize = 64 * 1024;
    private const long MaxUstarSize = 8L * 1024 * 1024 * 1024 - 1;

    private static readonly Encoding Utf8 = new UTF8Encoding(
        encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private readonly Stream _output;
    private readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    private long _position;

    public TarStreamWriter(Stream output) => _output = output;

    /// <summary>Байт записано в выход (общий размер архива на текущий момент).</summary>
    public long Position => _position;

    /// <summary>Синоним <see cref="Position"/>: общий размер архива.</summary>
    public long TotalBytes => _position;

    public Task WriteEntryAsync(TarEntry entry, Stream data, CancellationToken ct)
        => WriteEntryCoreAsync(entry, data, ct);

    public Task WriteEntryAsync(TarEntry entry, byte[] data, CancellationToken ct)
        => WriteEntryCoreAsync(entry, new MemoryStream(data, writable: false), ct);

    /// <summary>Запись с возвратом смещения её ДАННЫХ в выходном tar: считается
    /// после заголовков (включая служебные longname/PAX-записи) — единственно
    /// надёжный источник позиции для Range-чтения записей из объекта.</summary>
    public Task<long> WriteEntryWithDataOffsetAsync(
        TarEntry entry, Stream data, CancellationToken ct)
        => WriteEntryCoreAsync(entry, data, ct);

    /// <summary>То же для данных-массива (симметрия с Stream-перегрузкой записи).</summary>
    public Task<long> WriteEntryWithDataOffsetAsync(
        TarEntry entry, byte[] data, CancellationToken ct)
        => WriteEntryCoreAsync(entry, new MemoryStream(data, writable: false), ct);

    /// <summary>Запись заголовка (и служебных записей) без данных — шов
    /// теста PAX size >8 ГиБ: PAX-запись + основной заголовок с size-полем 0.</summary>
    internal async Task WriteEntryHeaderAsync(TarEntry entry, CancellationToken ct)
        => await WriteHeadersAsync(entry, ct);

    /// <summary>Добивка архива: 1024 нулевых байта (два нулевых блока).</summary>
    public async Task FinishAsync(CancellationToken ct)
        => await WriteChunkAsync(new byte[1024], ct);

    /// <summary>Сквозной sha256 записанного выхода (hex не конвертируется —
    /// байты); вызывать строго после <see cref="FinishAsync"/>.</summary>
    public byte[] Sha256Hash() => _hash.GetHashAndReset();

    // Возврат — смещение ДАННЫХ записи в выходе (после всех заголовков):
    // служебные longname/PAX-записи сдвигают его, «Position + 512» неверно.
    private async Task<long> WriteEntryCoreAsync(TarEntry entry, Stream data, CancellationToken ct)
    {
        await WriteHeadersAsync(entry, ct);
        var dataOffset = _position;
        if (entry.Size > 0)
        {
            var buffer = new byte[CopyBufferSize];
            var remaining = entry.Size;
            while (remaining > 0)
            {
                var read = await data.ReadAsync(
                    buffer.AsMemory(0, (int)Math.Min(buffer.Length, remaining)), ct);
                if (read == 0)
                    throw new IOException(
                        $"tar entry '{entry.Name}': data ended {remaining} bytes early");
                await WriteChunkAsync(buffer.AsMemory(0, read), ct);
                remaining -= read;
            }
        }

        await WritePaddingAsync(entry.Size, ct);
        return dataOffset;
    }

    private async Task WriteHeadersAsync(TarEntry entry, CancellationToken ct)
    {
        var name = entry.Name;
        var prefix = "";
        if (Utf8.GetByteCount(name) > 100)
        {
            if (TrySplitPrefix(name, out var splitPrefix, out var rest))
            {
                prefix = splitPrefix;
                name = rest;
            }
            else
            {
                await WriteLongEntryAsync(TarTypeFlags.GnuLongName, name, ct);
                // Основной заголовок несёт урезанное имя: значимое — в L-записи.
                name = TruncateUtf8(name, 99);
            }
        }

        // Цель symlink длиннее ustar-поля — GNU longlink (`K`), иначе цель
        // обрезалась бы молча (spec t01 §6.15).
        var linkName = entry.LinkName;
        if (Utf8.GetByteCount(linkName) > 100)
        {
            await WriteLongEntryAsync(TarTypeFlags.GnuLongLink, linkName, ct);
            linkName = TruncateUtf8(linkName, 100);
        }

        // Размер больше ustar-поля — в PAX-запись, в основном заголовке 0.
        var paxSize = entry.Size > MaxUstarSize;
        if (paxSize)
            await WritePaxRecordAsync("size", entry.Size.ToString(CultureInfo.InvariantCulture), ct);
        var header = BuildHeader(
            name, entry.TypeFlag, paxSize ? 0 : entry.Size, entry.Mode, linkName, prefix);
        await WriteChunkAsync(header, ct);
    }

    // Служебная GNU-запись longname (`L`)/longlink (`K`): данные + добивка.
    private async Task WriteLongEntryAsync(char typeFlag, string value, CancellationToken ct)
    {
        var data = Utf8.GetBytes(value + "\0");
        // GNU-имя служебной записи — как у GNU tar.
        var header = BuildHeader("././@LongLink", typeFlag, data.Length, 0, "", "");
        await WriteChunkAsync(header, ct);
        await WriteChunkAsync(data, ct);
        await WritePaddingAsync(data.Length, ct);
    }

    private async Task WritePaxRecordAsync(string key, string value, CancellationToken ct)
    {
        var data = Utf8.GetBytes(PaxRecord(key, value));
        var header = BuildHeader("PaxHeaders/entry", TarTypeFlags.PaxHeader, data.Length, 0, "", "");
        await WriteChunkAsync(header, ct);
        await WriteChunkAsync(data, ct);
        await WritePaddingAsync(data.Length, ct);
    }

    /// <summary>Запись PAX-поля: "&lt;len&gt; &lt;key&gt;=&lt;value&gt;\n", где
    /// len — полная длина записи в байтах (включая себя).</summary>
    internal static string PaxRecord(string key, string value)
    {
        var body = $" {key}={value}\n";
        var length = body.Length + 1;
        while (true)
        {
            var total = body.Length
                        + length.ToString(CultureInfo.InvariantCulture).Length;
            if (total == length)
                return length.ToString(CultureInfo.InvariantCulture) + body;
            length = total;
        }
    }

    // Разбиение длинного имени на ustar prefix (≤155) + name (≤100) по UTF-8.
    private static bool TrySplitPrefix(string name, out string prefix, out string rest)
    {
        for (var i = name.Length - 1; i > 0; i--)
        {
            if (name[i] != '/')
                continue;
            var candidatePrefix = name[..i];
            var candidateRest = name[(i + 1)..];
            if (Utf8.GetByteCount(candidateRest) <= 100
                && Utf8.GetByteCount(candidatePrefix) <= 155)
            {
                prefix = candidatePrefix;
                rest = candidateRest;
                return true;
            }
        }

        prefix = "";
        rest = name;
        return false;
    }

    // Усечение строки по UTF-8-байтам без разрезания символа (fallback-поля
    // основного заголовка; значимое значение идёт служебной записью).
    private static string TruncateUtf8(string value, int maxBytes)
    {
        var bytes = Utf8.GetBytes(value);
        if (bytes.Length <= maxBytes)
            return value;
        var cut = maxBytes;
        while (cut > 0 && (bytes[cut] & 0xC0) == 0x80)
            cut--;
        return Utf8.GetString(bytes, 0, cut);
    }

    private static byte[] BuildHeader(
        string name, char type, long size, int mode, string linkName, string prefix)
    {
        var header = new byte[HeaderSize];
        WriteString(header, 0, 100, name);
        WriteOctal(header, 100, 8, mode);
        WriteOctal(header, 108, 8, 0);  // uid
        WriteOctal(header, 116, 8, 0);  // gid
        WriteOctal(header, 124, 12, size);
        WriteOctal(header, 136, 12, 0); // mtime
        for (var i = 148; i < 156; i++)
            header[i] = 0x20;           // checksum считается по пробелам
        header[156] = (byte)type;
        WriteString(header, 157, 100, linkName);
        WriteString(header, 257, 6, "ustar\0");
        WriteString(header, 263, 2, "00");
        // uname/gname (2×32) и devmajor/devminor (2×8) остаются нулями.
        WriteString(header, 345, 155, prefix);
        var sum = 0L;
        foreach (var b in header)
            sum += b;
        WriteString(header, 148, 6, Convert.ToString(sum, 8).PadLeft(6, '0'));
        header[154] = 0;
        header[155] = 0x20;
        return header;
    }

    private static void WriteString(byte[] target, int offset, int length, string value)
    {
        byte[] bytes;
        try
        {
            bytes = Utf8.GetBytes(value);
        }
        catch (EncoderFallbackException e)
        {
            throw new TarFormatException($"tar field is not valid UTF-8 ({e.Message})");
        }

        Array.Copy(bytes, 0, target, offset, Math.Min(bytes.Length, length));
    }

    private static void WriteOctal(byte[] target, int offset, int length, long value)
    {
        var digits = Convert.ToString(value, 8).PadLeft(length - 1, '0');
        WriteString(target, offset, length, digits);
        target[offset + length - 1] = 0;
    }

    private async Task WritePaddingAsync(long dataSize, CancellationToken ct)
    {
        var padding = (int)((HeaderSize - dataSize % HeaderSize) % HeaderSize);
        if (padding > 0)
            await WriteChunkAsync(new byte[padding], ct);
    }

    private async Task WriteChunkAsync(ReadOnlyMemory<byte> chunk, CancellationToken ct)
    {
        await _output.WriteAsync(chunk, ct);
        _hash.AppendData(chunk.Span);
        _position += chunk.Length;
    }

    public void Dispose() => _hash.Dispose();
}
