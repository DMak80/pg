using System.Text;

namespace Shared.Docker;

/// <summary>
/// Минимальный ustar-тар (t06): запись/чтение набора файлов для Docker
/// volume-archive API (PUT/GET /volumes/&lt;name&gt;/archive). Только то, что
/// нужно TLS-сертам: короткие имена, малые размеры, без pax-расширений.
/// </summary>
public static class TarArchive
{
    public sealed record Entry(string Name, int Mode, byte[] Data);

    private const int Block = 512;

    public static byte[] Build(IReadOnlyList<Entry> entries)
    {
        using var ms = new MemoryStream();
        foreach (var e in entries)
        {
            var header = new byte[Block];
            WriteString(header, 0, 100, e.Name);
            WriteOctal(header, 100, 8, e.Mode);          // mode (0600/0644)
            WriteOctal(header, 108, 8, 0);               // uid
            WriteOctal(header, 116, 8, 0);               // gid
            WriteOctal(header, 124, 12, e.Data.Length);  // size
            WriteOctal(header, 136, 12, 0);              // mtime
            header[156] = (byte)'0';                     // typeflag: обычный файл
            Encoding.ASCII.GetBytes("ustar\0").CopyTo(header, 257);
            Encoding.ASCII.GetBytes("00").CopyTo(header, 263);
            // checksum: поле заполняется пробелами, сумма байтов — octal
            for (var i = 148; i < 156; i++) header[i] = (byte)' ';
            var sum = header.Sum(b => b);
            WriteOctal(header, 148, 7, sum);
            header[155] = 0;
            ms.Write(header);
            ms.Write(e.Data);
            var pad = (Block - e.Data.Length % Block) % Block;
            ms.Write(new byte[pad]);
        }

        ms.Write(new byte[Block * 2]); // завершающие нулевые блоки
        return ms.ToArray();
    }

    public static IReadOnlyDictionary<string, byte[]> Read(byte[] tar)
        => ReadEntries(tar).ToDictionary(e => e.Name, e => e.Data);

    // Полное чтение записей (имя + mode + данные): transport'у движка нужен
    // mode заголовка — файлы в volume ноды читает НЕ-root процесс (t06).
    public static IReadOnlyList<Entry> ReadEntries(byte[] tar)
    {
        var result = new List<Entry>();
        var offset = 0;
        while (offset + Block <= tar.Length)
        {
            var block = tar.AsSpan(offset, Block).ToArray();
            if (block.All(b => b == 0))
                break; // терминатор
            var name = ReadString(tar, offset, 100);
            var sizeField = ReadString(tar, offset + 124, 12).TrimEnd('\0', ' ');
            if (name.Length == 0 || sizeField.Length == 0)
                throw new ApplicationException($"tar: некорректный заголовок на смещении {offset}");
            var size = Convert.ToInt32(sizeField, 8);
            var modeField = ReadString(tar, offset + 100, 8).TrimEnd('\0', ' ');
            var data = tar.AsSpan(offset + Block, size).ToArray();
            result.Add(new Entry(name, string.IsNullOrEmpty(modeField) ? 0 : Convert.ToInt32(modeField, 8), data));
            offset += Block + size + (Block - size % Block) % Block;
        }

        return result;
    }

    private static void WriteString(byte[] header, int at, int len, string value)
    {
        var bytes = Encoding.ASCII.GetBytes(value);
        if (bytes.Length > len - 1)
            throw new ArgumentException($"tar-поле длиной {len} не вместило '{value}'");
        bytes.CopyTo(header, at);
    }

    private static void WriteOctal(byte[] header, int at, int len, long value)
    {
        var octal = Convert.ToString(value, 8).PadLeft(len - 1, '0');
        Encoding.ASCII.GetBytes(octal).CopyTo(header, at);
        header[at + len - 1] = 0;
    }

    private static string ReadString(byte[] source, int at, int len)
    {
        var end = at;
        while (end < at + len && source[end] != 0) end++;
        return Encoding.ASCII.GetString(source, at, end - at);
    }
}
