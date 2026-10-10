using System.Buffers.Binary;
using System.Text;

namespace OwnS3.Storage;

// Запись-версия unversioned-объекта (формат — спека §4.2 / канон 04 §2).
public sealed record XlMetaRecord(Guid VersionId, long Size, DateTimeOffset ModTime, string ETag,
    string ContentType, IReadOnlyDictionary<string, string> UserMetadata,
    IReadOnlyDictionary<string, string> Headers, string ContentSha256)
{
    // Имя каталога данных = versionId (P1: dataDir-имя — versionId записи).
    public string DataDirName => VersionId.ToString("N");
}

// Чтение/запись xl.meta: побайтовая сериализация (little-endian), порядок записи
// канона 04 §2 (xl.meta.bkp → tmp-файл → атомарный rename), фолбэк чтения bkp.
public static class XlMetaFile
{
    private const string FileName = "xl.meta";
    private const string BackupFileName = "xl.meta.bkp";
    private const string TmpFileName = "xl.meta.tmp";
    private const byte FormatVersion = 1;
    private const ushort RecordVersion = 1;

    // Строгая десериализация одного файла; битая структура/magic/версия —
    // XlIntegrityException. Отсутствие файла — FileNotFoundException.
    public static XlMetaRecord ReadFile(string path)
    {
        byte[] data;
        try
        {
            data = File.ReadAllBytes(path);
        }
        catch (FileNotFoundException)
        {
            throw; // сигнал «объекта нет» — вызывающий транслирует в NoSuchKey
        }
        catch (DirectoryNotFoundException)
        {
            throw new FileNotFoundException($"xl.meta не найден: {path}", path);
        }
        return Parse(path, data);
    }

    // Чтение каталога объекта: xl.meta → при порче фолбэк xl.meta.bkp
    // (fromBackup=true); отсутствие обоих файлов — FileNotFoundException;
    // оба битые — XlIntegrityException.
    public static XlMetaRecord Read(string objectDir, out bool fromBackup)
    {
        var mainPath = Path.Combine(objectDir, FileName);
        var bkpPath = Path.Combine(objectDir, BackupFileName);
        try
        {
            fromBackup = false;
            return ReadFile(mainPath);
        }
        catch (XlIntegrityException)
        {
            // Основной битый — страховочная копия (диагностика порчи, не клиентам)
            fromBackup = true;
            return ReadFile(bkpPath);
        }
        catch (FileNotFoundException) when (File.Exists(bkpPath))
        {
            // Основного нет, bkp есть (краш между bkp и rename) — читаем bkp
            fromBackup = true;
            return ReadFile(bkpPath);
        }
    }

    // Запись xl.meta в каталог: существующий xl.meta копируется в xl.meta.bkp →
    // новый пишется во временный файл (fsync ДО rename, спека §4.3 п.2) →
    // атомарный rename поверх (канон 04 §2). fsync каталога после rename —
    // ответственность вызывающего.
    public static void Write(string objectDir, XlMetaRecord record)
    {
        Directory.CreateDirectory(objectDir);
        var mainPath = Path.Combine(objectDir, FileName);
        var bkpPath = Path.Combine(objectDir, BackupFileName);
        var tmpPath = Path.Combine(objectDir, TmpFileName);
        if (File.Exists(mainPath))
            File.Copy(mainPath, bkpPath, overwrite: true);
        var data = Serialize(record);
        // fsync tmp-файла до rename: содержимое переживает сбой в момент коммита
        using (var stream = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            stream.Write(data, 0, data.Length);
            stream.Flush(flushToDisk: true);
        }
        File.Move(tmpPath, mainPath, overwrite: true);
    }

    // Сериализация: magic | uint8 formatVersion | uint16 recordVersion | 16B versionId |
    // int64 size | uint64 modTimeUnixMs | string etag | string contentType |
    // uint16 userMetadataCount | пары string | uint16 headersCount | пары string |
    // string contentSha256. string = uint16 byteLength + UTF-8 байты.
    private static byte[] Serialize(XlMetaRecord r)
    {
        using var ms = new MemoryStream(256);
        ms.Write("OWS3"u8);
        ms.WriteByte(FormatVersion);
        var version = new byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(version, RecordVersion);
        ms.Write(version);
        ms.Write(r.VersionId.ToByteArray());
        var size = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(size, r.Size);
        ms.Write(size);
        var modTime = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(modTime, (ulong)r.ModTime.ToUnixTimeMilliseconds());
        ms.Write(modTime);
        WriteString(ms, r.ETag);
        WriteString(ms, r.ContentType);
        WriteCountAndPairs(ms, r.UserMetadata);
        WriteCountAndPairs(ms, r.Headers);
        WriteString(ms, r.ContentSha256);
        return ms.ToArray();
    }

    private static void WriteString(Stream stream, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        var length = new byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(length, (ushort)bytes.Length);
        stream.Write(length);
        stream.Write(bytes);
    }

    private static void WriteCountAndPairs(Stream stream, IReadOnlyDictionary<string, string> pairs)
    {
        var count = new byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(count, (ushort)pairs.Count);
        stream.Write(count);
        foreach (var (key, value) in pairs)
        {
            WriteString(stream, key);
            WriteString(stream, value);
        }
    }

    // Строгий разбор: любое несоответствие (обрыв, magic, версии, остаточные
    // байты) — XlIntegrityException.
    private static XlMetaRecord Parse(string path, byte[] data)
    {
        try
        {
            var offset = 0;
            if (data.Length < 4 || !data.AsSpan(0, 4).SequenceEqual("OWS3"u8))
                throw new XlIntegrityException($"xl.meta {path}: неверный magic");
            offset = 4;
            if (data[offset] != FormatVersion)
                throw new XlIntegrityException($"xl.meta {path}: неизвестная версия формата {data[offset]}");
            offset++;
            if (BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset, 2)) != RecordVersion)
                throw new XlIntegrityException($"xl.meta {path}: неизвестная версия записи");
            offset += 2;
            var versionId = new Guid(data.AsSpan(offset, 16).ToArray());
            offset += 16;
            var size = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(offset, 8));
            offset += 8;
            var modTimeMs = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(offset, 8));
            offset += 8;
            var etag = ReadString(data, ref offset);
            var contentType = ReadString(data, ref offset);
            var userMetadata = ReadPairs(data, ref offset);
            var headers = ReadPairs(data, ref offset);
            var contentSha256 = ReadString(data, ref offset);
            if (offset != data.Length)
                throw new XlIntegrityException($"xl.meta {path}: остаточные байты ({data.Length - offset})");
            return new XlMetaRecord(versionId, size, DateTimeOffset.FromUnixTimeMilliseconds((long)modTimeMs),
                etag, contentType, userMetadata, headers, contentSha256);
        }
        catch (Exception ex) when (ex is XlIntegrityException or ArgumentOutOfRangeException or IndexOutOfRangeException)
        {
            // Обрыв данных — та же невосстановимая порча структуры
            throw new XlIntegrityException($"xl.meta {path}: битая структура ({ex.Message})");
        }
    }

    private static string ReadString(byte[] data, ref int offset)
    {
        var length = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset, 2));
        offset += 2;
        var value = Encoding.UTF8.GetString(data.AsSpan(offset, length));
        offset += length;
        return value;
    }

    private static IReadOnlyDictionary<string, string> ReadPairs(byte[] data, ref int offset)
    {
        var count = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset, 2));
        offset += 2;
        var pairs = new Dictionary<string, string>(count);
        for (var i = 0; i < count; i++)
        {
            var key = ReadString(data, ref offset);
            var value = ReadString(data, ref offset);
            pairs[key] = value;
        }
        return pairs;
    }
}
