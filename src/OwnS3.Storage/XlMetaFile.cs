using System.Buffers.Binary;
using System.Text;

namespace OwnS3.Storage;

// EC-часть записи v2 (канон 06 §5; EcIndex — позиция диска этой копии).
public sealed record XlMetaEcInfo(string EcAlgo, byte EcM, byte EcN, uint EcBSize, byte EcIndex,
    byte[] EcDist, IReadOnlyList<XlMetaPartBitrot> Parts);

// Bitrot-запись части: исходный размер + полный 2D-набор хэшей (канон 06 §5
// в правке Task 0). BlockHashes[shardIndex][blockIndex] — 32 байта; shardIndex
// 0..ecN-1, раскладка шард-мажор.
public sealed record XlMetaPartBitrot(long SourceSize, byte[][][] BlockHashes);

// Запись-версия unversioned-объекта (формат — спека §4.2 / канон 04 §2);
// запись v2 несёт EC-поля (канон 06 §5): v1-записи читаются с Ec == null,
// single-том v2-записей не пишет (спека §4.1).
public sealed record XlMetaRecord(Guid VersionId, long Size, DateTimeOffset ModTime, string ETag,
    string ContentType, IReadOnlyDictionary<string, string> UserMetadata,
    IReadOnlyDictionary<string, string> Headers, string ContentSha256, XlMetaEcInfo? Ec = null)
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
            // Основной битый: bkp отсутствует — невосстановимая порча (500,
            // спека §4.2), НЕ маскируется под «объекта нет» (404)
            if (!File.Exists(bkpPath))
                throw;
            // Страховочная копия (диагностика порчи, не клиентам)
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
    // string contentSha256 [| v2-хвост: string ecAlgo | uint8 ecM | uint8 ecN |
    // uint32 ecBSize | uint8 ecIndex | uint8 distCount + distCount×uint8 ecDist |
    // uint16 partCount | на часть: uint64 sourceSize | uint32 blockCount |
    // для каждого j=0..ecN−1: blockCount×32 Б HighwayHash-256 (шард-мажор)].
    // string = uint16 byteLength + UTF-8 байты.
    private static byte[] Serialize(XlMetaRecord r)
    {
        using var ms = new MemoryStream(256);
        ms.Write("OWS3"u8);
        ms.WriteByte(FormatVersion);
        var recordVersion = (ushort)(r.Ec is null ? 1 : 2);
        var version = new byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(version, recordVersion);
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
        if (r.Ec is { } ec)
        {
            WriteEcTail(ms, ec);
        }

        return ms.ToArray();
    }

    // Побайтовый v2-хвост (канон 06 §5 в правке Task 0).
    private static void WriteEcTail(Stream stream, XlMetaEcInfo ec)
    {
        WriteString(stream, ec.EcAlgo);
        stream.WriteByte(ec.EcM);
        stream.WriteByte(ec.EcN);
        var bsize = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(bsize, ec.EcBSize);
        stream.Write(bsize);
        stream.WriteByte(ec.EcIndex);
        stream.WriteByte((byte)ec.EcDist.Length);
        foreach (var d in ec.EcDist)
            stream.WriteByte(d);
        var partCount = new byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(partCount, (ushort)ec.Parts.Count);
        stream.Write(partCount);
        foreach (var part in ec.Parts)
        {
            var sourceSize = new byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(sourceSize, part.SourceSize);
            stream.Write(sourceSize);
            var blockCount = new byte[4];
            var blockCountValue = part.BlockHashes.Length > 0 ? part.BlockHashes[0].Length : 0;
            BinaryPrimitives.WriteUInt32LittleEndian(blockCount, (uint)blockCountValue);
            stream.Write(blockCount);
            for (var shard = 0; shard < ec.EcN; shard++)
            {
                var hashes = shard < part.BlockHashes.Length ? part.BlockHashes[shard] : [];
                for (var block = 0; block < blockCountValue; block++)
                {
                    var hash = block < hashes.Length ? hashes[block] : new byte[32];
                    stream.Write(hash.Length == 32 ? hash : PadTo32(hash));
                }
            }
        }
    }

    private static byte[] PadTo32(byte[] hash)
    {
        var padded = new byte[32];
        Array.Copy(hash, padded, Math.Min(hash.Length, 32));
        return padded;
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
    // байты) — XlIntegrityException. recordVersion 1 → v1 (Ec == null), 2 → v2.
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
            var recordVersion = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset, 2));
            if (recordVersion is not (1 or 2))
                throw new XlIntegrityException($"xl.meta {path}: неизвестная версия записи {recordVersion}");
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
            XlMetaEcInfo? ec = null;
            if (recordVersion == 2)
            {
                ec = ReadEcTail(path, data, ref offset);
            }

            if (offset != data.Length)
                throw new XlIntegrityException($"xl.meta {path}: остаточные байты ({data.Length - offset})");
            return new XlMetaRecord(versionId, size, DateTimeOffset.FromUnixTimeMilliseconds((long)modTimeMs),
                etag, contentType, userMetadata, headers, contentSha256, ec);
        }
        catch (Exception ex) when (ex is XlIntegrityException or ArgumentOutOfRangeException or IndexOutOfRangeException)
        {
            // Обрыв данных — та же невосстановимая порча структуры
            throw new XlIntegrityException($"xl.meta {path}: битая структура ({ex.Message})");
        }
    }

    // Разбор v2-хвоста (канон 06 §5 в правке Task 0; размер bitrot-секции части —
    // 8 + 4 + ecN × blockCount × 32 байт).
    private static XlMetaEcInfo ReadEcTail(string path, byte[] data, ref int offset)
    {
        var ecAlgo = ReadString(data, ref offset);
        var ecM = data[offset++];
        var ecN = data[offset++];
        var ecBSize = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(offset, 4));
        offset += 4;
        var ecIndex = data[offset++];
        var distCount = data[offset++];
        var ecDist = new byte[distCount];
        for (var i = 0; i < distCount; i++)
            ecDist[i] = data[offset++];
        var partCount = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset, 2));
        offset += 2;
        var parts = new XlMetaPartBitrot[partCount];
        for (var p = 0; p < partCount; p++)
        {
            var sourceSize = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(offset, 8));
            offset += 8;
            var blockCount = (int)BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(offset, 4));
            offset += 4;
            var hashes = new byte[ecN][][];
            for (var shard = 0; shard < ecN; shard++)
            {
                hashes[shard] = new byte[blockCount][];
                for (var block = 0; block < blockCount; block++)
                {
                    hashes[shard][block] = data.AsSpan(offset, 32).ToArray();
                    offset += 32;
                }
            }

            parts[p] = new XlMetaPartBitrot(sourceSize, hashes);
        }

        return new XlMetaEcInfo(ecAlgo, ecM, ecN, ecBSize, ecIndex, ecDist, parts);
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
