using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace OwnS3.Storage;

// Журналы multipart-контура (канон 04 §5): camelCase-JSON; атомарная запись
// tmp + fsync + rename (по образцу XlMetaFile.Write). internal — для
// XlVolume/XlObjectStore и юнит-тестов (InternalsVisibleTo).
internal static class MultipartJournals
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    // Запись журнала активных загрузок (uploads.json, канон 04 §5):
    // bucket/key — обратная расшифровка sha256-каталога для ListMultipartUploads;
    // accessKey — владелец для матрицы видимости (глава 05).
    internal sealed record UploadJournalEntry(string UploadId, string Bucket, string Key,
        long InitiatedMs, string AccessKey);

    // Запись журнала частей (parts.json, канон 04 §5; etag — hex-MD5 БЕЗ кавычек).
    // JsonPropertyName: camelCase от «ETag» дал бы «eTag», а имя поля по канону — «etag».
    internal sealed record PartJournalEntry(int PartNumber,
        [property: JsonPropertyName("etag")] string ETag, long Size, long ModTimeMs);

    // Маркер попытки Complete (attempt.json): dataDir текущей сборки.
    internal sealed record AttemptMarker(string DataDir);

    // Каталог ключа: sha256 UTF-8("<bucket>/<key>"), hex lowercase (канон 04 §1/§5).
    public static string KeyDir(string multipartRoot, string bucket, string key) =>
        Path.Combine(multipartRoot, Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes(bucket + "/" + key))).ToLowerInvariant());

    public static string UploadsJsonPath(string keyDir) => Path.Combine(keyDir, "uploads.json");

    public static string UploadDirPath(string keyDir, string uploadId) => Path.Combine(keyDir, uploadId);

    public static string PartsJsonPath(string uploadDir) => Path.Combine(uploadDir, "parts.json");

    public static string AttemptJsonPath(string uploadDir) => Path.Combine(uploadDir, "attempt.json");

    public static string PartFileName(int partNumber) => $"part.{partNumber}";

    public static string PartTmpFileName(int partNumber) => $"part.{partNumber}.tmp";

    // Уникальный tmp на попытку записи части (ревью t38): параллельные
    // UploadPart/UploadPartCopy одного partNumber пишут в РАЗНЫЕ tmp-файлы —
    // без IOException у второго writer'а; результат линеаризуется rename'ом
    // под _commitLock (last-writer-wins, спека §2.4).
    public static string PartTmpFileNameUnique(int partNumber) =>
        $"part.{partNumber}.{Guid.NewGuid():N}.tmp";

    // Чтение: файла нет ИЛИ пустой → []; JsonException — наружу (политику решает
    // вызывающий: операции — warning + NoSuchUpload; чистка — mtime-прокси).
    public static List<UploadJournalEntry> ReadUploads(string path) =>
        ReadList<UploadJournalEntry>(path);

    public static List<PartJournalEntry> ReadParts(string path) =>
        ReadList<PartJournalEntry>(path);

    // Битый/утраченный маркер — null: новой попытке Complete он не мешает (М7).
    public static AttemptMarker? ReadAttempt(string uploadDir)
    {
        try
        {
            var path = AttemptJsonPath(uploadDir);
            if (!File.Exists(path))
                return null;
            var json = File.ReadAllText(path);
            return json.Length == 0
                ? null
                : JsonSerializer.Deserialize<AttemptMarker>(json, JsonOptions);
        }
        catch (Exception ex) when (ex is JsonException or IOException)
        {
            return null;
        }
    }

    // Атомарная запись: <name>.tmp (Flush(flushToDisk: true)) → rename поверх.
    public static void WriteUploads(string path, IReadOnlyList<UploadJournalEntry> entries) =>
        WriteJsonAtomic(path, entries);

    public static void WriteParts(string path, IReadOnlyList<PartJournalEntry> entries) =>
        WriteJsonAtomic(path, entries);

    public static void WriteAttempt(string uploadDir, AttemptMarker marker) =>
        WriteJsonAtomic(AttemptJsonPath(uploadDir), marker);

    // Чтение списка: файла нет ИЛИ пустой → []; JsonException — наружу.
    private static List<T> ReadList<T>(string path)
    {
        if (!File.Exists(path))
            return [];
        var json = File.ReadAllText(path);
        return json.Length == 0 ? [] : JsonSerializer.Deserialize<List<T>>(json, JsonOptions) ?? [];
    }

    // Атомарная запись: <name>.tmp (fsync) → rename поверх. Каталог создаёт вызывающий.
    private static void WriteJsonAtomic<T>(string path, T value)
    {
        var tmp = path + ".tmp";
        using (var stream = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            var bytes = JsonSerializer.SerializeToUtf8Bytes(value, JsonOptions);
            stream.Write(bytes, 0, bytes.Length);
            stream.Flush(flushToDisk: true);
        }
        File.Move(tmp, path, overwrite: true);
    }
}
