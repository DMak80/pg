using System.Text.Json;

namespace OwnS3.Storage;

// Метаданные бакета: .owns3.sys/buckets/<bucket>/bucket.json {"createdAt": unix-мс}
// — источник CreationDate листинга ListBuckets (канон 04 §1, правка спеки §3.2).
public sealed class BucketMetaStore(XlVolume volume, TimeProvider timeProvider)
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    // Модель bucket.json.
    private sealed record BucketMeta(long CreatedAt);

    // Создаёт bucket.json, возвращает createdAt.
    public DateTimeOffset CreateBucketMeta(string bucket)
    {
        var dir = Path.Combine(volume.BucketsMetaDir, bucket);
        Directory.CreateDirectory(dir);
        var createdAt = timeProvider.GetUtcNow();
        File.WriteAllText(Path.Combine(dir, "bucket.json"),
            JsonSerializer.Serialize(new BucketMeta(createdAt.ToUnixTimeMilliseconds()), JsonOptions));
        return createdAt;
    }

    // null — файла нет (CreationDate по умолчанию у вызывающего).
    public DateTimeOffset? TryReadCreationDate(string bucket)
    {
        var path = Path.Combine(volume.BucketsMetaDir, bucket, "bucket.json");
        if (!File.Exists(path))
            return null;
        try
        {
            var meta = JsonSerializer.Deserialize<BucketMeta>(File.ReadAllText(path), JsonOptions);
            return meta is null ? null : DateTimeOffset.FromUnixTimeMilliseconds(meta.CreatedAt);
        }
        catch (JsonException)
        {
            return null; // битая мета — не роняет листинг
        }
    }
}
