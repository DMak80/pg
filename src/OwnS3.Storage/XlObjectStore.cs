using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Реализация IObjectStore на xl-томе (том обязан быть Initialized до первого
// вызова). Partial-раскладка: бакеты/объекты/copy/листинги/multipart.
public sealed partial class XlObjectStore(XlVolume volume, TimeProvider timeProvider,
    ILogger<XlObjectStore> logger) : IObjectStore
{
    private readonly BucketMetaStore _bucketMeta = new(volume, timeProvider);

    // Логгер объекта (задачи 6–9: warning битых xl.meta, fromBackup и т.п.)
    private readonly ILogger<XlObjectStore> _logger = logger;

    // Корень бакета в томе.
    internal string BucketRoot(string bucket) => Path.Combine(volume.Root, bucket);

    // Каталог существует и не служебный.
    internal bool BucketExists(string bucket) =>
        bucket != ".owns3.sys" && Directory.Exists(BucketRoot(bucket));

    // Каталог объекта по ключу (кодированный путь канона 04 §1).
    internal string ObjectDir(string bucket, string key) =>
        Path.Combine(BucketRoot(bucket), XlPathEncoder.EncodePath(key));

    // Отсутствие бакета — доменный исход NoSuchBucket.
    internal void EnsureBucket(string bucket)
    {
        if (!BucketExists(bucket))
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchBucket);
    }
}
