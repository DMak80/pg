using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Реализация IObjectStore поверх драйва размещения (IXlDrive): держит ВСЮ
// логику 21 операции (conditional/Range/ETag/листинги/multipart-валидации/
// идемпотентность), способ размещения — за драйвом (канон 04/06).
// Partial-раскладка: бакеты/объекты/copy/листинги/multipart.
public sealed partial class XlObjectStore(IXlDrive drive, TimeProvider timeProvider,
    ILogger<XlObjectStore> logger) : IObjectStore
{
    // Драйв размещения (single/EC — единая логика операций).
    internal readonly IXlDrive Drive = drive;

    // Логгер объекта (warning битых журналов, fromBackup и т.п.).
    private readonly ILogger<XlObjectStore> _logger = logger;

    // Отсутствие бакета — доменный исход NoSuchBucket.
    internal void EnsureBucket(string bucket)
    {
        if (!Drive.BucketExists(bucket))
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchBucket);
    }
}
