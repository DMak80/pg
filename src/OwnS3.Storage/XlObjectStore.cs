using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Реализация IObjectStore поверх драйва размещения (IXlDrive): держит ВСЮ
// логику 21 операции (conditional/Range/ETag/листинги/multipart-валидации/
// идемпотентность), способ размещения — за драйвом (канон 04/06).
// Partial-раскладка: бакеты/объекты/copy/листинги/multipart.
// Исходы операций — Result-конвенция главы 01 §4: ошибки каталога — Failed
// с доменным кодом, значения успеха — в Value.
public sealed partial class XlObjectStore(IXlDrive drive, TimeProvider timeProvider,
    ILogger<XlObjectStore> logger) : IObjectStore
{
    // Драйв размещения (single/EC — единая логика операций).
    internal readonly IXlDrive Drive = drive;

    // Логгер объекта (warning битых журналов, fromBackup и т.п.).
    private readonly ILogger<XlObjectStore> _logger = logger;

    // Отсутствие бакета — доменный исход NoSuchBucket (Failed по конвенции 01 §4);
    // кворумная недоступность EC — Failed(InsufficientReadQuorum) («503, не „бакета нет“», канон 06 §7)
    internal Result EnsureBucket(string bucket)
    {
        var exists = Drive.BucketExists(bucket);
        return !exists.IsSuccess
            ? Result.Failed(exists.Error!)
            : exists.Value
                ? Result.Success()
                : Result.Failed(new ObjectStoreException(ObjectStoreErrorCode.NoSuchBucket));
    }
}
