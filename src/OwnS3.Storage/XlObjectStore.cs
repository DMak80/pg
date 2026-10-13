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

    // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
    private static XlMetaRecord? UnwrapMeta(Result<XlMetaLookup> lookup)
        => lookup.IsSuccess
            ? lookup.Value is XlMetaLookup.Found f ? f.Record : null
            : throw lookup.Error!;

    // мост t38: демонтаж в задаче перевода IObjectStore/IReplicaStore
    // (кворумный отказ BucketExists бросается, как сегодня)
    internal void EnsureBucket(string bucket)
    {
        if (!Drive.BucketExists(bucket).Throw().Value)
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchBucket);
    }
}
