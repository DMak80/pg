namespace OwnS3.Storage;

// Доменные коды исходов объектного слоя (глава 02): имена совпадают с
// S3-кодами каталога главы 03 — App маппит по имени; собственного
// служебного кода «хранилище не подключено» нет (для этого —
// ObjectStoreUnavailableException).
public enum ObjectStoreErrorCode
{
    NoSuchBucket,
    NoSuchKey,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    InvalidPart,
    InvalidPartOrder,
    NoSuchUpload,
    InvalidRange,
    PreconditionFailed,
    NotModified,
    EntityTooLarge,
}
