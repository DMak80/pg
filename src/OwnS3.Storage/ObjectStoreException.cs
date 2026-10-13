namespace OwnS3.Storage;

// Доменный исход объектного слоя; App переводит в S3Error по каталогу.
public sealed class ObjectStoreException(ObjectStoreErrorCode code, string? message = null)
    : Exception(message ?? code.ToString())
{
    public ObjectStoreErrorCode Code { get; } = code;
}
