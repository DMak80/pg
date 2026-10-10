namespace OwnS3.Storage;

// Доменный исход объектного слоя; App переводит в S3Error по каталогу.
public sealed class ObjectStoreException(ObjectStoreErrorCode code, string? message = null)
    : Exception(message ?? code.ToString())
{
    public ObjectStoreErrorCode Code { get; } = code;
}

// Заглушка t36 (spec §3.3): сервис без хранилища; App ловит по типу и
// отвечает 500 InternalError.
public sealed class ObjectStoreUnavailableException : Exception
{
    public ObjectStoreUnavailableException() : base("object store is not wired (t37)") { }
}
