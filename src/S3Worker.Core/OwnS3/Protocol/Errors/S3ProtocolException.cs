namespace OwnS3.Protocol.Errors;

// Протокольное исключение с каноническим S3-кодом (arch/owns3/03 §5): несут
// парсеры/сверки подписи и тела; App переводит в канонический XML-ответ.
// Message по умолчанию — канонический Message кода из каталога.
public sealed class S3ProtocolException(S3ErrorCode code, string? message = null)
    : Exception(message ?? S3ErrorCatalog.Get(code).Message)
{
    public S3ErrorCode Code { get; } = code;
}
