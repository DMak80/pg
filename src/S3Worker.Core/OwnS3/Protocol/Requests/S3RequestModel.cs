namespace OwnS3.Protocol.Requests;

// Транспортно-независимая модель запроса (arch/owns3/01 §3): Protocol работает
// только с ней; App строит из HttpRequest, тесты — напрямую.
public sealed class S3RequestModel
{
    public required string Method { get; init; }           // верхний регистр
    public required string RawPath { get; init; }           // как прислал клиент, без декодирования
    public required string RawQuery { get; init; }          // без ведущего '?'
    public required S3HeaderCollection Headers { get; init; }
    public required string Host { get; init; }
    public required Func<Stream> OpenBody { get; init; }    // ленивый доступ к телу (повторно не открывается)
}
