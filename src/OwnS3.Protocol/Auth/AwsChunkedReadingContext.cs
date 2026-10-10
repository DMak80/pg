namespace OwnS3.Protocol.Auth;

// Контекст чанковой верификации (arch/owns3/03 §3): seed-подпись из
// SigV4HeaderVerifier, размер декодированного тела, список трейлеров.
public sealed record AwsChunkedReadingContext(
    string SecretKey,
    string AmzDate,          // yyyyMMdd'T'HHmmss'Z'
    string Scope,            // <date>/<region>/s3/aws4_request
    string SeedSignature,
    long DecodedContentLength,
    bool WithTrailers,
    IReadOnlyList<string> TrailerNames);   // имена из x-amz-trailer, нижний регистр
