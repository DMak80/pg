using System.Globalization;
using System.Security.Cryptography;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.Requests;

namespace OwnS3.Protocol.Auth;

// Результат верификации SigV4: успех (seed-подпись для чанковой цепочки) или
// канонический отказ (глава 03 §1 + arch-правки 1/2/8).
public abstract record SigV4Result
{
    private SigV4Result() { }

    public sealed record Ok(
        string AccessKey,
        string SeedSignature,
        string AmzDate,
        string Scope,
        PayloadHashMode Mode,
        string PayloadValue) : SigV4Result;

    public sealed record Fail(S3ErrorCode Code, string? Detail = null) : SigV4Result;
}

// Верификатор заголовочной SigV4 (arch/owns3/03 §1). Порядок проверок:
// парсинг → классификация payload → resolver(accessKey) → InvalidAccessKeyId →
// дата (формат/подстановка Date, arch-правка 2) / skew → канонический запрос →
// подпись (постоянное сравнение байтов).
public sealed class SigV4HeaderVerifier(TimeProvider timeProvider)
{
    private static readonly TimeSpan MaxSkew = TimeSpan.FromMinutes(15);

    public SigV4Result Verify(S3RequestModel model, Func<string, string?> secretResolver)
    {
        var authHeader = model.Headers.First("Authorization");
        if (authHeader is null)
            return new SigV4Result.Fail(S3ErrorCode.AccessDenied);

        ParsedAuthorization parsed;
        try
        {
            parsed = AuthorizationHeaderParser.Parse(authHeader);
        }
        catch (S3ProtocolException ex)
        {
            return new SigV4Result.Fail(ex.Code, ex.Message);
        }

        PayloadHashModeClassification payload;
        try
        {
            payload = PayloadHashModeClassifier.Classify(model.Headers.First("x-amz-content-sha256"), model.Method);
        }
        catch (S3ProtocolException ex)
        {
            return new SigV4Result.Fail(ex.Code, ex.Message);
        }

        var secret = secretResolver(parsed.Credential.AccessKey);
        if (secret is null)
            return new SigV4Result.Fail(S3ErrorCode.InvalidAccessKeyId);

        // Дата: x-amz-date (yyyyMMdd'T'HHmmss'Z'), при отсутствии — Date
        // (RFC 7231); оба отсутствуют/невалидны — AuthorizationHeaderMalformed
        // (arch-правка 2: Missing/Invalid).
        var amzDate = ResolveAmzDate(model);
        if (amzDate is null)
            return new SigV4Result.Fail(S3ErrorCode.AuthorizationHeaderMalformed, "Missing/Invalid x-amz-date header");

        // Skew ±15 минут (глава 03 §1).
        var requestTime = DateTimeOffset.ParseExact(amzDate, "yyyyMMdd'T'HHmmss'Z'",
            CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        if (Math.Abs((timeProvider.GetUtcNow() - requestTime).TotalMinutes) > MaxSkew.TotalMinutes)
            return new SigV4Result.Fail(S3ErrorCode.RequestTimeTooSkewed);

        var canonical = CanonicalRequestBuilder.Build(model, parsed.SignedHeaders, payload.HexValue);
        var stringToSign = SigV4Core.StringToSign(amzDate, parsed.Credential.Scope, canonical);
        var signingKey = SigV4Core.SigningKey(secret, parsed.Credential.ScopeDate, parsed.Credential.Region);
        var expected = SigV4Core.SignHex(signingKey, stringToSign);

        // Постоянное по времени сравнение байтов подписи.
        if (!CryptographicOperations.FixedTimeEquals(
                Convert.FromHexString(expected), Convert.FromHexString(parsed.Signature)))
            return new SigV4Result.Fail(S3ErrorCode.SignatureDoesNotMatch);

        return new SigV4Result.Ok(
            parsed.Credential.AccessKey, expected, amzDate, parsed.Credential.Scope, payload.Mode, payload.HexValue);
    }

    private static string? ResolveAmzDate(S3RequestModel model)
    {
        var amzDate = model.Headers.First("x-amz-date");
        if (amzDate is not null)
            return IsValidAmzDate(amzDate) ? amzDate : null;

        // Подстановка Date (RFC 7231 IMF-fixdate).
        var date = model.Headers.First("Date");
        if (date is null)
            return null;
        return DateTimeOffset.TryParseExact(date, "R", CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed)
            ? parsed.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture)
            : null;
    }

    private static bool IsValidAmzDate(string value) =>
        DateTimeOffset.TryParseExact(value, "yyyyMMdd'T'HHmmss'Z'",
            CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out _);
}
