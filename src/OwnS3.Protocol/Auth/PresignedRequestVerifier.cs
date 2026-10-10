using System.Globalization;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.Requests;
using OwnS3.Protocol.Uris;

namespace OwnS3.Protocol.Auth;

// Верификатор presigned SigV4 (arch/owns3/03 §2): подпись в query-параметрах,
// тело не подписывается (payload-строка UNSIGNED-PAYLOAD), canonical query —
// все параметры кроме X-Amz-Signature. Порядок проверок: полнота 6 параметров →
// операция в списке → resolver → формат даты → будущее-skew → диапазон
// Expires → просрочка (строго) → подпись.
public sealed class PresignedRequestVerifier(TimeProvider timeProvider)
{
    private const long MaxExpiresSeconds = 604800;
    private static readonly TimeSpan MaxSkew = TimeSpan.FromMinutes(15);

    public SigV4Result Verify(S3RequestModel model, string operation, Func<string, string?> secretResolver)
    {
        // Параметры query в исходном виде (регистр значений X-Amz-* сохраняется).
        var pairs = CanonicalRequestBuilder.QueryPairs(model.RawQuery).ToList();

        string? Get(string name) =>
            pairs.FirstOrDefault(p => string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase)).Value;

        var algorithm = Get("X-Amz-Algorithm");
        var credential = Get("X-Amz-Credential");
        var amzDate = Get("X-Amz-Date");
        var expires = Get("X-Amz-Expires");
        var signedHeaders = Get("X-Amz-SignedHeaders");
        var signature = Get("X-Amz-Signature");

        // Все параметры обязательны (спека §3.2).
        if (algorithm is null || credential is null || amzDate is null
            || expires is null || signedHeaders is null || signature is null)
            return new SigV4Result.Fail(S3ErrorCode.AuthorizationQueryParametersError);

        if (!S3PresignedOperations.Allowed.Contains(operation))
            return new SigV4Result.Fail(S3ErrorCode.AuthorizationQueryParametersError);

        // Credential — та же структура, что в заголовочном режиме.
        ParsedAuthorization parsed;
        try
        {
            parsed = AuthorizationHeaderParser.Parse(
                $"{AuthorizationHeaderParser.Algorithm} Credential={credential}, SignedHeaders={signedHeaders}, Signature={signature}");
        }
        catch (S3ProtocolException ex)
        {
            return new SigV4Result.Fail(S3ErrorCode.AuthorizationQueryParametersError, ex.Message);
        }

        var secret = secretResolver(parsed.Credential.AccessKey);
        if (secret is null)
            return new SigV4Result.Fail(S3ErrorCode.InvalidAccessKeyId);

        // Дата: формат обязателен (глава 03 §2).
        if (!DateTimeOffset.TryParseExact(amzDate, "yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var requestTime))
            return new SigV4Result.Fail(S3ErrorCode.AuthorizationQueryParametersError);
        var now = timeProvider.GetUtcNow();

        // Skew presigned — только на будущее (arch-правка 10): X-Amz-Date дальше
        // now + 15 минут → RequestTimeTooSkewed; прошедшие даты skew-проверкой
        // не отвергаются — URL валиден всё время окна.
        if (requestTime - now > MaxSkew)
            return new SigV4Result.Fail(S3ErrorCode.RequestTimeTooSkewed);

        // Expires ∈ [0, 604800]; невалидное значение — тот же код (глава 03 §2).
        if (!long.TryParse(expires, NumberStyles.Integer, CultureInfo.InvariantCulture, out var expiresSeconds)
            || expiresSeconds < 0 || expiresSeconds > MaxExpiresSeconds)
            return new SigV4Result.Fail(S3ErrorCode.AuthorizationQueryParametersError);

        // Просрочка — строгое неравенство now − X-Amz-Date > X-Amz-Expires →
        // AccessDenied (arch-правка 10): непросроченный presigned принимается
        // независимо от возраста.
        if (now - requestTime > TimeSpan.FromSeconds(expiresSeconds))
            return new SigV4Result.Fail(S3ErrorCode.AccessDenied);

        // Canonical query — все параметры запроса кроме X-Amz-Signature.
        var canonicalQuery = UriEncoding.EncodeQuery(pairs.Where(p =>
            !string.Equals(p.Name, "X-Amz-Signature", StringComparison.OrdinalIgnoreCase)));

        var canonical = CanonicalRequestBuilder.Build(model, parsed.SignedHeaders,
            PayloadHashModeClassifier.UnsignedPayloadValue);
        // canonical query подставляется отдельно (все query-параметры, не только X-Amz-*).
        canonical = ReplaceCanonicalQuery(canonical, canonicalQuery);

        var stringToSign = SigV4Core.StringToSign(amzDate, parsed.Credential.Scope, canonical);
        var signingKey = SigV4Core.SigningKey(secret, parsed.Credential.ScopeDate, parsed.Credential.Region);
        var expected = SigV4Core.SignHex(signingKey, stringToSign);

        if (!System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(
                Convert.FromHexString(expected), Convert.FromHexString(parsed.Signature)))
            return new SigV4Result.Fail(S3ErrorCode.SignatureDoesNotMatch);

        return new SigV4Result.Ok(parsed.Credential.AccessKey, expected, amzDate, parsed.Credential.Scope,
            PayloadHashMode.UnsignedPayload, PayloadHashModeClassifier.UnsignedPayloadValue);
    }

    // Канонический запрос: строка 3 — canonical query; подменяем её
    // пересобранной строкой всех параметров без X-Amz-Signature.
    private static string ReplaceCanonicalQuery(string canonical, string canonicalQuery)
    {
        var first = canonical.IndexOf('\n');
        var second = canonical.IndexOf('\n', first + 1);
        var third = canonical.IndexOf('\n', second + 1);
        return string.Concat(canonical.AsSpan(0, second + 1), canonicalQuery, canonical.AsSpan(third));
    }
}
