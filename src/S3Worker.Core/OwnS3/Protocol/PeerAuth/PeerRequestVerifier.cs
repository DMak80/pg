using System.Globalization;
using System.Security.Cryptography;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.Requests;

namespace OwnS3.Protocol.PeerAuth;

// Результат верификации peer-подписи (образец SigV4Result): успех с именем пира
// или канонический отказ каталога главы 03 §5.
public abstract record PeerVerifyResult
{
    private PeerVerifyResult() { }

    public sealed record Ok(string PeerName) : PeerVerifyResult;

    public sealed record Fail(S3ErrorCode Code, string? Detail = null) : PeerVerifyResult;
}

// Верификатор peer-подписи OWNS3-PEER-ECC-P256-SHA256 (глава 07 §6; зеркало
// SigV4HeaderVerifier). Порядок проверок: парсинг Authorization → дата/skew
// (±15 мин) → канонический запрос (переиспользование CanonicalRequestBuilder,
// фиксированный список заголовков) → подпись ECDSA P-256 P1363.
public sealed class PeerRequestVerifier(TimeProvider timeProvider)
{
    private static readonly TimeSpan MaxSkew = TimeSpan.FromMinutes(15);

    public PeerVerifyResult Verify(S3RequestModel model, ECDsa publicKey)
    {
        var authHeader = model.Headers.First("Authorization");
        if (authHeader is null)
            return new PeerVerifyResult.Fail(S3ErrorCode.AuthorizationHeaderMalformed, "Missing Authorization header");

        ParsedAuthorization parsed;
        try
        {
            parsed = Parse(authHeader);
        }
        catch (S3ProtocolException ex)
        {
            return new PeerVerifyResult.Fail(ex.Code, ex.Message);
        }

        var contentSha = model.Headers.First("x-owns3-peer-content-sha256");
        if (contentSha is null)
            return new PeerVerifyResult.Fail(S3ErrorCode.AuthorizationHeaderMalformed,
                "Missing x-owns3-peer-content-sha256 header");

        // Дата схемы: x-owns3-peer-date (yyyyMMdd'T'HHmmss'Z'); отсутствует/невалидна — malformed.
        var peerDate = model.Headers.First("x-owns3-peer-date");
        if (peerDate is null || !DateTimeOffset.TryParseExact(peerDate, "yyyyMMdd'T'HHmmss'Z'",
                CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out var requestTime))
            return new PeerVerifyResult.Fail(S3ErrorCode.AuthorizationHeaderMalformed,
                "Missing/Invalid x-owns3-peer-date header");

        // Skew ±15 минут (глава 07 §6 — тот же допуск, что SigV4).
        if (Math.Abs((timeProvider.GetUtcNow() - requestTime).TotalMinutes) > MaxSkew.TotalMinutes)
            return new PeerVerifyResult.Fail(S3ErrorCode.RequestTimeTooSkewed);

        var canonical = CanonicalRequestBuilder.Build(model, PeerRequestSigner.SignedHeaderNames, contentSha);
        var stringToSign = $"{PeerRequestSigner.Algorithm}\n{peerDate}\n" +
            SigV4Core.HexSha256(System.Text.Encoding.UTF8.GetBytes(canonical));

        byte[] signature;
        try
        {
            signature = Convert.FromBase64String(parsed.Signature);
        }
        catch (FormatException)
        {
            return new PeerVerifyResult.Fail(S3ErrorCode.AuthorizationHeaderMalformed,
                "Signature is not valid base64");
        }

        bool verified;
        try
        {
            verified = publicKey.VerifyData(System.Text.Encoding.UTF8.GetBytes(stringToSign), signature,
                HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation);
        }
        catch (CryptographicException)
        {
            verified = false;
        }

        // Постоянное по времени сравнение не требуется: VerifyData — константная проверка ECDSA.
        return verified
            ? new PeerVerifyResult.Ok(parsed.Credential)
            : new PeerVerifyResult.Fail(S3ErrorCode.SignatureDoesNotMatch);
    }

    // Имя пира из Credential= Authorization ИЛИ null при любом malformed (используется
    // гранью ДО резолва записи; Protocol — без Result-сигнатур: маппинг
    // null → 400 AuthorizationHeaderMalformed делает middleware).
    public static string? CredentialOf(S3RequestModel model)
    {
        var authHeader = model.Headers.First("Authorization");
        if (authHeader is null)
            return null;
        var spaceIdx = authHeader.IndexOf(' ');
        if (spaceIdx <= 0 || !string.Equals(authHeader[..spaceIdx], PeerRequestSigner.Algorithm,
                StringComparison.Ordinal))
            return null;
        foreach (var segment in authHeader[(spaceIdx + 1)..].Split(','))
        {
            var trimmed = segment.Trim();
            if (trimmed.StartsWith("Credential=", StringComparison.Ordinal))
            {
                var name = trimmed["Credential=".Length..];
                return name.Length > 0 ? name : null;
            }
        }
        return null;
    }

    private static ParsedAuthorization Parse(string authHeader)
    {
        var spaceIdx = authHeader.IndexOf(' ');
        if (spaceIdx <= 0)
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed,
                "Authorization header is malformed");
        if (!string.Equals(authHeader[..spaceIdx], PeerRequestSigner.Algorithm, StringComparison.Ordinal))
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed,
                "Unsupported peer authorization scheme");

        string? credential = null, signedHeaders = null, signature = null;
        foreach (var segment in authHeader[(spaceIdx + 1)..].Split(','))
        {
            var trimmed = segment.Trim();
            const string credPrefix = "Credential=";
            const string headersPrefix = "SignedHeaders=";
            const string signaturePrefix = "Signature=";
            if (trimmed.StartsWith(credPrefix, StringComparison.Ordinal))
                credential = trimmed[credPrefix.Length..];
            else if (trimmed.StartsWith(headersPrefix, StringComparison.Ordinal))
                signedHeaders = trimmed[headersPrefix.Length..];
            else if (trimmed.StartsWith(signaturePrefix, StringComparison.Ordinal))
                signature = trimmed[signaturePrefix.Length..];
            else
                throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed,
                    "Unknown field in Authorization header");
        }

        // Ровно три поля схемы, подписаны ровно фиксированные заголовки (глава 07 §6).
        if (string.IsNullOrEmpty(credential) || signedHeaders is null || signature is null)
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed,
                "Authorization header is missing required fields");
        var expected = string.Join(";", PeerRequestSigner.SignedHeaderNames);
        if (!string.Equals(signedHeaders, expected, StringComparison.Ordinal))
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed,
                "SignedHeaders must be exactly " + expected);

        return new ParsedAuthorization(credential, signature);
    }

    private sealed record ParsedAuthorization(string Credential, string Signature);
}
