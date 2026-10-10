using System.Globalization;
using OwnS3.Protocol.Errors;

namespace OwnS3.Protocol.Auth;

// Парсер Authorization-заголовка SigV4 (arch/owns3/03 §1; референс
// signature-v4-parser.go): Credential-часть сплитится справа (accessKey может
// содержать '/'), scope = ровно 4 последних сегмента; service обязан s3,
// терминал aws4_request, дата yyyyMMdd; алгоритм строго AWS4-HMAC-SHA256,
// прочее (вкл. SigV2 «AWS …») — 400 InvalidRequest (arch-правка 8).
public sealed record ParsedAuthorization(ParsedCredential Credential, IReadOnlyList<string> SignedHeaders, string Signature);

public sealed record ParsedCredential(
    string AccessKey, string ScopeDate, string Region, string Service, string Terminal)
{
    public string Scope => $"{ScopeDate}/{Region}/{Service}/{Terminal}";
}

public static class AuthorizationHeaderParser
{
    public const string Algorithm = "AWS4-HMAC-SHA256";
    public const string UnsupportedMechanismMessage =
        "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.";

    public static ParsedAuthorization Parse(string header)
    {
        if (!header.StartsWith(Algorithm + " ", StringComparison.Ordinal))
            throw new S3ProtocolException(S3ErrorCode.InvalidRequest, UnsupportedMechanismMessage);

        var rest = header[(Algorithm.Length + 1)..];
        var parts = rest.Split(',');
        if (parts.Length != 3)
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed, "The authorization header is malformed.");

        var credential = PartValue(parts[0], "Credential");
        var signedHeaders = PartValue(parts[1], "SignedHeaders");
        var signature = PartValue(parts[2], "Signature");

        // Сплот справа: accessKey может содержать '/', scope — ровно 4 последних сегмента.
        var segments = credential.Split('/');
        if (segments.Length < 5)
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed, "The authorization header is malformed.");
        var scope = segments[^4..];
        var accessKey = string.Join("/", segments[..^4]);

        if (accessKey.Length == 0)
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed, "The authorization header is malformed.");
        if (scope[2] != "s3")
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed, "The authorization header is malformed; incorrect service.");
        if (scope[3] != "aws4_request")
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed, "The authorization header is malformed; incorrect terminal.");
        if (!IsValidScopeDate(scope[0]))
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed, "The authorization header is malformed; incorrect scope date.");
        if (scope[1].Length == 0)
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed, "The authorization header is malformed; incorrect region.");

        var signed = signedHeaders.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(n => n.ToLowerInvariant()).ToList();
        if (signed.Count == 0)
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed, "The authorization header is malformed.");

        if (signature.Length != 64 || !signature.All(c => c is (>= '0' and <= '9') or (>= 'a' and <= 'f')))
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed, "The authorization header is malformed; incorrect signature.");

        return new ParsedAuthorization(
            new ParsedCredential(accessKey, scope[0], scope[1], scope[2], scope[3]), signed, signature);
    }

    private static string PartValue(string part, string name)
    {
        var eq = part.IndexOf('=');
        if (eq < 0)
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed, "The authorization header is malformed.");
        var key = part[..eq].Trim();
        if (!string.Equals(key, name, StringComparison.Ordinal))
            throw new S3ProtocolException(S3ErrorCode.AuthorizationHeaderMalformed, "The authorization header is malformed.");
        return part[(eq + 1)..].Trim();
    }

    private static bool IsValidScopeDate(string value) =>
        value.Length == 8
        && DateTime.TryParseExact(value, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out _);
}
