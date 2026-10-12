using System.Globalization;
using OwnS3.Protocol.Requests;

namespace OwnS3.Protocol.Auth;

// Исходящая заголовочная SigV4 (t35, глава 07 §6; зеркало SigV4HeaderVerifier):
// собирает Authorization для (method, path, canonicalQuery, headers, payloadString)
// по CredentialDate/region "us-east-1" (регион не проверяется верификатором жёстко —
// scope из заголовка). Канонизация — переиспользует CanonicalRequestBuilder/SigV4Core
// (не дублирует), взаимная совместимость с собственной replication-гранью проверяется
// юнитом roundtrip (подпись проходит SigV4HeaderVerifier).
public static class SigV4Signer
{
    public static (string Authorization, string AmzDate, string ContentSha256) Sign(
        string secretKey, string accessKey, string method, string path, string canonicalQuery,
        IReadOnlyList<(string Name, string Value)> headers, string payloadString,
        DateTimeOffset at, string region = "us-east-1")
    {
        var amzDate = at.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var scopeDate = at.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        var scope = $"{scopeDate}/{region}/s3/aws4_request";

        var signedHeaders = headers.Select(h => h.Name.ToLowerInvariant()).Distinct()
            .OrderBy(n => n, StringComparer.Ordinal).ToList();

        // Тот же канонический запрос, что строит верификатор (глава 03 §1)
        var host = headers.FirstOrDefault(h => string.Equals(h.Name, "host", StringComparison.OrdinalIgnoreCase))
            .Value ?? string.Empty;
        var model = new S3RequestModel
        {
            Method = method.ToUpperInvariant(),
            RawPath = path,
            RawQuery = canonicalQuery,
            Headers = S3HeaderCollection.FromPairs(headers),
            Host = host,
            OpenBody = () => Stream.Null,
        };
        var canonical = CanonicalRequestBuilder.Build(model, signedHeaders, payloadString);
        var stringToSign = SigV4Core.StringToSign(amzDate, scope, canonical);
        var signingKey = SigV4Core.SigningKey(secretKey, scopeDate, region);
        var signature = SigV4Core.SignHex(signingKey, stringToSign);

        var authorization = $"{SigV4Core.Algorithm} Credential={accessKey}/{scope}, " +
            $"SignedHeaders={string.Join(";", signedHeaders)}, Signature={signature}";
        return (authorization, amzDate, payloadString);
    }
}
