using System.Security.Cryptography;
using System.Text;

namespace S3Worker.Core.OwnS3;

/// <summary>
/// Клиентская SigV4-подпись GET-запроса без payload (админ-грань ownS3
/// /admin/storage — owns3/05 §7; то же ядро, что OwnS3.Protocol SigV4Core,
/// но сторона клиента — без ссылки воркера на серверный протокол).
/// Canonical request: GET / canonical URI / пустой query /
/// host;x-amz-content-sha256;x-amz-date / x-amz-content-sha256 = SHA256
/// пустого тела.
/// </summary>
public static class SigV4GetSigner
{
    public const string Algorithm = "AWS4-HMAC-SHA256";

    public sealed record SignedGet(
        string Authorization,
        string AmzDate,
        string ContentSha256);

    /// <summary>Подпись GET: URI-путь (без query), host:port, креды, регион,
    /// время UTC (по умолчанию — сейчас).</summary>
    public static SignedGet Sign(
        string uriPath,
        string hostHeader,
        string accessKey,
        string secretKey,
        string region = "us-east-1",
        DateTimeOffset? now = null)
    {
        var instant = now ?? DateTimeOffset.UtcNow;
        var amzDate = instant.ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'", System.Globalization.CultureInfo.InvariantCulture);
        var date = amzDate[..8];
        var payloadHash = HexSha256([]);

        // Canonical request: method \n canonicalURI \n canonicalQuery \n
        // canonicalHeaders \n signedHeaders \n payloadHash.
        var canonicalUri = UriEncodingPath(uriPath);
        var canonicalRequest =
            $"GET\n{canonicalUri}\n\n" +
            $"host:{hostHeader}\n" +
            $"x-amz-content-sha256:{payloadHash}\n" +
            $"x-amz-date:{amzDate}\n" +
            $"host;x-amz-content-sha256;x-amz-date\n" +
            payloadHash;

        var scope = $"{date}/{region}/s3/aws4_request";
        var stringToSign = $"{Algorithm}\n{amzDate}\n{scope}\n{HexSha256(Encoding.UTF8.GetBytes(canonicalRequest))}";
        var signature = SignHex(SigningKey(secretKey, date, region), stringToSign);

        var authorization =
            $"{Algorithm} Credential={accessKey}/{scope}, " +
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date, " +
            $"Signature={signature}";

        return new SignedGet(authorization, amzDate, payloadHash);
    }

    public static string HexSha256(byte[] data) => Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();

    public static byte[] SigningKey(string secretKey, string date, string region)
    {
        var key = Hmac(Encoding.UTF8.GetBytes("AWS4" + secretKey), Encoding.UTF8.GetBytes(date));
        key = Hmac(key, Encoding.UTF8.GetBytes(region));
        key = Hmac(key, Encoding.UTF8.GetBytes("s3"));
        return Hmac(key, Encoding.UTF8.GetBytes("aws4_request"));
    }

    public static string SignHex(byte[] key, string stringToSign)
        => Convert.ToHexString(Hmac(key, Encoding.UTF8.GetBytes(stringToSign))).ToLowerInvariant();

    private static byte[] Hmac(byte[] key, byte[] data) => HMACSHA256.HashData(key, data);

    // Canonical URI: посегментное percent-encoding зарезервированных (пути
    // админ-грани — только ASCII-буквы/слэш; функция полная на будущее).
    private static string UriEncodingPath(string path)
    {
        var builder = new StringBuilder(path.Length);
        foreach (var ch in path)
        {
            if (char.IsAsciiLetterOrDigit(ch) || ch is '/' or '-' or '.' or '_' or '~')
                builder.Append(ch);
            else
                builder.Append('%').Append(((int)ch).ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
        }

        return builder.ToString();
    }
}
