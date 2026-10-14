using System.Security.Cryptography;
using System.Text;

namespace S3Worker.Core.OwnS3;

/// <summary>
/// Клиентская SigV4-подпись запроса без payload (админ-грань ownS3
/// /admin/storage — owns3/05 §7; админ-грань репликации arch/24 §5 G/зонд
/// arch/24 §5 F; то же ядро, что OwnS3.Protocol SigV4Core,
/// но сторона клиента — без ссылки воркера на серверный протокол).
/// Canonical request: METHOD / canonical URI / canonical query (пустая,
/// либо CanonicalQuery для GET с query — sync-report) /
/// host;x-amz-content-sha256;x-amz-date / payload — UNSIGNED-PAYLOAD
/// (эталонный клиент ownS3, owns3/05 §7; пустой hex-хэш тела сервером
/// тоже принимается, но эталон — unsigned).
/// </summary>
public static class SigV4GetSigner
{
    public const string Algorithm = "AWS4-HMAC-SHA256";

    public const string UnsignedPayload = "UNSIGNED-PAYLOAD";

    public sealed record SignedGet(
        string Authorization,
        string AmzDate,
        string ContentSha256);

    /// <summary>Подпись GET (историческая сигнатура админ-грани; совместимость).</summary>
    public static SignedGet Sign(
        string uriPath,
        string hostHeader,
        string accessKey,
        string secretKey,
        string region = "us-east-1",
        DateTimeOffset? now = null)
        => SignRequest("GET", uriPath, hostHeader, accessKey, secretKey, region, now);

    /// <summary>Подпись произвольного метода (POST/DELETE/PUT — админ-грань
    /// репликации arch/24 §5 G, spec §5.1): canonical request строится с фактическим
    /// методом; payload — UNSIGNED-PAYLOAD (эталонный клиент ownS3, owns3/05 §7).
    /// Отдельное имя (не перегрузка Sign) — устранение неоднозначности с GET-версией.
    /// canonicalQuery — уже канонизованная query-строка (без ведущего '?';
    /// CanonicalQuery), входит в подпись третьей строкой canonical request —
    /// серверный CanonicalRequestBuilder включает RawQuery (owns3/03 §1).</summary>
    public static SignedGet SignRequest(
        string method,
        string uriPath,
        string hostHeader,
        string accessKey,
        string secretKey,
        string region = "us-east-1",
        DateTimeOffset? now = null,
        string? canonicalQuery = null)
    {
        var instant = now ?? DateTimeOffset.UtcNow;
        var amzDate = instant.ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'", System.Globalization.CultureInfo.InvariantCulture);
        var date = amzDate[..8];
        const string payloadHash = UnsignedPayload;

        // Canonical request: METHOD \n canonicalURI \n canonicalQuery \n
        // canonicalHeaders (каждая строка с \n, блок завершается пустой строкой —
        // терминатор заголовков) \n signedHeaders \n payloadHash.
        var canonicalUri = UriEncodingPath(uriPath);
        var canonicalRequest =
            $"{method.ToUpperInvariant()}\n{canonicalUri}\n{canonicalQuery ?? ""}\n" +
            $"host:{hostHeader}\n" +
            $"x-amz-content-sha256:{payloadHash}\n" +
            $"x-amz-date:{amzDate}\n" +
            "\n" +
            "host;x-amz-content-sha256;x-amz-date\n" +
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

    // Каноническая query (RFC 3986, пары отсортированы по имени Ordinal):
    // точка совместимости с CanonicalRequestBuilder сервера (значения null/пусто не шлются).
    public static string CanonicalQuery(params (string Name, string? Value)[] pairs)
        => string.Join("&", pairs
            .Where(p => !string.IsNullOrEmpty(p.Value))
            .OrderBy(p => p.Name, StringComparer.Ordinal)
            .Select(p => $"{Uri.EscapeDataString(p.Name)}={Uri.EscapeDataString(p.Value!)}"));

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
