using System.Security.Cryptography;
using System.Text;

namespace OwnS3.Protocol.Auth;

// Ядро SigV4 (arch/owns3/03 §1): string-to-sign, signing key, hex-подпись.
public static class SigV4Core
{
    public const string Algorithm = "AWS4-HMAC-SHA256";
    public const string EmptySha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    public static string StringToSign(string amzDate, string scope, string canonicalRequest) =>
        $"{Algorithm}\n{amzDate}\n{scope}\n{HexSha256(Encoding.UTF8.GetBytes(canonicalRequest))}";

    // Цепочка HMAC-SHA256: AWS4<secret> → date → region → s3 → aws4_request.
    public static byte[] SigningKey(string secretKey, string date, string region)
    {
        var key = HMAC(Encoding.UTF8.GetBytes("AWS4" + secretKey), Encoding.UTF8.GetBytes(date));
        key = HMAC(key, Encoding.UTF8.GetBytes(region));
        key = HMAC(key, Encoding.UTF8.GetBytes("s3"));
        return HMAC(key, Encoding.UTF8.GetBytes("aws4_request"));
    }

    public static string SignHex(byte[] key, string stringToSign) =>
        Convert.ToHexString(HMAC(key, Encoding.UTF8.GetBytes(stringToSign))).ToLowerInvariant();

    public static string HexSha256(byte[] data) => Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();

    public static byte[] HexToBytes(string hex) => Convert.FromHexString(hex);

    private static byte[] HMAC(byte[] key, byte[] data) => HMACSHA256.HashData(key, data);
}
