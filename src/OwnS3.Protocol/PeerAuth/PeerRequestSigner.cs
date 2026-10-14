using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Requests;

namespace OwnS3.Protocol.PeerAuth;

// Исходящая прикладная подпись replication-грани для key-записей (глава 07 §6):
// канонизация — CanonicalRequestBuilder (переиспользование SigV4, не дублирование),
// string-to-sign "OWNS3-PEER-ECC-P256-SHA256\n<date>\n<hex-sha256(canonical)>",
// подпись ECDSA P-256 P1363 base64.
public static class PeerRequestSigner
{
    public const string Algorithm = "OWNS3-PEER-ECC-P256-SHA256";

    // Фиксированный список подписанных заголовков схемы (глава 07 §6).
    public static readonly string[] SignedHeaderNames =
        ["host", "x-owns3-peer-date", "x-owns3-peer-content-sha256"];

    public static (string Authorization, string PeerDate, string ContentSha256) Sign(
        ECDsa privateKey, string peerName, string method, string path, string canonicalQuery,
        IReadOnlyList<(string Name, string Value)> headers, string payloadString, DateTimeOffset at)
    {
        var peerDate = at.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var host = headers.FirstOrDefault(h => string.Equals(h.Name, "host", StringComparison.OrdinalIgnoreCase))
            .Value ?? string.Empty;
        var model = new S3RequestModel
        {
            Method = method.ToUpperInvariant(), RawPath = path, RawQuery = canonicalQuery,
            Headers = S3HeaderCollection.FromPairs(headers), Host = host, OpenBody = () => Stream.Null,
        };
        var canonical = CanonicalRequestBuilder.Build(model, SignedHeaderNames, payloadString);
        var stringToSign = $"{Algorithm}\n{peerDate}\n{SigV4Core.HexSha256(Encoding.UTF8.GetBytes(canonical))}";
        var signature = Convert.ToBase64String(privateKey.SignData(Encoding.UTF8.GetBytes(stringToSign),
            HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation));
        var signedHeaders = string.Join(";", SignedHeaderNames);
        var authorization = $"{Algorithm} Credential={peerName}, " +
            $"SignedHeaders={signedHeaders}, Signature={signature}";
        return (authorization, peerDate, payloadString);
    }
}
