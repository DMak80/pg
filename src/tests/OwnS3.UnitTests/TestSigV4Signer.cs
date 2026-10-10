using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace OwnS3.UnitTests;

// Независимый SigV4-signer тестов (spec §3.5, источник векторов (б)): собственная
// канонизация и HMAC-цепочка; код OwnS3.Protocol не переиспользует. Служит
// «вторым клиентом» против верификатора и строителем aws-chunked-тел.
public static class TestSigV4Signer
{
    public static readonly DateTimeOffset DefaultDate = new(2013, 5, 24, 0, 0, 0, TimeSpan.Zero);

    public static string HexSha256(byte[] data) => Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();

    public static string AmzDateOf(DateTimeOffset date) =>
        date.ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);

    public static string ScopeOf(DateTimeOffset date, string region) =>
        $"{AmzDateOf(date)[..8]}/{region}/s3/aws4_request";

    public static byte[] SigningKey(string secret, DateTimeOffset date, string region)
    {
        // HMAC-цепочка AWS4<secret> → date → region → s3 → aws4_request.
        var key = Hmac(Encoding.UTF8.GetBytes("AWS4" + secret), Encoding.UTF8.GetBytes(AmzDateOf(date)[..8]));
        key = Hmac(key, Encoding.UTF8.GetBytes(region));
        key = Hmac(key, Encoding.UTF8.GetBytes("s3"));
        return Hmac(key, Encoding.UTF8.GetBytes("aws4_request"));
    }

    public static string Sign(string secret, string stringToSign, DateTimeOffset date, string region) =>
        Convert.ToHexString(Hmac(SigningKey(secret, date, region), Encoding.UTF8.GetBytes(stringToSign)))
            .ToLowerInvariant();

    // Trimall стандарта SigV4: усечение краёв + схлопывание внутренних пробелов.
    public static string TrimAll(string value) =>
        string.Join(" ", value.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));

    // Заголовочная подпись: canonical request строится из переданных частей.
    public static string HeaderSignature(string secret, string method, string canonicalUri,
        string canonicalQuery, IEnumerable<(string Name, string Value)> headers, string payloadString,
        DateTimeOffset date, string region)
    {
        var sorted = headers
            .Select(h => (Name: h.Name.ToLowerInvariant(), Value: TrimAll(h.Value)))
            .OrderBy(h => h.Name, StringComparer.Ordinal)
            .ToList();
        var canonicalHeaders = string.Concat(sorted.Select(h => $"{h.Name}:{h.Value}\n"));
        var signedHeaders = string.Join(";", sorted.Select(h => h.Name));

        var canonicalRequest = string.Join("\n",
            method, canonicalUri, canonicalQuery, canonicalHeaders, signedHeaders, payloadString);
        var amzDate = AmzDateOf(date);
        var stringToSign =
            $"AWS4-HMAC-SHA256\n{amzDate}\n{ScopeOf(date, region)}\n{HexSha256(Encoding.UTF8.GetBytes(canonicalRequest))}";
        return Sign(secret, stringToSign, date, region);
    }

    // Готовый Authorization-заголовок по подписи и списку подписанных имён.
    public static string BuildAuthorization(string accessKey, string region, DateTimeOffset date,
        IEnumerable<string> signedHeaderNames, string signature) =>
        $"AWS4-HMAC-SHA256 Credential={accessKey}/{AmzDateOf(date)[..8]}/{region}/s3/aws4_request, " +
        $"SignedHeaders={string.Join(";", signedHeaderNames.Select(n => n.ToLowerInvariant()).OrderBy(n => n, StringComparer.Ordinal))}, " +
        $"Signature={signature}";

    // Чанковая цепочка (глава 03 §3): подпись чанка от предыдущей подписи.
    public static string ChunkSignature(string secret, string prevSignature, byte[] chunk,
        DateTimeOffset date, string region)
    {
        var amzDate = AmzDateOf(date);
        var stringToSign =
            $"AWS4-HMAC-SHA256-PAYLOAD\n{amzDate}\n{ScopeOf(date, region)}\n{prevSignature}\n" +
            $"{HexSha256([])}\n{HexSha256(chunk)}";
        return Sign(secret, stringToSign, date, region);
    }

    // Подпись трейлера (глава 03 §3): от подписи 0-чанка по trailer-строке.
    public static string TrailerSignature(string secret, string prevSignature, string trailerString,
        DateTimeOffset date, string region)
    {
        var amzDate = AmzDateOf(date);
        var stringToSign =
            $"AWS4-HMAC-SHA256-TRAILER\n{amzDate}\n{ScopeOf(date, region)}\n{prevSignature}\n" +
            $"{HexSha256(Encoding.UTF8.GetBytes(trailerString))}";
        return Sign(secret, stringToSign, date, region);
    }

    // Сборка aws-chunked-тела (фрейминг + трейлеры) для ридера и интеграционных.
    public static byte[] BuildChunkedBody(string secret, byte[] data, string seedSignature,
        DateTimeOffset date, string region, string? trailerName = null, byte[]? trailerChecksum = null,
        int chunkSize = int.MaxValue)
    {
        var body = new MemoryStream();
        var prev = seedSignature;

        // Чанки данных (пустое тело — сразу финальный 0-чанк).
        for (var offset = 0; offset < data.Length; offset += chunkSize)
        {
            var chunk = data[offset..Math.Min(offset + chunkSize, data.Length)];
            var sig = ChunkSignature(secret, prev, chunk, date, region);
            WriteFrame(body, chunk.Length, sig, chunk);
            prev = sig;
        }

        var zeroSig = ChunkSignature(secret, prev, [], date, region);
        WriteFrame(body, 0, zeroSig, []);

        if (trailerName is not null)
        {
            var value = trailerChecksum is null ? string.Empty : Convert.ToBase64String(trailerChecksum);
            // Строка трейлера в подписи — с одним завершающим \n (референсная нормализация).
            var trailerString = $"{trailerName}:{value}\n";
            var trailerSig = TrailerSignature(secret, zeroSig, trailerString, date, region);
            body.Write(Encoding.ASCII.GetBytes($"{trailerName}:{value}\r\n"));
            body.Write(Encoding.ASCII.GetBytes($"x-amz-trailer-signature:{trailerSig}\r\n"));
        }

        return body.ToArray();
    }

    private static void WriteFrame(MemoryStream body, int size, string signature, byte[] chunk)
    {
        body.Write(Encoding.ASCII.GetBytes($"{size:x};chunk-signature={signature}\r\n"));
        body.Write(chunk);
        body.Write(Encoding.ASCII.GetBytes("\r\n"));
    }

    private static byte[] Hmac(byte[] key, byte[] data) => HMACSHA256.HashData(key, data);
}
