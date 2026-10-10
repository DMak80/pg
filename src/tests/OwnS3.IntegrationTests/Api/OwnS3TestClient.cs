using System.Text;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Uris;
using OwnS3.UnitTests;

namespace OwnS3.IntegrationTests.Api;

// Тестовый SigV4-клиент (spec §3.5): полный контроль сырого пути/заголовков
// (HttpClient + HttpRequestMessage); подпись — независимый signer юнитов.
public sealed class OwnS3TestClient(HttpClient http)
{
    public HttpClient Http { get; } = http;

    private string Host => Http.BaseAddress!.Authority;

    public record Credentials(string AccessKey, string SecretKey);

    public static Credentials Reader() => new(OwnS3AppFactory.ReaderAccessKey, OwnS3AppFactory.ReaderSecretKey);
    public static Credentials Writer() => new(OwnS3AppFactory.WriterAccessKey, OwnS3AppFactory.WriterSecretKey);
    public static Credentials Admin() => new(OwnS3AppFactory.AdminAccessKey, OwnS3AppFactory.AdminSecretKey);

    // Заголовочная подпись: path/query как отправляются; payload —
    // UNSIGNED-PAYLOAD или hex-sha256/режимная строка (по параметру).
    public async Task<HttpResponseMessage> SendSignedAsync(string method, string pathAndQuery,
        string? payloadString = null, byte[]? body = null,
        IReadOnlyDictionary<string, string>? headers = null, Credentials? credentials = null,
        DateTimeOffset? at = null, string region = "us-east-1")
    {
        var credentials2 = credentials ?? Writer();
        var date = at ?? DateTimeOffset.UtcNow;
        var payload = payloadString ?? PayloadHashModeClassifier.UnsignedPayloadValue;

        var (path, query) = SplitPathQuery(pathAndQuery);
        var headerList = new List<(string, string)> { ("host", Host) };
        headerList.Add(("x-amz-date", TestSigV4Signer.AmzDateOf(date)));
        headerList.Add(("x-amz-content-sha256", payload));
        if (headers is not null)
        {
            foreach (var (name, value) in headers)
                headerList.Add((name.ToLowerInvariant(), value));
        }

        // Канонический query — из сырой строки (форм-декод → RFC 3986, как у сервера).
        var canonicalQuery = OwnS3.Protocol.Auth.CanonicalRequestBuilder.QueryPairs(query) is { } pairs
            ? UriEncoding.EncodeQuery(pairs)
            : string.Empty;

        var signature = TestSigV4Signer.HeaderSignature(credentials2.SecretKey, method, CanonicalPath(path),
            canonicalQuery, headerList, payload, date, region);
        var authorization = TestSigV4Signer.BuildAuthorization(credentials2.AccessKey, region, date,
            headerList.Select(h => h.Item1).Where(n => n != "host"), signature);

        using var request = new HttpRequestMessage(new System.Net.Http.HttpMethod(method), pathAndQuery);
        var bodyBytes = body ?? [];
        if (bodyBytes.Length > 0)
            request.Content = new ByteArrayContent(bodyBytes);
        foreach (var (lowerName, value) in headerList.Where(h => h.Item1 is not "host"))
        {
            var name = OriginalCase(lowerName, headers);
            // Content-* заголовки живут в Content.Headers (MD5, Encoding, Length).
            if (lowerName.StartsWith("content-", StringComparison.Ordinal) && request.Content is not null)
                request.Content.Headers.TryAddWithoutValidation(name, value);
            else
                request.Headers.TryAddWithoutValidation(name, value);
        }
        request.Headers.TryAddWithoutValidation("Authorization", authorization);

        return await Http.SendAsync(request, TestContext.Current.CancellationToken);
    }

    // Presigned URL: query без X-Amz-Signature + подпись канонического запроса.
    public string BuildPresignedUrl(string method, string pathAndQuery, Credentials credentials,
        long expiresSeconds = 86400, DateTimeOffset? at = null, string region = "us-east-1")
    {
        var date = at ?? DateTimeOffset.UtcNow;
        var amzDate = TestSigV4Signer.AmzDateOf(date);
        var credential = $"{credentials.AccessKey}/{amzDate[..8]}/{region}/s3/aws4_request";
        var pairs = new List<(string Name, string? Value)>
        {
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            ("X-Amz-Credential", credential),
            ("X-Amz-Date", amzDate),
            ("X-Amz-Expires", expiresSeconds.ToString()),
            ("X-Amz-SignedHeaders", "host"),
        };
        var (path, _) = SplitPathQuery(pathAndQuery);
        var canonicalQuery = UriEncoding.EncodeQuery(pairs);
        var signature = TestSigV4Signer.HeaderSignature(credentials.SecretKey, method, CanonicalPath(path),
            canonicalQuery, [("host", Host)], PayloadHashModeClassifier.UnsignedPayloadValue, date, region);

        var encoded = string.Join("&", pairs.Select(p =>
            $"{UriEncoding.EncodeValue(p.Name)}={UriEncoding.EncodeValue(p.Value ?? string.Empty)}"));
        return $"{pathAndQuery}?{encoded}&X-Amz-Signature={signature}";
    }

    public async Task<HttpResponseMessage> SendAnonymousAsync(string method, string pathAndQuery) =>
        await Http.SendAsync(new HttpRequestMessage(new System.Net.Http.HttpMethod(method), pathAndQuery));

    public async Task<HttpResponseMessage> SendPresignedAsync(string method, string presignedUrl) =>
        await Http.SendAsync(new HttpRequestMessage(new System.Net.Http.HttpMethod(method), presignedUrl));

    // Канонический путь клиента = путь как отправлен (уже кодирован при необходимости).
    private static string CanonicalPath(string path) => path;

    private static (string Path, string Query) SplitPathQuery(string pathAndQuery)
    {
        var q = pathAndQuery.IndexOf('?');
        return q < 0 ? (pathAndQuery, string.Empty) : (pathAndQuery[..q], pathAndQuery[(q + 1)..]);
    }

    // Восстановление исходного регистра имён заголовков из словаря клиента.
    private static string OriginalCase(string lowerName, IReadOnlyDictionary<string, string>? original)
    {
        if (original is null)
            return lowerName;
        foreach (var name in original.Keys)
        {
            if (string.Equals(name, lowerName, StringComparison.OrdinalIgnoreCase))
                return name;
        }
        return lowerName;
    }
}
