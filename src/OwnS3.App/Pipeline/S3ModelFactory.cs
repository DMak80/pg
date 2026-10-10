using Microsoft.AspNetCore.Http;
using OwnS3.Protocol.Requests;

namespace OwnS3.App.Pipeline;

// Сборка транспортно-независимой модели из HttpRequest (spec §3.2): сырые
// путь/query без декодирования, регистронезависимые заголовки, ленивое тело.
public static class S3ModelFactory
{
    public static S3RequestModel Create(HttpContext http)
    {
        var headers = S3HeaderCollection.FromPairs(
            http.Request.Headers.SelectMany(
                kv => kv.Value.Select(v => (kv.Key ?? string.Empty, v ?? string.Empty))).ToList());

        return new S3RequestModel
        {
            Method = http.Request.Method,
            RawPath = http.Request.Path.Value ?? "/",
            RawQuery = http.Request.QueryString.Value?.TrimStart('?') ?? string.Empty,
            Host = http.Request.Host.Value ?? string.Empty,
            Headers = headers,
            // Kestrel-поток читается один раз — лениво, по требованию Protocol.
            OpenBody = () => http.Request.Body,
        };
    }
}
