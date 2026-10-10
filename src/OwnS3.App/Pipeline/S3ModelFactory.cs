using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using OwnS3.Protocol.Requests;

namespace OwnS3.App.Pipeline;

// Сборка транспортно-независимой модели из HttpRequest (spec §3.2): сырые
// путь/query без декодирования, регистронезависимые заголовки, ленивое тело.
public static class S3ModelFactory
{
    public static S3RequestModel Create(HttpContext http)
    {
        // Сырая строка request-line — приоритетный источник пути/query для
        // подписи: IHttpRequestFeature.RawTarget хранит байты как прислал
        // клиент (Kestrel заполняет всегда); Request.Path/QueryString НЕсут
        // частично декодированные значения и для canonical URI не годятся.
        // TestServer RawTarget не заполняет — откат на escaped Path/QueryString
        // (PathString хранит percent-кодированную форму без декодирования).
        string rawPath;
        string rawQuery;
        if (http.Features.Get<IHttpRequestFeature>()?.RawTarget is { Length: > 0 } rawTarget)
        {
            var queryIndex = rawTarget.IndexOf('?');
            rawPath = queryIndex < 0 ? rawTarget : rawTarget[..queryIndex];
            rawQuery = queryIndex < 0 ? string.Empty : rawTarget[(queryIndex + 1)..];
        }
        else
        {
            rawPath = http.Request.Path.Value ?? "/";
            rawQuery = http.Request.QueryString.Value?.TrimStart('?') ?? string.Empty;
        }

        var headers = S3HeaderCollection.FromPairs(
            http.Request.Headers.SelectMany(
                kv => kv.Value.Select(v => (kv.Key ?? string.Empty, v ?? string.Empty))).ToList());

        return new S3RequestModel
        {
            Method = http.Request.Method,
            RawPath = rawPath.Length > 0 ? rawPath : "/",
            RawQuery = rawQuery,
            Host = http.Request.Host.Value ?? string.Empty,
            Headers = headers,
            // Kestrel-поток читается один раз — лениво, по требованию Protocol.
            OpenBody = () => http.Request.Body,
        };
    }
}
