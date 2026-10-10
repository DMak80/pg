using Microsoft.AspNetCore.Http;
using OwnS3.App.Pipeline;
using OwnS3.App.Routing;
using OwnS3.Storage;

namespace OwnS3.App.Handlers;

// Каркас хендлера t36 (spec §3.4.1): полный протокольный контур — парсинг →
// валидация → (оборачивание тела) → IObjectStore → сборка ответа. Финальный шаг
// в t36 — заглушка (500 InternalError); t37/t38 подставляют реализацию без
// изменения контура.
public interface IOperationHandler
{
    S3Operation Operation { get; }
    Task HandleAsync(S3HandlerContext context, CancellationToken ct);
}

public abstract class OperationHandlerBase(IObjectStore store) : IOperationHandler
{
    protected readonly IObjectStore Store = store;

    public abstract S3Operation Operation { get; }

    public abstract Task HandleAsync(S3HandlerContext context, CancellationToken ct);
}

// Контекст хендлера: HTTP + протокольный контекст конвейера + ответные хелперы.
public sealed class S3HandlerContext
{
    private readonly HttpContext _http;

    public S3HandlerContext(HttpContext http, RequestContext request)
    {
        _http = http;
        Request = request;
    }

    public RequestContext Request { get; }
    public S3Route Route => Request.Route;
    public string Bucket => Route.Bucket ?? string.Empty;
    public string Key => Route.Key ?? string.Empty;
    public HttpRequest Http => _http.Request;
    public HttpResponse Response => _http.Response;

    public bool TryGetChunkedContext(out OwnS3.Protocol.Auth.AwsChunkedReadingContext? chunked)
    {
        if (_http.Items.TryGetValue(S3Authenticator.ChunkedContextKey, out var value)
            && value is OwnS3.Protocol.Auth.AwsChunkedReadingContext context)
        {
            chunked = context;
            return true;
        }
        chunked = null;
        return false;
    }

    public async Task WriteXmlAsync(string xml, CancellationToken ct, int status = 200)
    {
        Response.StatusCode = status;
        Response.ContentType = "application/xml";
        await Response.WriteAsync(xml, ct);
    }

    // Канонический формат дат S3 (ISO8601, мс, Z).
    public static string FormatDate(DateTimeOffset utc) =>
        utc.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", System.Globalization.CultureInfo.InvariantCulture);
}
