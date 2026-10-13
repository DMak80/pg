using OwnS3.App.Access;
using OwnS3.App.Routing;
using OwnS3.Protocol.Requests;

namespace OwnS3.App.Pipeline;

// Контекст обработки S3-запроса: вход хендлеров (задача 10) — маршрут,
// идентичность, requestId и транспортная модель.
public sealed class RequestContext
{
    public required string RequestId { get; init; }
    public required S3Route Route { get; init; }
    public required AuthenticatedIdentity Identity { get; init; }
    public required S3RequestModel Model { get; init; }

    // Сырой nullable OwnS3Options.HostId (как middleware-ветки писали в S3Error):
    // null → пустой <HostId></HostId> в XML-ошибке — байт-в-байт как сегодня;
    // MachineName-фолбэк — только originHostId репликации (DI, x-owns3-repl-origin).
    public required string? HostId { get; init; }
}
