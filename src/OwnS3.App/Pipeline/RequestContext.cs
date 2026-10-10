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
}
