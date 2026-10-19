using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using EtcdWorker.App.Api.Operations;

namespace EtcdWorker.App.Api;

// HTTP API воркера etcd-домена (arch/25 §1.1, фаза 5): мутации состава
// принимает исполнитель. Каждый эндпоинт в финале инкрементирует
// etcdworker_api_operations_total фактическим кодом (EtcdApiMetrics.Track —
// успех и ошибки).
public static class ApiModule
{
    public static IEndpointRouteBuilder MapWorkerApi(this IEndpointRouteBuilder endpoints)
    {
        var metrics = endpoints.ServiceProvider.GetRequiredService<EtcdApiMetrics>();

        // PUT /api/etcd/config — 200 / 400 (N вне 0..7) / 409 (кворум-гвард) /
        // 503 (etcd-отказ/RMW-гонка — ретрай клиентом).
        endpoints.MapPut("/api/etcd/config", async (
            PutEtcdConfigRequest request, PutEtcdConfigHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(request, ct);
            var response = result.IsSuccess
                ? Results.Ok(new { nodes = request.Nodes })
                : result.Error switch
                {
                    EtcdConfigValidationException => Results.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        title: "Validation failed", detail: result.Error.Message),
                    EtcdQuorumGuardException => Results.Problem(
                        statusCode: StatusCodes.Status409Conflict,
                        title: "Unsafe membership", detail: result.Error.Message),
                    _ => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable,
                        title: "Etcd unavailable", detail: result.Error!.Message),
                };
            metrics.Track("put-etcd-config", GetStatusCode(response));
            return response;
        });

        // POST /api/restart — 202 graceful self-stop (arch/14 §1.1).
        endpoints.MapPost("/api/restart", (HttpRequest http, RestartHandler handler) =>
        {
            var dto = handler.Handle(RequestedBy(http));
            return Results.Accepted();
            // Оператор панели (fallback "api" — заголовок X-Requested-By).
            static string? RequestedBy(HttpRequest r)
                => r.Headers.TryGetValue("X-Requested-By", out var v) && v.Count > 0 ? v[0] : null;
        });

        return endpoints;
    }

    private static int GetStatusCode(IResult result)
        => result switch
        {
            IStatusCodeHttpResult withCode => withCode.StatusCode ?? 500,
            _ => 500,
        };
}
