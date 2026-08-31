using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using PgWorker.App.Api.Operations;
using PgWorker.Core.Writing;

namespace PgWorker.App.Api;

// HTTP API воркера (arch/14 §1.1, task etcd-via-worker-api): мутации декларативного
// etcd-контракта принимает исполнитель. Маппинг исключений — порт панельного
// OperationsModule 1:1 (коды/тексты ProblemDetails не меняются); успешный POST
// отвечает 201 БЕЗ Location — Location строит панель (прокси).
public static class ApiModule
{
    public static IEndpointRouteBuilder MapWorkerApi(this IEndpointRouteBuilder endpoints)
    {
        // POST /api/clusters — создание кластера (arch/02 §9.1/§9.2).
        endpoints.MapPost("/api/clusters", async (
            CreateClusterRequest request, CreateClusterHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(request, ct);
            if (result.IsSuccess)
                return Results.Created((string?)null, result.Value);

            return result.Error switch
            {
                CreateClusterValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    // Канон ProblemDetails (RFC 9457): errors.<field> — МАССИВ сообщений
                    // (как Mvc ValidationProblemDetails); панель проксирует как есть.
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ClusterAlreadyExistsException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict,
                    title: "Cluster already exists",
                    detail: result.Error!.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // DELETE /api/clusters/{name} — перевод в TO_REMOVE (arch/02 §9.4);
        // 204 без тела, идемпотентен; 404 «не найден», прочие отказы — 503.
        endpoints.MapDelete("/api/clusters/{name}", async (
            string name, DeleteClusterHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(name, ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return result.Error switch
            {
                ClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound,
                    title: "Cluster not found",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        return endpoints;
    }
}
