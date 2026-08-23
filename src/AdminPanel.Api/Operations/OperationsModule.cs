using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure.CQRS;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AdminPanel.Api.Operations;

// Модуль операций (мутирующие эндпоинты): POST /api/clusters — создание
// (arch/03 §1.1), DELETE /api/clusters/{name} — перевод в TO_REMOVE
// (arch/03 §1.2). InspectionModule остаётся read-only (spec t12 §8.16).
public static class OperationsModule
{
    public static IEndpointRouteBuilder MapOperationsApi(this IEndpointRouteBuilder endpoints)
    {
        // POST /api/clusters — создание кластера (auth-guard /api/* уже закрыл, arch/03 §1).
        endpoints.MapPost("/api/clusters", async (
            CreateClusterRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<CreateClusterCommand, ClusterCreatedDto>(
                new CreateClusterCommand(request), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/clusters/{result.Value.Name}", result.Value);

            return result.Error switch
            {
                CreateClusterValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    // Канон ProblemDetails (RFC 9457): errors.<field> — МАССИВ сообщений
                    // (как Mvc ValidationProblemDetails); тест 6.1 читает GetArrayLength().
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

        // DELETE /api/clusters/{name} — перевод в TO_REMOVE (arch/02 §9.4, arch/03 §1.2);
        // 204 без тела, идемпотентен; 404 «не найден», прочие отказы — 503.
        endpoints.MapDelete("/api/clusters/{name}", async (
            string name, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<DeleteClusterCommand, ClusterDeletedDto>(
                new DeleteClusterCommand(name), ct);
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
