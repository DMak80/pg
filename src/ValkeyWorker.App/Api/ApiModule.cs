using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using ValkeyWorker.App.Api.Operations;

namespace ValkeyWorker.App.Api;

// HTTP API воркера valkey-домена (arch/21 §1.1): мутации декларативного
// контракта /valkey/ принимает исполнитель. Маппинг исключений — порт
// ApiModule kfw (коды/тексты ProblemDetails 1:1); успешные POST/PUT отвечают
// 201/200 без Location (панель строит Location сама).
public static class ApiModule
{
    public static IEndpointRouteBuilder MapWorkerApi(this IEndpointRouteBuilder endpoints)
    {
        // POST /api/valkey/clusters — создание кластера (spec §4.7); 201/409/400.
        endpoints.MapPost("/api/valkey/clusters", async (
            CreateValkeyClusterRequest request, CreateClusterHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(request, ct);
            if (result.IsSuccess)
                return Results.Created((string?)null, result.Value);

            return result.Error switch
            {
                ValkeyValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    // Канон ProblemDetails (RFC 9457): errors.<field> — массив.
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ValkeyClusterAlreadyExistsException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict,
                    title: "Cluster already exists",
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

        // DELETE /api/valkey/clusters/{cluster} — перевод в TO_REMOVE (202:
        // демонтаж исполнит процесс B); идемпотентен; 404 «не найден».
        endpoints.MapDelete("/api/valkey/clusters/{cluster}", async (
            string cluster, DeleteClusterHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, ct);
            if (result.IsSuccess)
                return Results.Accepted();

            return result.Error switch
            {
                ValkeyClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidValkeyConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // PUT /api/valkey/clusters/{cluster}/config — конфиг-мутация
        // maxmemory_bytes/maxmemory_policy (converge D применит); 200/404/400.
        endpoints.MapPut("/api/valkey/clusters/{cluster}/config", async (
            string cluster, ValkeyConfigUpdateRequest request, UpdateConfigHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, request, ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);

            return result.Error switch
            {
                ValkeyValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ValkeyClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                ValkeyConcurrentWriteException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Concurrent write",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidValkeyConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // PUT /api/valkey/clusters/{cluster}/nodes/{node}/resources — лимиты
        // ноды (v1 node1); применит автоконверге надзора; 200/404/400.
        endpoints.MapPut("/api/valkey/clusters/{cluster}/nodes/{node}/resources", async (
            string cluster, string node, ValkeyResourcesUpdateRequest request,
            UpdateResourcesHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, node, request, ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);

            return result.Error switch
            {
                ValkeyValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ValkeyClusterNotFoundException or ValkeyNodeNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidValkeyConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // POST /api/valkey/clusters/{cluster}/password/rotate — заявка ротации
        // (окно двух паролей исполняет процесс E); 202/400 (role)/404/409.
        endpoints.MapPost("/api/valkey/clusters/{cluster}/password/rotate", async (
            string cluster, RotateValkeyPasswordRequest request, HttpRequest http,
            RotatePasswordHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(
                cluster, request.Role ?? "", RequestedBy(http), ct);
            if (result.IsSuccess)
                return Results.Accepted((string?)null, result.Value);

            return result.Error switch
            {
                // t03 (фикс): роль вне канона app|admin — валидация, как у
                // create/config/resources: 400 с errors.<field> (02 §11.2).
                ValkeyValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ValkeyClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                ValkeyClusterNotActiveException or ValkeyRotationAlreadyRequestedException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Rotation rejected",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidValkeyConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // POST /api/seed/demo — стендовый сид за флагом (идемпотентен).
        endpoints.MapPost("/api/seed/demo", async (SeedDemoHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(ct);
            if (result.IsSuccess)
                return result.Value.Seeded
                    ? Results.Created((string?)null, result.Value)
                    : Results.Ok(result.Value);

            return result.Error switch
            {
                WorkerApiNotFoundException notFound => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found",
                    detail: notFound.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // POST /api/restart — graceful self-stop; 202 {"restarting":true}.
        endpoints.MapPost("/api/restart", (HttpRequest http, RestartHandler handler) =>
        {
            var result = handler.Handle(RequestedBy(http));
            return Results.Accepted((string?)null, result);
        });

        return endpoints;
    }

    // Оператор панели (fallback "api" — у панели ClaimsPrincipal).
    private static string RequestedBy(HttpRequest http)
        => http.Headers.TryGetValue("X-Requested-By", out var value) && value.Count > 0
            ? value[0]!
            : "api";
}
