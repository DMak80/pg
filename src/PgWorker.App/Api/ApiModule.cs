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

        // POST /api/clusters/{cluster}/shards — добавить шард Active-кластеру (02 §9.5).
        endpoints.MapPost("/api/clusters/{cluster}/shards", async (
            string cluster, AddShardRequest request, AddShardHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, request, ct);
            if (result.IsSuccess)
                return Results.Created((string?)null, result.Value);

            return result.Error switch
            {
                AddShardValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found", detail: result.Error.Message),
                ClusterNotActiveException or ShardNameTakenException or ShardLimitReachedException
                    or NonShardedClusterException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Shard add rejected", detail: result.Error.Message),
                EtcdWriteUnavailableException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable", detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
            };
        });

        // DELETE /api/clusters/{cluster}/shards/{shard} — маркер демонтажа (02 §9.6);
        // 204 идемпотентен; 404/409/503.
        endpoints.MapDelete("/api/clusters/{cluster}/shards/{shard}", async (
            string cluster, string shard, DeleteShardHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, shard, ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return result.Error switch
            {
                ClusterNotFoundException or ShardNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: result.Error.Message),
                ClusterNotActiveException or ShardRemoveBlockedException or NonShardedClusterException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Shard remove rejected", detail: result.Error.Message),
                EtcdWriteUnavailableException or ShardPrecheckUnavailableException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable", detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
            };
        });

        // POST /api/clusters/{cluster}/moves — заявки на переезды бакетов (02 §9.7):
        // txn-клэйм per key; сбой посередине без компенсации — повтор досдаст остаток.
        // requested_by — заголовок X-Requested-By (панель шлёт оператора), fallback "api".
        endpoints.MapPost("/api/clusters/{cluster}/moves", async (
            string cluster, MoveBucketsRequest request, HttpRequest http, MoveBucketsHandler handler, CancellationToken ct) =>
        {
            var requestedBy = http.Headers.TryGetValue("X-Requested-By", out var by)
                && !string.IsNullOrWhiteSpace(by)
                ? by.ToString()
                : "api";
            var result = await handler.HandleAsync(cluster, request, requestedBy, ct);
            if (result.IsSuccess)
                return Results.Created((string?)null, result.Value);

            return result.Error switch
            {
                MoveBucketsValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ClusterNotFoundException or ShardNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: result.Error.Message),
                ClusterNotActiveException or NonShardedClusterException or MoveTargetRemovingException
                    or BucketNotOnSourceException or MoveRequestConflictException or MoveClaimLostException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Moves rejected", detail: result.Error.Message),
                EtcdWriteUnavailableException or ShardPrecheckUnavailableException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable", detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
            };
        });

        // POST /api/clusters/{cluster}/app-password/rotate — заявка ротации app-пароля
        // (02 §9.8): здесь только клэйм заявки; выполнение — AppPasswordRotator.
        endpoints.MapPost("/api/clusters/{cluster}/app-password/rotate", async (
            string cluster, HttpRequest http, RotateAppPasswordHandler handler, CancellationToken ct) =>
        {
            var requestedBy = http.Headers.TryGetValue("X-Requested-By", out var by)
                && !string.IsNullOrWhiteSpace(by)
                ? by.ToString()
                : "api";
            var result = await handler.HandleAsync(cluster, requestedBy, ct);
            if (result.IsSuccess)
                return Results.Created((string?)null, result.Value);

            return result.Error switch
            {
                ClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                ClusterNotActiveException or RotationAlreadyRequestedException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Rotation rejected",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // POST /api/ha/{scope}/nodes/{node}/recreate — маркер пересоздания ноды
        // (TO_RECREATE) с режимом soft|hard (нет тела — soft); rebuild выполнит
        // NodeSupervisor. Битый JSON — 400, а не 500.
        endpoints.MapPost("/api/ha/{scope}/nodes/{node}/recreate", async (
            string scope, string node, HttpRequest http, RecreateNodeHandler handler, CancellationToken ct) =>
        {
            RecreateNodeRequest? body = null;
            if (http.HasJsonContentType())
            {
                try
                {
                    body = await http.ReadFromJsonAsync<RecreateNodeRequest>(ct);
                }
                catch (System.Text.Json.JsonException)
                {
                    return Results.Problem(
                        statusCode: StatusCodes.Status400BadRequest, title: "Invalid body",
                        detail: "тело запроса — JSON вида {\"mode\":\"soft|hard\"}");
                }
            }

            var result = await handler.HandleAsync(scope, node, body?.Mode, ct);
            if (result.IsSuccess)
                return Results.Created((string?)null, result.Value);

            return result.Error switch
            {
                ScopeNotFoundException or NodeNotFoundException or ClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: result.Error.Message),
                ClusterNotActiveException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Cluster not active", detail: result.Error.Message),
                LastNodeException or AllOthersRecreatingException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Recreate rejected", detail: result.Error.Message),
                InvalidRecreateModeException => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest, title: "Invalid mode", detail: result.Error.Message),
                EtcdWriteUnavailableException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable", detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
            };
        });

        return endpoints;
    }
}
