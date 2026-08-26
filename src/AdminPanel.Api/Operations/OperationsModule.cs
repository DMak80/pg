using System.Net.Http.Json;
using System.Security.Claims;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure.CQRS;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AdminPanel.Api.Operations;

// Модуль операций (мутирующие эндпоинты): POST /api/clusters — создание
// (arch/03 §1.1), DELETE /api/clusters/{name} — перевод в TO_REMOVE
// (arch/03 §1.2), POST/DELETE /api/clusters/{cluster}/shards… — добавление
// и демонтаж шарда (arch/03 §1.3/§1.4, t06), POST /api/clusters/{cluster}/moves —
// заявки на переезды (arch/03 §1.5). InspectionModule остаётся read-only.
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

        // POST /api/clusters/{cluster}/shards — добавить шард Active-кластеру (02 §9.5, t06).
        endpoints.MapPost("/api/clusters/{cluster}/shards", async (
            string cluster, AddShardRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<AddShardCommand, ShardAddedDto>(
                new AddShardCommand(cluster, request), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/clusters/{cluster}/shards/{result.Value.Name}", result.Value);

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

        // DELETE /api/clusters/{cluster}/shards/{shard} — маркер демонтажа (02 §9.6, t06);
        // 204 идемпотентен; 404/409/503.
        endpoints.MapDelete("/api/clusters/{cluster}/shards/{shard}", async (
            string cluster, string shard, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<DeleteShardCommand, ShardDeletedDto>(
                new DeleteShardCommand(cluster, shard), ct);
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

        // POST /api/clusters/{cluster}/moves — заявки на переезды бакетов (02 §9.7, 03 §1.5):
        // txn-клэйм per key; сбой посередине без компенсации — повтор досдаст остаток.
        endpoints.MapPost("/api/clusters/{cluster}/moves", async (
            string cluster, MoveBucketsRequest request, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<MoveBucketsCommand, MovesQueuedDto>(
                new MoveBucketsCommand(
                    cluster, request.From, request.To, request.Buckets ?? [],
                    user.Identity?.Name ?? "adminpanel"), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/clusters/{cluster}", result.Value);

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

        // POST /api/ha/{scope}/nodes/{node}/recreate — маркер пересоздания ноды
        // (TO_RECREATE) с режимом soft|hard (нет тела — soft); NodeSupervisor
        // PgWorker выполнит rebuild.
        endpoints.MapPost("/api/ha/{scope}/nodes/{node}/recreate", async (
            string scope, string node, HttpRequest http, IHandler handler, CancellationToken ct) =>
        {
            // Тело опционально (обратная совместимость: POST без тела = soft);
            // битый JSON — 400, а не 500.
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
            var result = await handler.HandleCommand<RecreateNodeCommand, NodeRecreatedDto>(
                new RecreateNodeCommand(scope, node, body?.Mode), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/ha/{scope}", result.Value);

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
