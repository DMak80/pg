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

        // POST /api/clusters/{cluster}/moves/rollback — заявки на откат (t07, 02 §9.7.2):
        // направление определяет воркер по обратной подписке; общий протокол §9.7 п.1–5.
        endpoints.MapPost("/api/clusters/{cluster}/moves/rollback", async (
            string cluster, RollbackBucketsRequest request, HttpRequest http, RollbackBucketsHandler handler, CancellationToken ct) =>
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
                MoveOpValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: result.Error.Message),
                ClusterNotActiveException or NonShardedClusterException or BucketNotActiveForMoveOpException
                    or MoveRequestConflictException or MoveClaimLostException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Move ops rejected", detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
            };
        });

        // POST /api/clusters/{cluster}/moves/finalize — заявка уборки артефактов
        // (t07, 02 §9.7.3): DROP SCHEMA СО ДАННЫМИ на oldShard — необратимо.
        endpoints.MapPost("/api/clusters/{cluster}/moves/finalize", async (
            string cluster, FinalizeBucketRequest request, HttpRequest http, FinalizeBucketHandler handler, CancellationToken ct) =>
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
                MoveOpValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ClusterNotFoundException or ShardNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: result.Error.Message),
                ClusterNotActiveException or NonShardedClusterException or BucketNotActiveForMoveOpException
                    or FinalizeTargetIsOwnerException or MoveRequestConflictException or MoveClaimLostException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Move ops rejected", detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
            };
        });

        // POST /api/clusters/{cluster}/moves/abort — заявка отмены переезда (t07,
        // 02 §9.7.4): force ломает защиты свежести/routing==target.
        endpoints.MapPost("/api/clusters/{cluster}/moves/abort", async (
            string cluster, AbortBucketRequest request, HttpRequest http, AbortBucketHandler handler, CancellationToken ct) =>
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
                MoveOpValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: result.Error.Message),
                ClusterNotActiveException or NonShardedClusterException or BucketNotActiveForMoveOpException
                    or MoveStatusFreshException or MoveAlreadyFlippedException
                    or MoveRequestConflictException or MoveClaimLostException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Move ops rejected", detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
            };
        });

        // DELETE /api/clusters/{cluster}/moves/{bucket} — отмена стоящей заявки (t07,
        // 02 §9.7.5): 204; ключа нет/имена неканонические → 404; сбой etcd → 503.
        endpoints.MapDelete("/api/clusters/{cluster}/moves/{bucket}", async (
            string cluster, string bucket, CancelMoveHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, bucket, ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return result.Error switch
            {
                ClusterNotFoundException or MoveTicketNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
            };
        });

        // POST /api/clusters/{cluster}/secrets/rotate — заявка ротации per-cluster
        // секретов (02 §9.8, t02): здесь только клэйм заявки; выполнение —
        // ClusterSecretRotator.
        endpoints.MapPost("/api/clusters/{cluster}/secrets/rotate", async (
            string cluster, HttpRequest http, RotateClusterSecretsHandler handler, CancellationToken ct) =>
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

        // POST /api/clusters/{cluster}/backups/policy — per-cluster политика
        // ретенции/бэкапов (t06, arch/19 §4): валидация + put policy-ключа;
        // применяется следующим тиком планировщика/ретенции. Мусорный JSON — 400.
        endpoints.MapPost("/api/clusters/{cluster}/backups/policy", async (
            string cluster, HttpRequest http, BackupsPolicyHandler handler, CancellationToken ct) =>
        {
            string rawBody;
            using (var reader = new StreamReader(http.Body))
                rawBody = await reader.ReadToEndAsync(ct);
            var result = await handler.HandleAsync(cluster, rawBody, ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);

            return result.Error switch
            {
                BackupsPolicyValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
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

        // POST /api/clusters/{cluster}/shards/{shard}/restore — заявка восстановления
        // шарда из бэкапа (t05, arch/19 §3.5/§4): 202 Accepted (не 201 — операция
        // разрушающая, долгая; воркер пишет PLANNED-ключ сам, исполняет держатель
        // клэйма). Гварды: confirm/активная заявка/RFC3339/Active/шард заявлен.
        endpoints.MapPost("/api/clusters/{cluster}/shards/{shard}/restore", async (
            string cluster, string shard, RestoreShardRequest? body, HttpRequest http,
            RestoreShardHandler handler, CancellationToken ct) =>
        {
            if (body is null)
                return Results.Problem(statusCode: StatusCodes.Status400BadRequest,
                    title: "Invalid body", detail: "тело запроса обязательно: {\"confirm\":\"<shard>\", …}");
            var requestedBy = !string.IsNullOrWhiteSpace(body.RequestedBy)
                ? body.RequestedBy
                : http.Headers.TryGetValue("X-Requested-By", out var by) && !string.IsNullOrWhiteSpace(by)
                    ? by.ToString()
                    : "api";
            var result = await handler.HandleAsync(cluster, shard, body, requestedBy, ct);
            if (result.IsSuccess)
                return Results.Accepted((string?)null, result.Value); // 202

            return result.Error switch
            {
                RestoreValidationException validation => Results.Problem(statusCode: 400, title: "Validation failed",
                    detail: result.Error.Message, extensions: new Dictionary<string, object?>
                    { ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }) }),
                ConfirmMismatchException or InvalidTargetTimeException => Results.Problem(statusCode: 400,
                    title: "Restore rejected", detail: result.Error.Message),
                ClusterNotFoundException or ShardNotFoundException => Results.Problem(statusCode: 404,
                    title: "Not found", detail: result.Error.Message),
                ClusterNotActiveException or RestoreAlreadyActiveException => Results.Problem(statusCode: 409,
                    title: "Restore rejected", detail: result.Error.Message),
                EtcdWriteUnavailableException => Results.Problem(statusCode: 503,
                    title: "Etcd write unavailable", detail: result.Error.Message),
                _ => Results.Problem(statusCode: 503, title: "Etcd write failed", detail: result.Error!.Message),
            };
        });

        // POST /api/seed/demo — демо-сид pg-контура (arch/14 §1.1.1, стенд):
        // перенос seed.sh 1:1; идемпотентен по /clusters/demo/config.
        // Флаг EnableSeedEndpoint проверяет хендлер (выключен → 404).
        endpoints.MapPost("/api/seed/demo", async (SeedDemoHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);

            return result.Error switch
            {
                WorkerApiNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: result.Error.Message),
                EtcdWriteUnavailableException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable", detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
            };
        });

        return endpoints;
    }
}
