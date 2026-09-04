using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using KafkaWorker.App.Api.Operations;
using KafkaWorker.Core.Writing;

namespace KafkaWorker.App.Api;

// HTTP API воркера (arch/16 §1.1, task etcd-via-worker-api): мутации
// декларативного etcd-контракта kafka-домена (arch/02 §10.2) принимает
// исполнитель. Маппинг исключений — порт панельного KafkaOperationsModule 1:1
// (коды/тексты ProblemDetails не меняются); успешные POST/PUT отвечают
// 201/200 БЕЗ Location — Location строит панель (прокси).
public static class ApiModule
{
    public static IEndpointRouteBuilder MapWorkerApi(this IEndpointRouteBuilder endpoints)
    {
        // POST /api/kafka/clusters — создание кластера (arch/02 §10.2-1).
        endpoints.MapPost("/api/kafka/clusters", async (
            CreateKafkaClusterRequest request, CreateClusterHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(request, ct);
            if (result.IsSuccess)
                return Results.Created((string?)null, result.Value);

            return result.Error switch
            {
                KafkaValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    // Канон ProblemDetails (RFC 9457): errors.<field> — МАССИВ
                    // сообщений; панель проксирует как есть.
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                KafkaClusterAlreadyExistsException => Results.Problem(
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

        // DELETE /api/kafka/clusters/{cluster} — перевод в TO_REMOVE (§10.2-2);
        // 204 идемпотентен; 404 «не найден», прочие отказы — 503.
        endpoints.MapDelete("/api/kafka/clusters/{cluster}", async (
            string cluster, DeleteClusterHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return result.Error switch
            {
                KafkaClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // PUT /api/kafka/clusters/{cluster}/config — default-конфиги (§10.2-3);
        // успех — 200 с DTO обновлённых полей (панель отвечает Ok; чек 50 шаг 7).
        endpoints.MapPut("/api/kafka/clusters/{cluster}/config", async (
            string cluster, KafkaConfigUpdateRequest request, UpdateConfigHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, request, ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);

            return result.Error switch
            {
                KafkaValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                KafkaClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                KafkaClusterNotActiveException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Cluster not active",
                    detail: result.Error.Message),
                KafkaConcurrentWriteException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Concurrent write",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // POST /api/kafka/clusters/{cluster}/brokers — добавление брокера (§10.2-4).
        endpoints.MapPost("/api/kafka/clusters/{cluster}/brokers", async (
            string cluster, AddKafkaBrokerRequest request, AddBrokerHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, request, ct);
            if (result.IsSuccess)
                return Results.Created((string?)null, result.Value);

            return result.Error switch
            {
                KafkaValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                KafkaClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                KafkaClusterNotActiveException or KafkaBrokerNameTakenException or KafkaBrokerLimitException
                    => Results.Problem(
                        statusCode: StatusCodes.Status409Conflict, title: "Broker add rejected",
                        detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // DELETE /api/kafka/clusters/{cluster}/brokers/{broker} — маркер TO_REMOVE
        // (§10.2-5); 204 идемпотентен; 404/409/503.
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/brokers/{broker}", async (
            string cluster, string broker, DeleteBrokerHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, broker, ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return result.Error switch
            {
                KafkaClusterNotFoundException or KafkaBrokerNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found",
                    detail: result.Error.Message),
                KafkaClusterNotActiveException or KafkaBrokerIsControllerException or KafkaLastBrokerException
                    => Results.Problem(
                        statusCode: StatusCodes.Status409Conflict, title: "Broker remove rejected",
                        detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // PUT /api/kafka/clusters/{cluster}/brokers/{broker}/resources — мутация
        // №15 (t06, 02 §10.2-15): декларация ресурсов; применяет NodeRegenerator
        // воркера rolling-пересозданием (автоконверге, без заявки). Идемпотентен.
        endpoints.MapPut("/api/kafka/clusters/{cluster}/brokers/{broker}/resources", async (
            string cluster, string broker, KafkaResourcesUpdateRequest request,
            UpdateBrokerResourcesHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, broker, request, ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);

            return result.Error switch
            {
                KafkaValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                KafkaClusterNotFoundException or KafkaBrokerNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found",
                    detail: result.Error.Message),
                KafkaClusterNotActiveException or KafkaBrokerRemovalInProgressException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Broker resources update rejected",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // POST /api/kafka/clusters/{cluster}/app-password/rotate — заявка ротации
        // (§10.2-8); исполнение — AppPasswordRotator воркера.
        // requested_by — заголовок X-Requested-By (панель шлёт оператора), fallback "api".
        endpoints.MapPost("/api/kafka/clusters/{cluster}/app-password/rotate", async (
            string cluster, HttpRequest http, RotateAppPasswordHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, RequestedBy(http), ct);
            if (result.IsSuccess)
                return Results.Created((string?)null, result.Value);

            return result.Error switch
            {
                KafkaClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                KafkaClusterNotActiveException or KafkaRotationAlreadyRequestedException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Rotation rejected",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // POST /api/kafka/clusters/{cluster}/admin-password/rotate — мутация №16
        // (adminpanel/02 §10.2); исполнение — PasswordRotator (роль admin).
        // requested_by — заголовок X-Requested-By (панель шлёт оператора), fallback "api".
        endpoints.MapPost("/api/kafka/clusters/{cluster}/admin-password/rotate", async (
            string cluster, HttpRequest http, RotateAdminPasswordHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, RequestedBy(http), ct);
            if (result.IsSuccess)
                return Results.Created((string?)null, result.Value);

            return result.Error switch
            {
                KafkaClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                KafkaClusterNotActiveException or KafkaRotationAlreadyRequestedException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Rotation rejected",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // POST /api/kafka/clusters/{cluster}/rebalance — заявка ребалансировки
        // партиций (§10.2-13); исполнение — PartitionReassigner воркера.
        // requested_by — заголовок X-Requested-By, fallback "api".
        endpoints.MapPost("/api/kafka/clusters/{cluster}/rebalance", async (
            string cluster, HttpRequest http, RebalanceHandler handler, CancellationToken ct) =>
        {
            var result = await handler.RequestAsync(cluster, RequestedBy(http), ct);
            if (result.IsSuccess)
                return Results.Created((string?)null, result.Value);

            return result.Error switch
            {
                KafkaClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                KafkaClusterNotActiveException or KafkaRebalanceAlreadyRequestedException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Rebalance rejected",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // DELETE /api/kafka/clusters/{cluster}/rebalance — отмена ребалансировки
        // (§10.2-14): новые батчи не подаются, поданные Kafka доиграет.
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/rebalance", async (
            string cluster, RebalanceHandler handler, CancellationToken ct) =>
        {
            var result = await handler.CancelAsync(cluster, ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return result.Error switch
            {
                KafkaClusterNotFoundException or KafkaRebalanceNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // PUT /api/kafka/clusters/{cluster}/topics/{topic} — конфиг-заявка
        // (§10.2-6). desired_by — заголовок X-Requested-By, fallback "api".
        endpoints.MapPut("/api/kafka/clusters/{cluster}/topics/{topic}", async (
            string cluster, string topic, TopicDesiredRequest request, HttpRequest http,
            UpdateTopicDesiredHandler handler, CancellationToken ct) =>
        {
            var requestedBy = RequestedBy(http);
            var result = await handler.HandleAsync(cluster, topic, request, requestedBy, ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);

            return result.Error switch
            {
                KafkaValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                KafkaClusterNotFoundException or KafkaTopicNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found",
                    detail: result.Error.Message),
                KafkaClusterNotActiveException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Cluster not active",
                    detail: result.Error.Message),
                KafkaConcurrentWriteException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Concurrent write",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException or InvalidKafkaTopicKeyException
                    => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                        detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // DELETE /api/kafka/clusters/{cluster}/topics/{topic}/desired — отмена
        // конфиг-заявки (§10.2-7): desired=null RMW.
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/topics/{topic}/desired", async (
            string cluster, string topic, DeleteDesiredHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, topic, ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return result.Error switch
            {
                KafkaClusterNotFoundException or KafkaTopicNotFoundException
                    or KafkaTopicDesiredNotFoundException => Results.Problem(
                        statusCode: StatusCodes.Status404NotFound, title: "Not found",
                        detail: result.Error.Message),
                KafkaClusterNotActiveException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Cluster not active",
                    detail: result.Error.Message),
                KafkaConcurrentWriteException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Concurrent write",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException or InvalidKafkaTopicKeyException
                    => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                        detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // POST /api/kafka/clusters/{cluster}/topics — создание топика (§10.2-9);
        // requested_by заявки — заголовок X-Requested-By, fallback "api".
        endpoints.MapPost("/api/kafka/clusters/{cluster}/topics", async (
            string cluster, CreateTopicRequest request, HttpRequest http,
            CreateTopicHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, request, RequestedBy(http), ct);
            if (result.IsSuccess)
                return Results.Created((string?)null, result.Value);

            return result.Error switch
            {
                KafkaValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                KafkaClusterNotFoundException or KafkaTopicNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found",
                    detail: result.Error.Message),
                KafkaClusterNotActiveException or KafkaTopicExistsException
                    or KafkaLifecyclePendingException or KafkaDesiredPendingException => Results.Problem(
                        statusCode: StatusCodes.Status409Conflict, title: "Topic create rejected",
                        detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException or InvalidKafkaTopicKeyException
                    => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                        detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // DELETE /api/kafka/clusters/{cluster}/topics/{topic} — удаление топика
        // (§10.2-10): клэйм desired.delete; идемпотентен.
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/topics/{topic}", async (
            string cluster, string topic, HttpRequest http, DeleteTopicHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, topic, RequestedBy(http), ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return result.Error switch
            {
                KafkaClusterNotFoundException or KafkaTopicNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found",
                    detail: result.Error.Message),
                KafkaClusterNotActiveException or KafkaLifecyclePendingException
                    or KafkaDesiredPendingException => Results.Problem(
                        statusCode: StatusCodes.Status409Conflict, title: "Topic delete rejected",
                        detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException or InvalidKafkaTopicKeyException
                    => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                        detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        // DELETE .../desired.create и .../desired.delete — отмена lifecycle-заявок
        // (§10.2-11/12): успех → 204 (общая ветка, порт панельного модуля).
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/topics/{topic}/desired.create", async (
            string cluster, string topic, CancelLifecycleHandler handler, CancellationToken ct) =>
            await CancelTopicLifecycleAsync(cluster, topic, "create", handler, ct));
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/topics/{topic}/desired.delete", async (
            string cluster, string topic, CancelLifecycleHandler handler, CancellationToken ct) =>
            await CancelTopicLifecycleAsync(cluster, topic, "delete", handler, ct));

        // POST /api/seed/demo — демо-сид kafka-домена (arch/16 §1.1.1, стенд):
        // перенос kafka-seed.sh 1:1; идемпотентен по /kafka/clusters/events/config.
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
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });

        return endpoints;
    }

    // Имя оператора из заголовка X-Requested-By (панель шлёт на прокси-мутациях);
    // fallback "api" — тот же источник, что ClaimsPrincipal у панели (spec §3.7).
    private static string RequestedBy(HttpRequest http)
        => http.Headers.TryGetValue("X-Requested-By", out var by)
            && !string.IsNullOrWhiteSpace(by)
            ? by.ToString()
            : "api";

    // Общий хендлер отмены lifecycle-заявок (§10.2-11/12): успех → 204.
    private static async Task<IResult> CancelTopicLifecycleAsync(
        string cluster, string topic, string op, CancelLifecycleHandler handler, CancellationToken ct)
    {
        var result = await handler.HandleAsync(cluster, topic, op, ct);
        if (result.IsSuccess)
            return Results.NoContent();

        return result.Error switch
        {
            KafkaClusterNotFoundException or KafkaTopicNotFoundException
                or KafkaLifecycleNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found",
                    detail: result.Error.Message),
            KafkaClusterNotActiveException => Results.Problem(
                statusCode: StatusCodes.Status409Conflict, title: "Cluster not active",
                detail: result.Error.Message),
            EtcdWriteUnavailableException or InvalidKafkaConfigException or InvalidKafkaTopicKeyException
                => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
            _ => Results.Problem(
                statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                detail: result.Error!.Message),
        };
    }
}
