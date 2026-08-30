using System.Security.Claims;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure.CQRS;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AdminPanel.Api.Operations.Kafka;

// Модуль kafka-мутаций (arch/03 §7.1): 8 эндпоинтов (волна B + C2 desired-
// мутации топиков PUT/DELETE topics/{t}[/desired]).
public static class KafkaOperationsModule
{
    public static IEndpointRouteBuilder MapKafkaOperationsApi(this IEndpointRouteBuilder endpoints)
    {
        // POST /api/kafka/clusters — создание кластера (02 §10.2-1).
        endpoints.MapPost("/api/kafka/clusters", async (
            CreateKafkaClusterRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<CreateKafkaClusterCommand, KafkaClusterCreatedDto>(
                new CreateKafkaClusterCommand(request), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/kafka/clusters/{result.Value.Name}", result.Value);

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

        // DELETE /api/kafka/clusters/{cluster} — перевод в TO_REMOVE (02 §10.2-2).
        endpoints.MapDelete("/api/kafka/clusters/{cluster}", async (
            string cluster, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<DeleteKafkaClusterCommand, KafkaClusterDeletedDto>(
                new DeleteKafkaClusterCommand(cluster), ct);
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

        // PUT /api/kafka/clusters/{cluster}/config — default-конфиги (02 §10.2-3).
        endpoints.MapPut("/api/kafka/clusters/{cluster}/config", async (
            string cluster, KafkaConfigUpdateRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<UpdateKafkaConfigCommand, KafkaConfigUpdatedDto>(
                new UpdateKafkaConfigCommand(cluster, request), ct);
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

        // POST /api/kafka/clusters/{cluster}/brokers — добавление брокера (02 §10.2-4).
        endpoints.MapPost("/api/kafka/clusters/{cluster}/brokers", async (
            string cluster, AddKafkaBrokerRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<AddKafkaBrokerCommand, KafkaBrokerAddedDto>(
                new AddKafkaBrokerCommand(cluster, request), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/kafka/clusters/{cluster}/brokers/{result.Value.Name}", result.Value);

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

        // DELETE /api/kafka/clusters/{cluster}/brokers/{broker} — маркер TO_REMOVE (02 §10.2-5).
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/brokers/{broker}", async (
            string cluster, string broker, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RemoveKafkaBrokerCommand, KafkaBrokerRemovedDto>(
                new RemoveKafkaBrokerCommand(cluster, broker), ct);
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

        // POST /api/kafka/clusters/{cluster}/app-password/rotate — заявка ротации (02 §10.2-8).
        endpoints.MapPost("/api/kafka/clusters/{cluster}/app-password/rotate", async (
            string cluster, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RotateKafkaPasswordCommand, KafkaPasswordRotatedDto>(
                new RotateKafkaPasswordCommand(cluster, user.Identity?.Name ?? "adminpanel"), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/kafka/clusters/{cluster}", result.Value);

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
        // партиций (t02, 02 §10.2-9): клэйм-txn по живой заявке.
        endpoints.MapPost("/api/kafka/clusters/{cluster}/rebalance", async (
            string cluster, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RequestKafkaRebalanceCommand, KafkaRebalanceRequestedDto>(
                new RequestKafkaRebalanceCommand(cluster, user.Identity?.Name ?? "adminpanel"), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/kafka/clusters/{cluster}", result.Value);

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
        // (t02, 02 §10.2-10): новые батчи не подаются, поданные Kafka доиграет.
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/rebalance", async (
            string cluster, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<CancelKafkaRebalanceCommand, KafkaRebalanceCancelledDto>(
                new CancelKafkaRebalanceCommand(cluster), ct);
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

        // PUT /api/kafka/clusters/{cluster}/topics/{topic} — конфиг-заявка (02 §10.2-7).
        endpoints.MapPut("/api/kafka/clusters/{cluster}/topics/{topic}", async (
            string cluster, string topic, TopicDesiredRequest request, ClaimsPrincipal user,
            IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<UpsertTopicDesiredCommand, KafkaTopicDesiredDto>(
                new UpsertTopicDesiredCommand(cluster, topic, request, user.Identity?.Name ?? "adminpanel"), ct);
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

        // DELETE /api/kafka/clusters/{cluster}/topics/{topic}/desired — отмена заявки (02 §10.2-8).
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/topics/{topic}/desired", async (
            string cluster, string topic, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<CancelTopicDesiredCommand, KafkaTopicDesiredCancelledDto>(
                new CancelTopicDesiredCommand(cluster, topic), ct);
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

        // POST /api/kafka/clusters/{cluster}/topics — создание топика (02 §10.2-9).
        endpoints.MapPost("/api/kafka/clusters/{cluster}/topics", async (
            string cluster, CreateTopicRequest request, ClaimsPrincipal user,
            IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<CreateKafkaTopicCommand, KafkaTopicCreatedDto>(
                new CreateKafkaTopicCommand(cluster, request, user.Identity?.Name ?? "adminpanel"), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/kafka/clusters/{cluster}/topics/{request.Name}", result.Value);

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

        // DELETE /api/kafka/clusters/{cluster}/topics/{topic} — удаление топика (02 §10.2-10).
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/topics/{topic}", async (
            string cluster, string topic, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<DeleteKafkaTopicCommand, KafkaTopicDeletedDto>(
                new DeleteKafkaTopicCommand(cluster, topic, user.Identity?.Name ?? "adminpanel"), ct);
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

        // DELETE /api/kafka/clusters/{cluster}/topics/{topic}/desired.create — отмена (02 §10.2-11).
        // DELETE /api/kafka/clusters/{cluster}/topics/{topic}/desired.delete — отмена (02 §10.2-12).
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/topics/{topic}/desired.create", async (
            string cluster, string topic, IHandler handler, CancellationToken ct) =>
            await CancelTopicLifecycleAsync(cluster, topic, "create", handler, ct));
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/topics/{topic}/desired.delete", async (
            string cluster, string topic, IHandler handler, CancellationToken ct) =>
            await CancelTopicLifecycleAsync(cluster, topic, "delete", handler, ct));

        return endpoints;
    }

    // Общий хендлер отмены lifecycle-заявок (02 §10.2-11/12): успех → 204.
    private static async Task<IResult> CancelTopicLifecycleAsync(
        string cluster, string topic, string op, IHandler handler, CancellationToken ct)
    {
        var result = await handler.HandleCommand<CancelTopicLifecycleCommand, KafkaTopicLifecycleCancelledDto>(
            new CancelTopicLifecycleCommand(cluster, topic, op), ct);
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
