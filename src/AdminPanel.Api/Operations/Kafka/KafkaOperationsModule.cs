using System.Security.Claims;
using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AdminPanel.Api.Operations.Kafka;

// Модуль kafka-мутаций (arch/03 §7.1) — ПРОКСИ в API KafkaWorker (arch/16 §1.1):
// панель не пишет в etcd; успех — DTO воркера, ошибки — ProblemDetails как есть,
// недоступность API — собственный 503. UI-контракт §10.2 не меняется.
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

            return Error(result);
        });

        // DELETE /api/kafka/clusters/{cluster} — перевод в TO_REMOVE (02 §10.2-2).
        endpoints.MapDelete("/api/kafka/clusters/{cluster}", async (
            string cluster, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<DeleteKafkaClusterCommand, KafkaClusterDeletedDto>(
                new DeleteKafkaClusterCommand(cluster), ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return Error(result);
        });

        // PUT /api/kafka/clusters/{cluster}/config — default-конфиги (02 §10.2-3).
        endpoints.MapPut("/api/kafka/clusters/{cluster}/config", async (
            string cluster, KafkaConfigUpdateRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<UpdateKafkaConfigCommand, KafkaConfigUpdatedDto>(
                new UpdateKafkaConfigCommand(cluster, request), ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);

            return Error(result);
        });

        // POST /api/kafka/clusters/{cluster}/brokers — добавление брокера (02 §10.2-4).
        endpoints.MapPost("/api/kafka/clusters/{cluster}/brokers", async (
            string cluster, AddKafkaBrokerRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<AddKafkaBrokerCommand, KafkaBrokerAddedDto>(
                new AddKafkaBrokerCommand(cluster, request), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/kafka/clusters/{cluster}/brokers/{result.Value.Name}", result.Value);

            return Error(result);
        });

        // DELETE /api/kafka/clusters/{cluster}/brokers/{broker} — маркер TO_REMOVE (02 §10.2-5).
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/brokers/{broker}", async (
            string cluster, string broker, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RemoveKafkaBrokerCommand, KafkaBrokerRemovedDto>(
                new RemoveKafkaBrokerCommand(cluster, broker), ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return Error(result);
        });

        // PUT /api/kafka/clusters/{cluster}/brokers/{broker}/resources — мутация
        // №15 (t06, 02 §10.2-15): прокси в API воркера; применяет NodeRegenerator.
        endpoints.MapPut("/api/kafka/clusters/{cluster}/brokers/{broker}/resources", async (
            string cluster, string broker, KafkaBrokerResourcesRequestDto request,
            IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<UpdateKafkaBrokerResourcesCommand, KafkaBrokerResourcesUpdatedDto>(
                new UpdateKafkaBrokerResourcesCommand(cluster, broker, request), ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);

            return Error(result);
        });

        // POST /api/kafka/clusters/{cluster}/app-password/rotate — заявка ротации (02 §10.2-8);
        // оператор сессии уходит воркеру заголовком X-Requested-By (spec §3.7).
        endpoints.MapPost("/api/kafka/clusters/{cluster}/app-password/rotate", async (
            string cluster, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RotateKafkaPasswordCommand, KafkaPasswordRotatedDto>(
                new RotateKafkaPasswordCommand(cluster, user.Identity?.Name ?? "adminpanel"), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/kafka/clusters/{cluster}", result.Value);

            return Error(result);
        });

        // POST /api/kafka/clusters/{cluster}/rebalance — заявка ребалансировки
        // партиций (t02, 02 §10.2-13): клэйм по живой заявке — в воркере.
        endpoints.MapPost("/api/kafka/clusters/{cluster}/rebalance", async (
            string cluster, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RequestKafkaRebalanceCommand, KafkaRebalanceRequestedDto>(
                new RequestKafkaRebalanceCommand(cluster, user.Identity?.Name ?? "adminpanel"), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/kafka/clusters/{cluster}", result.Value);

            return Error(result);
        });

        // DELETE /api/kafka/clusters/{cluster}/rebalance — отмена ребалансировки
        // (t02, 02 §10.2-14): новые батчи не подаются, поданные Kafka доиграет.
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/rebalance", async (
            string cluster, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<CancelKafkaRebalanceCommand, KafkaRebalanceCancelledDto>(
                new CancelKafkaRebalanceCommand(cluster), ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return Error(result);
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

            return Error(result);
        });

        // DELETE /api/kafka/clusters/{cluster}/topics/{topic}/desired — отмена заявки (02 §10.2-8).
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/topics/{topic}/desired", async (
            string cluster, string topic, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<CancelTopicDesiredCommand, KafkaTopicDesiredCancelledDto>(
                new CancelTopicDesiredCommand(cluster, topic), ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return Error(result);
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

            return Error(result);
        });

        // DELETE /api/kafka/clusters/{cluster}/topics/{topic} — удаление топика (02 §10.2-10).
        endpoints.MapDelete("/api/kafka/clusters/{cluster}/topics/{topic}", async (
            string cluster, string topic, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<DeleteKafkaTopicCommand, KafkaTopicDeletedDto>(
                new DeleteKafkaTopicCommand(cluster, topic, user.Identity?.Name ?? "adminpanel"), ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return Error(result);
        });

        // DELETE .../desired.create и .../desired.delete — отмена lifecycle-заявок
        // (02 §10.2-11/12): успех → 204.
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

        return Error(result);
    }

    // Error-ветка прокси (общая с pg-модулем): недоступность API воркера →
    // собственный 503 панели; ProblemDetails воркера (400/404/409/503 + errors[])
    // — телом как есть.
    private static IResult Error(Result result) => result.Error switch
    {
        WorkerApiUnavailableException unavailable => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "API воркера недоступен",
            detail: unavailable.Message),
        WorkerProblemDetails problem => Results.Text(
            problem.Body, "application/problem+json", System.Text.Encoding.UTF8, problem.StatusCode),
        _ => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Etcd write failed",
            detail: result.Error!.Message),
    };
}
