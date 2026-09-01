using System.Net.Http.Json;
using System.Security.Claims;
using System.Text;
using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AdminPanel.Api.Operations;

// Модуль операций (мутирующие эндпоинты) — ПРОКСИ в API воркеров (arch/01 §1,
// arch/14 §1.1): панель не пишет в etcd; успех — десериализованный DTO воркера,
// ошибки — ProblemDetails воркера как есть (UI-контракт arch/03 §1 не меняется),
// недоступность API — собственный 503. InspectionModule остаётся read-only.
public static class OperationsModule
{
    private const string ProblemContentType = "application/problem+json";

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

            return Error(result);
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

            return Error(result);
        });

        // POST /api/clusters/{cluster}/shards — добавить шард Active-кластеру (02 §9.5).
        endpoints.MapPost("/api/clusters/{cluster}/shards", async (
            string cluster, AddShardRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<AddShardCommand, ShardAddedDto>(
                new AddShardCommand(cluster, request), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/clusters/{cluster}/shards/{result.Value.Name}", result.Value);

            return Error(result);
        });

        // DELETE /api/clusters/{cluster}/shards/{shard} — маркер демонтажа (02 §9.6);
        // 204 идемпотентен; 404/409/503.
        endpoints.MapDelete("/api/clusters/{cluster}/shards/{shard}", async (
            string cluster, string shard, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<DeleteShardCommand, ShardDeletedDto>(
                new DeleteShardCommand(cluster, shard), ct);
            if (result.IsSuccess)
                return Results.NoContent();

            return Error(result);
        });

        // POST /api/clusters/{cluster}/moves — заявки на переезды бакетов (02 §9.7):
        // оператор сессии уходит воркеру заголовком X-Requested-By (spec §3.7).
        endpoints.MapPost("/api/clusters/{cluster}/moves", async (
            string cluster, MoveBucketsRequest request, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<MoveBucketsCommand, MovesQueuedDto>(
                new MoveBucketsCommand(
                    cluster, request.From, request.To, request.Buckets ?? [],
                    user.Identity?.Name ?? "adminpanel"), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/clusters/{cluster}", result.Value);

            return Error(result);
        });

        // POST /api/clusters/{cluster}/app-password/rotate — заявка ротации app-пароля
        // (02 §9.8): клэймит заявку воркер; выполнение — AppPasswordRotator PgWorker.
        endpoints.MapPost("/api/clusters/{cluster}/app-password/rotate", async (
            string cluster, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RotateAppPasswordCommand, AppPasswordRotatedDto>(
                new RotateAppPasswordCommand(cluster, user.Identity?.Name ?? "adminpanel"), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/clusters/{cluster}", result.Value);

            return Error(result);
        });

        // POST /api/ha/{scope}/nodes/{node}/recreate — маркер пересоздания ноды
        // (TO_RECREATE) с режимом soft|hard (нет тела — soft); битый JSON — 400.
        endpoints.MapPost("/api/ha/{scope}/nodes/{node}/recreate", async (
            string scope, string node, HttpRequest http, IHandler handler, CancellationToken ct) =>
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
            var result = await handler.HandleCommand<RecreateNodeCommand, NodeRecreatedDto>(
                new RecreateNodeCommand(scope, node, body?.Mode), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/ha/{scope}", result.Value);

            return Error(result);
        });

        return endpoints;
    }

    // Error-ветка прокси: недоступность API воркера → собственный 503 панели;
    // ProblemDetails воркера (400/404/409/503 + errors[]) — телом как есть.
    private static IResult Error(Result result) => result.Error switch
    {
        WorkerApiUnavailableException unavailable => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "API воркера недоступен",
            detail: unavailable.Message),
        WorkerProblemDetails problem => Results.Text(
            problem.Body, ProblemContentType, Encoding.UTF8, problem.StatusCode),
        _ => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Etcd write failed",
            detail: result.Error!.Message),
    };
}
