using System.Security.Claims;
using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AdminPanel.Api.Operations.Valkey;

// Модуль valkey-мутаций (arch/03 §8.1) — ПРОКСИ в API ValkeyWorker (arch/21 §1.1):
// панель не пишет в etcd; успех — DTO воркера, ошибки — ProblemDetails как есть,
// недоступность API — собственный 503. Успех не дёргает refresher: следующий
// тик (3 c) подхватывает новые ключи (arch/02 §4).
public static class ValkeyOperationsModule
{
    public static IEndpointRouteBuilder MapValkeyOperationsApi(this IEndpointRouteBuilder endpoints)
    {
        // POST /api/valkey/clusters — создание (02 §11.2-1): 201.
        endpoints.MapPost("/api/valkey/clusters", async (
            CreateValkeyClusterRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<CreateValkeyClusterCommand, ValkeyClusterCreatedDto>(
                new CreateValkeyClusterCommand(request), ct);
            return result.IsSuccess
                ? Results.Created($"/api/valkey/clusters/{result.Value.Name}", result.Value)
                : Error(result);
        });

        // DELETE /api/valkey/clusters/{cluster} — TO_REMOVE (02 §11.2-2): 202
        // (демонтаж асинхронный — процесс B воркера), без тела.
        endpoints.MapDelete("/api/valkey/clusters/{cluster}", async (
            string cluster, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<DeleteValkeyClusterCommand, ValkeyClusterDeletedDto>(
                new DeleteValkeyClusterCommand(cluster), ct);
            return result.IsSuccess ? Results.Accepted() : Error(result);
        });

        // PUT /api/valkey/clusters/{cluster}/config — maxmemory_* (02 §11.2-3): 200.
        endpoints.MapPut("/api/valkey/clusters/{cluster}/config", async (
            string cluster, ValkeyConfigUpdateRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<UpdateValkeyConfigCommand, ValkeyConfigUpdatedDto>(
                new UpdateValkeyConfigCommand(cluster, request), ct);
            return result.IsSuccess ? Results.Ok(result.Value) : Error(result);
        });

        // PUT /api/valkey/clusters/{cluster}/nodes/{node}/resources (02 §11.2-4): 200.
        endpoints.MapPut("/api/valkey/clusters/{cluster}/nodes/{node}/resources", async (
            string cluster, string node, ValkeyResourcesUpdateRequest request,
            IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<UpdateValkeyResourcesCommand, ValkeyResourcesUpdatedDto>(
                new UpdateValkeyResourcesCommand(cluster, node, request), ct);
            return result.IsSuccess ? Results.Ok(result.Value) : Error(result);
        });

        // POST /api/valkey/clusters/{cluster}/password/rotate — заявка (02 §11.2-5):
        // 202; оператор сессии — X-Requested-By (arch/02 §11.2).
        endpoints.MapPost("/api/valkey/clusters/{cluster}/password/rotate", async (
            string cluster, RotateValkeyPasswordRequest request, ClaimsPrincipal user,
            IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RotateValkeyPasswordCommand, ValkeyPasswordRotatedDto>(
                new RotateValkeyPasswordCommand(cluster, request.Role ?? "", user.Identity?.Name ?? "adminpanel"), ct);
            return result.IsSuccess ? Results.Accepted((string?)null, result.Value) : Error(result);
        });

        // POST /api/valkey/clusters/{cluster}/ca/rotate — заявка ротации CA/сертов
        // (02 §11.2-6, t07): 202; оператор сессии — X-Requested-By.
        endpoints.MapPost("/api/valkey/clusters/{cluster}/ca/rotate", async (
            string cluster, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RotateValkeyCaCommand, ValkeyCaRotatedDto>(
                new RotateValkeyCaCommand(cluster, user.Identity?.Name ?? "adminpanel"), ct);
            return result.IsSuccess ? Results.Accepted((string?)null, result.Value) : Error(result);
        });

        return endpoints;
    }

    // Error-ветка прокси (общая с pg/kafka-модулями): недоступность API воркера →
    // собственный 503 панели; ProblemDetails воркера — телом как есть.
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
