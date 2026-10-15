using System.Security.Claims;
using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AdminPanel.Api.Operations.S3;

// Модуль s3-мутаций (arch/03 §12.1) — ПРОКСИ в API S3Worker (arch/24 §1.1):
// панель не пишет в etcd; успех — код воркера 1:1 (202|204 различаются,
// 201|200 у ротации — WorkerRaw<T>.StatusCode), ошибки — ProblemDetails как
// есть, недоступность API — собственный 503. Успех не дёргает refresher:
// следующий тик (3 c) подхватывает новые ключи (arch/02 §4).
public static class S3OperationsModule
{
    public static IEndpointRouteBuilder MapS3OperationsApi(this IEndpointRouteBuilder endpoints)
    {
        // POST /api/s3/clusters — создание (02 §12.2-1): 201.
        endpoints.MapPost("/api/s3/clusters", async (
            CreateS3ClusterPanelRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<CreateS3ClusterCommand, S3ClusterCreatedPanelDto>(
                new CreateS3ClusterCommand(request), ct);
            return result.IsSuccess
                ? Results.Created($"/api/s3/clusters/{result.Value.Name}", result.Value)
                : Error(result);
        });

        // DELETE /api/s3/clusters/{cluster} — TO_REMOVE (02 §12.2-2): 202 (записан)
        // / 204 (already) — код воркера зеркалится, без тела.
        endpoints.MapDelete("/api/s3/clusters/{cluster}", async (
            string cluster, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<DeleteS3ClusterCommand, WorkerRaw<S3ClusterDeletedPanelDto>>(
                new DeleteS3ClusterCommand(cluster), ct);
            return result.IsSuccess ? Mirror(result.Value.StatusCode) : Error(result);
        });

        // POST /api/s3/clusters/{cluster}/nodes — добавление инстанса (02 §12.2-3): 201.
        endpoints.MapPost("/api/s3/clusters/{cluster}/nodes", async (
            string cluster, AddS3NodePanelRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<AddS3NodeCommand, S3NodeAddedPanelDto>(
                new AddS3NodeCommand(cluster, request), ct);
            return result.IsSuccess
                ? Results.Created($"/api/s3/clusters/{cluster}", result.Value)
                : Error(result);
        });

        // DELETE /api/s3/clusters/{cluster}/nodes/{node} — демонтаж one-way
        // (02 §12.2-4): 202 / 204 (already).
        endpoints.MapDelete("/api/s3/clusters/{cluster}/nodes/{node}", async (
            string cluster, string node, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RemoveS3NodeCommand, WorkerRaw<S3NodeRemovedPanelDto>>(
                new RemoveS3NodeCommand(cluster, node), ct);
            return result.IsSuccess ? Mirror(result.Value.StatusCode) : Error(result);
        });

        // PUT /api/s3/clusters/{cluster}/nodes/{node}/resources — ресурсы
        // (02 §12.2-5): 200.
        endpoints.MapPut("/api/s3/clusters/{cluster}/nodes/{node}/resources", async (
            string cluster, string node, UpdateS3ResourcesPanelRequest request,
            IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<UpdateS3ResourcesCommand, S3ResourcesUpdatedPanelDto>(
                new UpdateS3ResourcesCommand(cluster, node, request), ct);
            return result.IsSuccess ? Results.Ok(result.Value) : Error(result);
        });

        // PUT /api/s3/clusters/{cluster}/links — граф целиком (02 §12.2-6): 202
        // (применит доводка G воркера).
        endpoints.MapPut("/api/s3/clusters/{cluster}/links", async (
            string cluster, PutS3LinksPanelRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<PutS3LinksCommand, WorkerRaw<S3LinksPutPanelDto>>(
                new PutS3LinksCommand(cluster, request), ct);
            return result.IsSuccess ? Mirror(result.Value.StatusCode) : Error(result);
        });

        // POST /api/s3/clusters/{cluster}/nodes/{node}/deny — отзыв ключа
        // (02 §12.2-7): 202 / 204 (already).
        endpoints.MapPost("/api/s3/clusters/{cluster}/nodes/{node}/deny", async (
            string cluster, string node, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<DenyS3NodeCommand, WorkerRaw<S3NodeDenyPanelDto>>(
                new DenyS3NodeCommand(cluster, node), ct);
            return result.IsSuccess ? Mirror(result.Value.StatusCode) : Error(result);
        });

        // POST /api/s3/clusters/{cluster}/nodes/{node}/allow — снятие отзыва
        // (02 §12.2-8): 202 / 204 (already).
        endpoints.MapPost("/api/s3/clusters/{cluster}/nodes/{node}/allow", async (
            string cluster, string node, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<AllowS3NodeCommand, WorkerRaw<S3NodeAllowPanelDto>>(
                new AllowS3NodeCommand(cluster, node), ct);
            return result.IsSuccess ? Mirror(result.Value.StatusCode) : Error(result);
        });

        // POST /api/s3/clusters/{cluster}/rotations — заявка ротации секретов
        // (02 §12.2-9): 201 (создана) / 200 (уже стоит — идемпотентность воркера);
        // оператор сессии — X-Requested-By (аудит).
        endpoints.MapPost("/api/s3/clusters/{cluster}/rotations", async (
            string cluster, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RotateS3SecretsCommand, WorkerRaw<S3RotationRequestedPanelDto>>(
                new RotateS3SecretsCommand(cluster, user.Identity?.Name ?? "adminpanel"), ct);
            return result.IsSuccess ? Mirror(result.Value.StatusCode, result.Value.Value) : Error(result);
        });

        // POST /api/s3/clusters/{cluster}/sync-probe — внеплановый зонд
        // (02 §12.2-10): 202; оператор сессии — X-Requested-By (аудит).
        endpoints.MapPost("/api/s3/clusters/{cluster}/sync-probe", async (
            string cluster, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<S3SyncProbeCommand, WorkerRaw<S3ProbeAcceptedPanelDto>>(
                new S3SyncProbeCommand(cluster, user.Identity?.Name ?? "adminpanel"), ct);
            return result.IsSuccess ? Mirror(result.Value.StatusCode, result.Value.Value) : Error(result);
        });

        return endpoints;
    }

    // Зеркало исходного кода воркера: 201 → Created, 200 → Ok, 202 → Accepted,
    // 204 → NoContent (arch/02 §12.2: already→204/202 без перезаписи кода).
    private static IResult Mirror(int statusCode, object? value = null) => statusCode switch
    {
        201 => Results.Created((string?)null, value),
        200 => Results.Ok(value),
        202 => Results.Accepted(),
        _ => Results.NoContent(),
    };

    // Error-ветка прокси (общая с pg/kafka/valkey-модулями): недоступность API
    // воркера → собственный 503 панели; ProblemDetails воркера — телом как есть.
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
