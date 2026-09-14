using System.Security.Claims;
using System.Text.Json.Serialization;
using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AdminPanel.Api.Operations;

// Грань «Воркеры» (spec §3.3 п.4, arch/adminpanel/03 §1/§3.7): сводка сертов
// и инстансов + управление (generate/PUT/DELETE) — ПРЯМАЯ запись панели в etcd
// (arch/adminpanel/02 §9.9, вторая категория после §9) + рестарт-прокси.
public static class WorkersModule
{
    public static IEndpointRouteBuilder MapWorkersApi(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/api/workers", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<GetWorkersQuery, WorkersViewDto>(new(), ct);
            return result.IsSuccess ? Results.Ok(result.Value) : Error(result);
        });

        endpoints.MapPost("/api/workers/{worker}/api-cert/generate", async (
            string worker, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<GenerateWorkerApiCertCommand, WorkerApiCertDto>(
                new(worker, user.Identity?.Name ?? "adminpanel"), ct);
            return result.IsSuccess
                ? Results.Created($"/api/workers/{worker}", result.Value)
                : Error(result);
        });

        endpoints.MapPut("/api/workers/{worker}/api-cert", async (
            string worker, UploadWorkerApiCertRequest request, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<UploadWorkerApiCertCommand, WorkerApiCertDto>(
                new(worker, request.CertPem ?? "", request.KeyPem ?? "", user.Identity?.Name ?? "adminpanel"), ct);
            return result.IsSuccess
                ? Results.Created($"/api/workers/{worker}", result.Value)
                : Error(result);
        });

        endpoints.MapDelete("/api/workers/{worker}/api-cert", async (
            string worker, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<DeleteWorkerApiCertCommand, WorkerApiCertDeletedDto>(
                new(worker), ct);
            return result.IsSuccess ? Results.NoContent() : Error(result);
        });

        endpoints.MapPost("/api/workers/{worker}/restart", async (
            string worker, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RestartWorkerCommand, WorkerRestartDto>(
                new(worker, user.Identity?.Name ?? "adminpanel"), ct);
            return result.IsSuccess ? Results.Accepted((string?)null, result.Value) : Error(result);
        });

        return endpoints;
    }

    // Тело PUT {cert_pem, key_pem} (spec §3.3 п.4 — snake_case в контракте;
    // Web-биндинг camelCase НЕ мапит подчёркивания — фиксируем атрибутом).
    public sealed record UploadWorkerApiCertRequest(
        [property: JsonPropertyName("cert_pem")] string? CertPem,
        [property: JsonPropertyName("key_pem")] string? KeyPem);

    // Error-ветка: 422 — ЯВНОЕ «влияет на исходящие» (spec §2 п.6); 400 — не годен
    // как серверный; 404/409/503 — по таблице §4.3/03 §1.
    private static IResult Error(Result result) => result.Error switch
    {
        WorkerCertAffectsOutgoingException affects => Results.Problem(
            statusCode: StatusCodes.Status422UnprocessableEntity,
            title: "Сертификат влияет на коммуникации воркеров с их подчинёнными сервисами — обновление отклонено",
            detail: affects.Message),
        WorkerCertInvalidException invalid => Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "Invalid certificate",
            detail: invalid.Message),
        WorkerCertAlreadyManagedException => Results.Problem(
            statusCode: StatusCodes.Status409Conflict,
            title: "Certificate already managed",
            detail: result.Error!.Message),
        WorkerNotFoundException or WorkerCertNotFoundException => Results.Problem(
            statusCode: StatusCodes.Status404NotFound,
            title: "Not found",
            detail: result.Error!.Message),
        WorkerApiUnavailableException unavailable => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "API воркера недоступен",
            detail: unavailable.Message),
        _ => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Etcd write failed",
            detail: result.Error!.Message),
    };
}

// ===== Query: сводка воркеров (spec §3.3 п.4) =====

public sealed record GetWorkersQuery() : IQuery<WorkersViewDto>;

[InjectAsScoped]
public sealed class GetWorkersQueryHandler(ISnapshotStore pg, IKafkaSnapshotReader kafka)
    : IQueryHandler<GetWorkersQuery, WorkersViewDto>
{
    public ValueTask<Result<WorkersViewDto>> Handle(GetWorkersQuery _, CancellationToken ct)
    {
        var p = pg.Current;
        var k = kafka.Current;

        static string ApplyStatus(WorkerApiCert? target, string? instanceThumb) => target switch
        {
            null => "unmanaged",                       // ключа нет — инстанс на env-серте
            _ when instanceThumb is null => "unknown", // старая версия не сообщает
            var t when t.Thumbprint == instanceThumb => "applied",
            _ => "pending restart",
        };

        static WorkerInstanceDto Instance(WorkerEndpoint e, WorkerApiCert? target, WorkerHealth? health) => new(
            e.InstanceId, e.Url, e.SinceUnix,
            health is null ? null : health.Status.ToString().ToLowerInvariant(),
            e.CertThumbprint,
            ApplyStatus(target, e.CertThumbprint));

        static WorkerCertDto? Cert(WorkerApiCert? c) => c is null ? null : new(
            c.Thumbprint, c.Subject, c.Issuer, c.San,
            c.NotBefore.ToUnixTimeSeconds(), c.NotAfter.ToUnixTimeSeconds(),
            c.UpdatedUnix, c.UpdatedBy);

        return ValueTask.FromResult(Result<WorkersViewDto>.Success(new WorkersViewDto(
        [
            new("pgworker",
                (p?.PgWorkerEndpoints ?? []).Select(e => Instance(e, p?.WorkerApiCert,
                    p?.WorkerHealth?.FirstOrDefault(h => h.InstanceId == e.InstanceId))).ToList(),
                Cert(p?.WorkerApiCert)),
            new("kafkaworker",
                (k?.WorkerEndpoints ?? []).Select(e => Instance(e, k?.WorkerApiCert,
                    k?.WorkerHealth?.FirstOrDefault(h => h.InstanceId == e.InstanceId))).ToList(),
                Cert(k?.WorkerApiCert)),
        ])));
    }
}
