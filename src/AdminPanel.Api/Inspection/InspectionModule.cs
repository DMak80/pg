using AdminPanel.Core;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AdminPanel.Api.Inspection;

// Композиция эндпоинтов инспекции etcd из снапшота (arch/03 §1; auth-guard уже закрыл /api/*).
public static class InspectionModule
{
    // До первого тика снапшота нет (t03 §3.13): хендлеры возвращают этот отказ → 503 (spec §3.12).
    public sealed class SnapshotNotReadyException() : Exception("etcd-снапшот ещё не собран");

    // GET /api/overview | /api/etcd/status | /api/alerts (arch/03 §1, spec §6.1).
    public static IEndpointRouteBuilder MapInspectionApi(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/api/overview", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<OverviewQuery, OverviewDto>(new OverviewQuery(), ct);
            return ResultToHttp(result);
        });

        endpoints.MapGet("/api/etcd/status", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<EtcdStatusQuery, EtcdStatusDto>(new EtcdStatusQuery(), ct);
            return ResultToHttp(result);
        });

        endpoints.MapGet("/api/alerts", async (string? severity, string? kind, IHandler handler, CancellationToken ct) =>
        {
            // Валидация до query: строго critical|warning|info, иначе 400 (spec §3.13).
            AlertSeverity? parsed = null;
            if (severity is not null)
            {
                if (!SeverityNames.TryGetValue(severity, out var value))
                    return Results.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        title: "Invalid severity",
                        detail: $"severity должен быть critical|warning|info, получено: {severity}");
                parsed = value;
            }

            var result = await handler.HandleQuery<AlertsQuery, IReadOnlyList<AlertDto>>(
                new AlertsQuery(parsed, kind), ct);
            return ResultToHttp(result);
        });

        return endpoints;
    }

    // Допустимые значения ?severity= — строчный канон arch/03 §1.
    private static readonly Dictionary<string, AlertSeverity> SeverityNames = new()
    {
        ["critical"] = AlertSeverity.Critical,
        ["warning"] = AlertSeverity.Warning,
        ["info"] = AlertSeverity.Info,
    };

    // Общий маппинг Result → HTTP: успех 200; отказ хендлера — 503 ProblemDetails (spec §3.12).
    private static IResult ResultToHttp<T>(Result<T> result)
        => result.IsSuccess
            ? Results.Ok(result.Value)
            : Results.Problem(
                statusCode: StatusCodes.Status503ServiceUnavailable,
                title: "Snapshot not ready",
                detail: result.Error!.Message);
}
