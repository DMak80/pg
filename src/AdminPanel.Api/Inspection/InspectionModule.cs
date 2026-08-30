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

    // Кластер отсутствует в снапшоте: 404 — отличается от 503 «снапшота нет» (spec §3.10).
    public sealed class ClusterNotFoundException(string cluster)
        : Exception($"кластер {cluster} не найден в снапшоте");

    // HA-scope отсутствует в снапшоте: 404 — как неизвестный кластер (spec §3.18).
    public sealed class ScopeNotFoundException(string scope)
        : Exception($"HA-scope {scope} не найден в снапшоте");

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

        // GET /api/clusters — сводный список (arch/03 §1; spec §6.1).
        endpoints.MapGet("/api/clusters", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<ClustersQuery, IReadOnlyList<ClusterSummaryDto>>(
                new ClustersQuery(), ct);
            return ResultToHttp(result);
        });

        // GET /api/clusters/{cluster}?owner=&state= — детали (arch/03 §1); state строго
        // ACTIVE|SYNCING|FROZEN|ABORTING|NOT_INITIALIZED, иначе 400 (spec §3.9);
        // ClusterNotFoundException → 404, прочий отказ → 503 (spec §3.10).
        endpoints.MapGet("/api/clusters/{cluster}", async (
            string cluster, string? owner, string? state, IHandler handler, CancellationToken ct) =>
        {
            // Валидация до query: строго канон статус-ключей, иначе 400 (spec §3.9).
            BucketState? parsed = null;
            if (state is not null)
            {
                if (!BucketStates.TryParse(state, out var value))
                    return Results.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        title: "Invalid state",
                        detail: $"state должен быть ACTIVE|SYNCING|FROZEN|ABORTING|NOT_INITIALIZED, получено: {state}");
                parsed = value;
            }

            var result = await handler.HandleQuery<ClusterDetailsQuery, ClusterDto>(
                new ClusterDetailsQuery(cluster, owner, parsed), ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);
            return result.Error is ClusterNotFoundException
                ? Results.Problem(
                    statusCode: StatusCodes.Status404NotFound,
                    title: "Cluster not found",
                    detail: result.Error.Message)
                : Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Snapshot not ready",
                    detail: result.Error!.Message);
        });

        // GET /api/ha — сводный список HA-скопов (arch/03 §1).
        endpoints.MapGet("/api/ha", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<HaScopesQuery, IReadOnlyList<HaScopeSummaryDto>>(
                new HaScopesQuery(), ct);
            return ResultToHttp(result);
        });

        // GET /api/ha/{scope} — детали скопа (arch/03 §1); ScopeNotFoundException → 404,
        // прочий отказ → 503 — маппинг как у /api/clusters/{cluster} (t05 §6.1).
        endpoints.MapGet("/api/ha/{scope}", async (string scope, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<HaScopeDetailsQuery, HaScopeDto>(
                new HaScopeDetailsQuery(scope), ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);
            return result.Error is ScopeNotFoundException
                ? Results.Problem(
                    statusCode: StatusCodes.Status404NotFound,
                    title: "Scope not found",
                    detail: result.Error.Message)
                : Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Snapshot not ready",
                    detail: result.Error!.Message);
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

    // GET /api/kafka/clusters[...] — инспекция kafka-домена из KafkaSnapshot (arch/03 §7.1).
    public static IEndpointRouteBuilder MapKafkaInspectionApi(this IEndpointRouteBuilder endpoints)
    {
        // GET /api/kafka/clusters — сводный список (arch/03 §7.1).
        endpoints.MapGet("/api/kafka/clusters", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<KafkaClustersQuery, IReadOnlyList<KafkaClusterSummaryDto>>(
                new KafkaClustersQuery(), ct);
            return ResultToHttp(result);
        });

        // GET /api/kafka/clusters/{cluster} — детали; 404 кластера нет, прочее — 503.
        endpoints.MapGet("/api/kafka/clusters/{cluster}", async (
            string cluster, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<KafkaClusterDetailsQuery, KafkaClusterDto>(
                new KafkaClusterDetailsQuery(cluster), ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);
            return result.Error is KafkaClusterNotFound
                ? Results.Problem(
                    statusCode: StatusCodes.Status404NotFound,
                    title: "Cluster not found",
                    detail: result.Error.Message)
                : Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Snapshot not ready",
                    detail: result.Error!.Message);
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
