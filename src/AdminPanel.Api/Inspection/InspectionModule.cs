using AdminPanel.Core;
using AdminPanel.Etcd;
using Shared.Core.CQRS;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AdminPanel.Api.Inspection;

// Композиция эндпоинтов инспекции etcd из снапшота (arch/03 §1; auth-guard уже закрыл /api/*).
public static class InspectionModule
{
    // До первого тика снапшота нет (t03 §3.13): хендлеры возвращают этот отказ → 503 (spec §3.12).
    public sealed class SnapshotNotReadyException() : Exception("etcd snapshot has not been collected yet");

    // Кластер отсутствует в снапшоте: 404 — отличается от 503 «снапшота нет» (spec §3.10).
    public sealed class ClusterNotFoundException(string cluster)
        : Exception($"cluster {cluster} not found in snapshot");

    // HA-scope отсутствует в снапшоте: 404 — как неизвестный кластер (spec §3.18).
    public sealed class ScopeNotFoundException(string scope)
        : Exception($"HA-scope {scope} not found in snapshot");

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

        // GET /api/backups/s3-pools (t36, adminpanel/02 §12.4): пулы — из единого
        // домена S3 (S3Snapshot, тот же тик) — Select модала «Пул бэкапов»
        // (без ключей/секретов).
        endpoints.MapGet("/api/backups/s3-pools", async (IS3SnapshotReader store, CancellationToken ct) =>
        {
            var snapshot = store.Current;
            if (snapshot is null)
                return Results.Problem(statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Snapshot not ready");
            return Results.Ok(S3Mappers.MapPools(snapshot.Clusters));
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
                        detail: $"state must be ACTIVE|SYNCING|FROZEN|ABORTING|NOT_INITIALIZED, got: {state}");
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

        // GET /api/reliability — грань «Надёжность» (arch/03 §1): RPO/RTO-числа.
        endpoints.MapGet("/api/reliability", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<ReliabilityQuery, ReliabilityDto>(new ReliabilityQuery(), ct);
            return result.IsSuccess
                ? Results.Ok(result.Value)
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
                        detail: $"severity must be critical|warning|info, got: {severity}");
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

    // GET /api/valkey/clusters[...] — инспекция valkey-домена из ValkeySnapshot (arch/03 §8.1).
    public static IEndpointRouteBuilder MapValkeyInspectionApi(this IEndpointRouteBuilder endpoints)
    {
        // GET /api/valkey/clusters — сводный список (arch/03 §8.1).
        endpoints.MapGet("/api/valkey/clusters", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<ValkeyClustersQuery, IReadOnlyList<ValkeyClusterSummaryDto>>(
                new ValkeyClustersQuery(), ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);
            return result.Error is ValkeyClusterNotFound
                ? Results.Problem(
                    statusCode: StatusCodes.Status404NotFound,
                    title: "Not found",
                    detail: result.Error.Message)
                : Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Valkey snapshot not ready",
                    detail: result.Error!.Message);
        });

        // GET /api/valkey/clusters/{cluster} — детали; 404 кластера нет, прочее — 503.
        endpoints.MapGet("/api/valkey/clusters/{cluster}", async (
            string cluster, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<ValkeyClusterDetailsQuery, ValkeyClusterDto>(
                new ValkeyClusterDetailsQuery(cluster), ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);
            return result.Error is ValkeyClusterNotFound
                ? Results.Problem(
                    statusCode: StatusCodes.Status404NotFound,
                    title: "Not found",
                    detail: result.Error.Message)
                : Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Valkey snapshot not ready",
                    detail: result.Error!.Message);
        });

        return endpoints;
    }

    // GET /api/backups/* — грань «Хранилище бэкапов» (t08, arch/03 §1): сводка/
    // детали — из снапшота; objects — on-demand list-v2 (единственный выход в ownS3).
    public static IEndpointRouteBuilder MapBackupsInspectionApi(this IEndpointRouteBuilder endpoints)
    {
        // GET /api/backups/storage — сводка (configured=false → 200 с одним полем+причиной).
        endpoints.MapGet("/api/backups/storage", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<BackupStorageQuery, BackupStorageDto>(
                new BackupStorageQuery(), ct);
            return ResultToHttp(result);
        });

        // GET /api/backups/storage/{cluster}/{shard} — детали шарда; BackupShardNotFound
        // → 404, прочий отказ → 503 (маппинг как у /api/clusters/{cluster}).
        endpoints.MapGet("/api/backups/storage/{cluster}/{shard}", async (
            string cluster, string shard, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<BackupShardStorageQuery, BackupShardStorageDto>(
                new BackupShardStorageQuery(cluster, shard), ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);
            return result.Error is BackupShardNotFound
                ? Results.Problem(
                    statusCode: StatusCodes.Status404NotFound,
                    title: "Backup shard not found",
                    detail: result.Error.Message)
                : Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Snapshot not ready",
                    detail: result.Error!.Message);
        });

        // GET /api/backups/objects?prefix=&maxKeys=&continuationToken= — on-demand
        // list-v2: ступень 1 гварда (форма prefix + диапазон maxKeys) inline до query —
        // 400; ступень 2 (принадлежность кластерам/дереву) и сбой хранилища — в handler'е
        // (InvalidBackupPrefixException → 400, BackupsS3UnavailableException → 502).
        endpoints.MapGet("/api/backups/objects", async (
            string? prefix, int? maxKeys, string? continuationToken,
            IHandler handler, CancellationToken ct) =>
        {
            if (!BackupObjectsQuery.IsValidPrefixForm(prefix))
                return Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Invalid prefix",
                    detail: $"prefix is empty or its first segment does not match ^[a-z][a-z0-9_]{{0,62}}$: {prefix}");
            if (!BackupObjectsQuery.IsValidMaxKeys(maxKeys))
                return Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Invalid maxKeys",
                    detail: $"maxKeys must be 1..{BackupObjectsQuery.MaxKeysLimit}, got: {maxKeys?.ToString() ?? "null"}");

            var result = await handler.HandleQuery<BackupObjectsQuery, BackupObjectsPageDto>(
                new BackupObjectsQuery(prefix, maxKeys, continuationToken), ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);
            return result.Error switch
            {
                InvalidBackupPrefixException or InvalidBackupMaxKeysException => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Invalid prefix",
                    detail: result.Error.Message),
                BackupsStorageNotConfiguredException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Backups storage not configured",
                    detail: result.Error.Message),
                BackupsS3UnavailableException => Results.Problem(
                    statusCode: StatusCodes.Status502BadGateway,
                    title: "S3 storage unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Snapshot not ready",
                    detail: result.Error!.Message),
            };
        });

        return endpoints;
    }

    // Инспекция s3-домена (t34; arch/03 §12.1): GET /api/s3/clusters — 200/503;
    // GET /api/s3/clusters/{cluster} — 200 / 404 кластера нет / 503.
    public static IEndpointRouteBuilder MapS3InspectionApi(this IEndpointRouteBuilder endpoints)
    {
        // GET /api/s3/clusters — сводный список (arch/03 §12.1).
        endpoints.MapGet("/api/s3/clusters", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<S3ClustersQuery, IReadOnlyList<S3ClusterSummaryDto>>(
                new S3ClustersQuery(), ct);
            return result.IsSuccess
                ? Results.Ok(result.Value)
                : Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "S3 snapshot not ready",
                    detail: result.Error!.Message);
        });

        // GET /api/s3/clusters/{cluster} — детали; 404 кластера нет, прочее — 503.
        endpoints.MapGet("/api/s3/clusters/{cluster}", async (
            string cluster, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<S3ClusterDetailsQuery, S3ClusterDto>(
                new S3ClusterDetailsQuery(cluster), ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);
            return result.Error is S3ClusterNotFound
                ? Results.Problem(
                    statusCode: StatusCodes.Status404NotFound,
                    title: "Not found",
                    detail: result.Error.Message)
                : Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "S3 snapshot not ready",
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
