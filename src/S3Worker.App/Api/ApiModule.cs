using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using S3Worker.App.Api.Operations;

namespace S3Worker.App.Api;

// HTTP API воркера S3-домена (arch/24 §1.1, spec §6 — таблица кодов 1:1):
// мутации декларативного контракта /s3/ принимает исполнитель. Каждый
// эндпоинт в финале инкрементирует s3worker_api_operations_total фактическим
// кодом (S3ApiMetrics.Track — успех и ошибки).
public static class ApiModule
{
    public static IEndpointRouteBuilder MapWorkerApi(this IEndpointRouteBuilder endpoints)
    {
        var metrics = endpoints.ServiceProvider.GetRequiredService<S3ApiMetrics>();

        // POST /api/s3/clusters — 201 / 409 / 400 / 422 / 503.
        endpoints.MapPost("/api/s3/clusters", async (
            CreateS3ClusterRequest request, CreateClusterHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(request, ct);
            var response = result.IsSuccess
                ? Results.Created((string?)null, result.Value)
                : result.Error switch
                {
                    S3ValidationException validation => Results.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        title: "Validation failed",
                        detail: result.Error.Message,
                        extensions: new Dictionary<string, object?>
                        {
                            ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                        }),
                    S3SecretProfileUnavailableException profile => Results.Problem(
                        statusCode: StatusCodes.Status422UnprocessableEntity,
                        title: "Secret profile unavailable",
                        detail: result.Error.Message,
                        extensions: new Dictionary<string, object?>
                        {
                            ["profiles"] = profile.Names,
                        }),
                    S3ClusterAlreadyExistsException => Results.Problem(
                        statusCode: StatusCodes.Status409Conflict,
                        title: "Cluster already exists",
                        detail: result.Error.Message),
                    _ => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable,
                        title: "Etcd write failed",
                        detail: result.Error!.Message),
                };
            metrics.Track("create-cluster", GetStatusCode(response));
            return response;
        });

        // DELETE /api/s3/clusters/{cluster} — 202 (TO_REMOVE) / 204 (already) / 404 / 503.
        endpoints.MapDelete("/api/s3/clusters/{cluster}", async (
            string cluster, DeleteClusterHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, ct);
            IResult response;
            if (result.IsSuccess)
                response = result.Value ? Results.Accepted() : Results.NoContent();
            else
            {
                response = result.Error switch
                {
                    S3ClusterNotFoundException => Results.Problem(
                        statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                        detail: result.Error.Message),
                    InvalidS3ConfigException or EtcdWriteUnavailableException => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd unavailable",
                        detail: result.Error.Message),
                    _ => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                        detail: result.Error!.Message),
                };
            }

            metrics.Track("delete-cluster", GetStatusCode(response));
            return response;
        });

        // POST /api/s3/clusters/{cluster}/nodes — 201 / 400 / 409 / 404 / 503.
        endpoints.MapPost("/api/s3/clusters/{cluster}/nodes", async (
            string cluster, AddNodeRequest request, AddNodeHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, request, ct);
            var response = result.IsSuccess
                ? Results.Created((string?)null, new { cluster, node = result.Value })
                : result.Error switch
                {
                    S3ValidationException validation => Results.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        title: "Validation failed",
                        detail: result.Error.Message,
                        extensions: new Dictionary<string, object?>
                        {
                            ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                        }),
                    S3ClusterNotFoundException => Results.Problem(
                        statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                        detail: result.Error.Message),
                    S3ClusterNotActiveException or S3LastNodeException => Results.Problem(
                        statusCode: StatusCodes.Status409Conflict, title: "Add node rejected",
                        detail: result.Error.Message),
                    _ => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                        detail: result.Error!.Message),
                };
            metrics.Track("add-node", GetStatusCode(response));
            return response;
        });

        // DELETE /api/s3/clusters/{cluster}/nodes/{node} — 202 / 204 / 404 / 409 / 503.
        endpoints.MapDelete("/api/s3/clusters/{cluster}/nodes/{node}", async (
            string cluster, string node, DeleteNodeHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, node, ct);
            IResult response;
            if (result.IsSuccess)
                response = result.Value ? Results.Accepted() : Results.NoContent();
            else
            {
                response = result.Error switch
                {
                    S3ClusterNotFoundException or S3NodeNotFoundException => Results.Problem(
                        statusCode: StatusCodes.Status404NotFound, title: "Not found",
                        detail: result.Error.Message),
                    S3LastNodeException => Results.Problem(
                        statusCode: StatusCodes.Status409Conflict, title: "Last node",
                        detail: result.Error.Message),
                    _ => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                        detail: result.Error!.Message),
                };
            }

            metrics.Track("delete-node", GetStatusCode(response));
            return response;
        });

        // PUT /api/s3/clusters/{cluster}/nodes/{node}/resources — 200 / 400 / 404 / 503.
        endpoints.MapPut("/api/s3/clusters/{cluster}/nodes/{node}/resources", async (
            string cluster, string node, UpdateResourcesRequest request,
            UpdateNodeResourcesHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, node, request, ct);
            var response = result.IsSuccess
                ? Results.Ok(result.Value)
                : result.Error switch
                {
                    S3ValidationException validation => Results.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        title: "Validation failed",
                        detail: result.Error.Message,
                        extensions: new Dictionary<string, object?>
                        {
                            ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                        }),
                    S3NodeNotFoundException => Results.Problem(
                        statusCode: StatusCodes.Status404NotFound, title: "Node not found",
                        detail: result.Error.Message),
                    _ => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                        detail: result.Error!.Message),
                };
            metrics.Track("update-node-resources", GetStatusCode(response));
            return response;
        });

        // POST /api/s3/clusters/{cluster}/rotations — 201 / 200 (идемпотентен) / 404 / 503.
        endpoints.MapPost("/api/s3/clusters/{cluster}/rotations", async (
            string cluster, HttpRequest http, PostRotationHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, RequestedBy(http), ct);
            var response = result.IsSuccess
                ? (result.Value ? Results.Created((string?)null, new { cluster }) : Results.Ok(new { cluster }))
                : result.Error switch
                {
                    S3ClusterNotFoundException => Results.Problem(
                        statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                        detail: result.Error.Message),
                    _ => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                        detail: result.Error!.Message),
                };
            metrics.Track("post-rotation", GetStatusCode(response));
            return response;
        });

        // POST /api/s3/clusters/{cluster}/sync-probe — 202 always (заглушка t33;
        // исполнение зонда синхронности mesh — t35).
        endpoints.MapPost("/api/s3/clusters/{cluster}/sync-probe", async (
            string cluster, SyncProbeHandler handler, CancellationToken ct) =>
        {
            await handler.HandleAsync(cluster, ct);
            var response = Results.Accepted();
            metrics.Track("sync-probe", GetStatusCode(response));
            return response;
        });

        // POST /api/seed/demo — за флагом; 201/200 идемпотентно / 404 (флаг off).
        endpoints.MapPost("/api/seed/demo", async (SeedDemoHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(ct);
            IResult response;
            if (result.IsSuccess)
                response = result.Value.Seeded ? Results.Created((string?)null, result.Value) : Results.Ok(result.Value);
            else
            {
                response = result.Error switch
                {
                    WorkerApiNotFoundException notFound => Results.Problem(
                        statusCode: StatusCodes.Status404NotFound, title: "Not found",
                        detail: notFound.Message),
                    S3ValidationException validation => Results.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        title: "Validation failed",
                        detail: result.Error.Message,
                        extensions: new Dictionary<string, object?>
                        {
                            ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                        }),
                    S3SecretProfileUnavailableException => Results.Problem(
                        statusCode: StatusCodes.Status422UnprocessableEntity,
                        title: "Secret profile unavailable",
                        detail: result.Error.Message),
                    _ => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                        detail: result.Error!.Message),
                };
            }

            metrics.Track("seed-demo", GetStatusCode(response));
            return response;
        });

        return endpoints;
    }

    // Оператор панели (fallback "api" — заголовок X-Requested-By).
    private static string RequestedBy(HttpRequest http)
        => http.Headers.TryGetValue("X-Requested-By", out var value) && value.Count > 0
            ? value[0]!
            : "api";

    // Фактический код IResult (метрика {code} — числом ответа).
    private static int GetStatusCode(IResult result)
        => result switch
        {
            IStatusCodeHttpResult withCode => withCode.StatusCode ?? StatusCodes.Status500InternalServerError,
            _ => StatusCodes.Status500InternalServerError,
        };
}
