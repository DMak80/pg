using System.Diagnostics;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using OwnS3.App.Access;
using OwnS3.App.Routing;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.Xml;
using OwnS3.Storage;
using Shared.Metrics;

namespace OwnS3.App.Pipeline;

// Конвейер S3-грани (глава 03 + решения spec §3.4): RequestId → OPTIONS →
// роутер (вне-наборные 501 / не-матч 400) → аутентификация SigV4 → матрица
// прав → диспетчеризация хендлера → единый обработчик ошибок → метрики и
// структурный лог. Служебные пути (/healthz, /metrics) пропускаются до
// зарегистрированных endpoint'ов (Map-эндпоинты исполняются в конце конвейера).
public sealed class S3Middleware(
    RequestDelegate next,
    AccessKeyRegistry registry,
    TimeProvider timeProvider,
    OwnS3Metrics metrics,
    Shared.Metrics.MetricsOptions metricsOptions,
    IOptions<OwnS3Options> options,
    ILogger<S3Middleware> logger,
    IEnumerable<Handlers.IOperationHandler> handlers)
{
    // Словарь диспетчеризации: операция → хендлер (22 хендлера протокольного контура).
    private readonly IReadOnlyDictionary<Routing.S3Operation, Handlers.IOperationHandler> _handlers =
        handlers.ToDictionary(h => h.Operation);
    public async Task InvokeAsync(HttpContext context)
    {
        // Шаг 0: служебные пути — точное совпадение, запрос уходит в endpoint'ы.
        var path = context.Request.Path;
        if (path == "/healthz" || (metricsOptions.Enabled && path == metricsOptions.Path))
        {
            await next(context);
            return;
        }

        var requestId = Guid.NewGuid().ToString();
        context.Response.Headers["x-amz-request-id"] = requestId;

        var operationLabel = "unknown";
        string? bucket = null, key = null;
        var stopwatch = Stopwatch.StartNew();
        var method = context.Request.Method;

        try
        {
            using (logger.BeginScope(new Dictionary<string, object> { ["RequestId"] = requestId }))
            {
                // Шаг 2: OPTIONS — пустой 200 без CORS-заголовков, до аутентификации (арх-правка 5).
                if (HttpMethods.IsOptions(method))
                {
                    context.Response.StatusCode = StatusCodes.Status200OK;
                    return;
                }

                // Шаг 3: роутер 22 операций.
                var model = S3ModelFactory.Create(context);
                var hasCopySource = context.Request.Headers.ContainsKey("x-amz-copy-source");
                var route = S3Router.Route(method, model.RawPath, model.RawQuery, hasCopySource);
                bucket = route.Bucket;
                key = route.Key;

                if (route.RejectedSubresource is not null)
                {
                    // Вне-наборный сабресурс → 501 NotImplemented ДО аутентификации (глава 02 §1).
                    operationLabel = route.RejectedSubresource;
                    await WriteErrorAsync(context, new S3Error(S3ErrorCode.NotImplemented, Resource: model.RawPath,
                        RequestId: requestId, HostId: options.Value.HostId));
                    return;
                }

                if (route.Operation == S3Operation.None)
                {
                    // Не-матч → 400 InvalidArgument «Unsupported request» (арх-правка 6).
                    operationLabel = "unknown";
                    await WriteErrorAsync(context, new S3Error(S3ErrorCode.InvalidArgument,
                        Resource: model.RawPath, RequestId: requestId, HostId: options.Value.HostId,
                        MessageOverride: "Unsupported request"));
                    return;
                }
                operationLabel = route.Operation.ToString();

                // Шаг 4: аутентификация (заголовочная / presigned / аноним → AccessDenied).
                var authenticator = new S3Authenticator(
                    new SigV4HeaderVerifier(timeProvider), new PresignedRequestVerifier(timeProvider), registry);
                var outcome = authenticator.Authenticate(context, model, route.Operation);
                if (outcome.Error is not null)
                {
                    await WriteErrorAsync(context, new S3Error(outcome.Error.Code,
                        Resource: model.RawPath, RequestId: requestId, HostId: options.Value.HostId,
                        MessageOverride: outcome.Error.Message));
                    return;
                }

                // Шаг 5: авторизация по матрице прав (глава 05 §3).
                var identity = outcome.Identity!;
                if (!OperationAccessMatrix.IsAllowed(identity.Policy, route.Operation))
                {
                    await WriteErrorAsync(context, new S3Error(S3ErrorCode.AccessDenied,
                        Resource: model.RawPath, RequestId: requestId, HostId: options.Value.HostId));
                    return;
                }

                // Шаг 6: диспетчеризация хендлера (задача 10 подставляет словарь операций).
                await DispatchAsync(context, new RequestContext
                {
                    RequestId = requestId,
                    Route = route,
                    Identity = identity,
                    Model = model,
                });
            }
        }
        catch (ObjectStoreException ex)
        {
            // Доменные исходы объектного слоя → S3-код по каталогу.
            await WriteErrorAsync(context, new S3Error(MapStorageCode(ex.Code),
                Resource: path, RequestId: requestId, HostId: options.Value.HostId));
        }
        catch (ObjectStoreUnavailableException)
        {
            // Заглушка t36 → 500 InternalError (служебного кода в каталоге нет).
            await WriteErrorAsync(context, new S3Error(S3ErrorCode.InternalError,
                Resource: path, RequestId: requestId, HostId: options.Value.HostId));
        }
        catch (S3ProtocolException ex)
        {
            await WriteErrorAsync(context, new S3Error(ex.Code,
                Resource: path, RequestId: requestId, HostId: options.Value.HostId,
                MessageOverride: ex.Message));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Необработанная ошибка S3-запроса requestId={RequestId}", requestId);
            await WriteErrorAsync(context, new S3Error(S3ErrorCode.InternalError,
                Resource: path, RequestId: requestId, HostId: options.Value.HostId));
        }
        finally
        {
            stopwatch.Stop();
            // Шаг 8: метрики + структурный лог запроса (все семь полей spec §3.4 п.8).
            metrics.RequestCompleted(operationLabel, context.Response.StatusCode, stopwatch.Elapsed);
            logger.LogInformation(
                "s3 request: requestId={RequestId} operation={Operation} bucket={Bucket} key={Key} method={Method} status={Status} durationMs={DurationMs}",
                requestId, operationLabel, bucket ?? string.Empty, key ?? string.Empty, method,
                context.Response.StatusCode, stopwatch.Elapsed.TotalMilliseconds);
        }
    }

    // Диспетчеризация: хендлер по операции; отсутствующий — 500 InternalError.
    private Task DispatchAsync(HttpContext context, RequestContext requestContext)
    {
        if (_handlers.TryGetValue(requestContext.Route.Operation, out var handler))
            return handler.HandleAsync(
                new Handlers.S3HandlerContext(context, requestContext), context.RequestAborted);

        context.Response.StatusCode = StatusCodes.Status500InternalServerError;
        return WriteErrorBodyAsync(context, new S3Error(S3ErrorCode.InternalError,
            Resource: requestContext.Model.RawPath, RequestId: requestContext.RequestId,
            HostId: options.Value.HostId));
    }

    // Канонический XML-ответ ошибки (HEAD — только статус/заголовки).
    private static async Task WriteErrorAsync(HttpContext context, S3Error error)
    {
        context.Response.StatusCode = error.Info.HttpStatus;
        await WriteErrorBodyAsync(context, error);
    }

    private static Task WriteErrorBodyAsync(HttpContext context, S3Error error)
    {
        if (HttpMethods.IsHead(context.Request.Method))
            return Task.CompletedTask;   // HEAD-ошибки — без тела (глава 03 §5)

        var xml = S3ErrorXmlWriter.Write(error);
        context.Response.ContentType = "application/xml";
        return context.Response.WriteAsync(xml);
    }

    // Маппинг доменных кодов Storage в каталог S3-ошибок (имена синхронны).
    private static S3ErrorCode MapStorageCode(ObjectStoreErrorCode code) => code switch
    {
        ObjectStoreErrorCode.NoSuchBucket => S3ErrorCode.NoSuchBucket,
        ObjectStoreErrorCode.NoSuchKey => S3ErrorCode.NoSuchKey,
        ObjectStoreErrorCode.BucketAlreadyOwnedByYou => S3ErrorCode.BucketAlreadyOwnedByYou,
        ObjectStoreErrorCode.BucketNotEmpty => S3ErrorCode.BucketNotEmpty,
        ObjectStoreErrorCode.InvalidPart => S3ErrorCode.InvalidPart,
        ObjectStoreErrorCode.InvalidPartOrder => S3ErrorCode.InvalidPartOrder,
        ObjectStoreErrorCode.NoSuchUpload => S3ErrorCode.NoSuchUpload,
        ObjectStoreErrorCode.InvalidRange => S3ErrorCode.InvalidRange,
        ObjectStoreErrorCode.PreconditionFailed => S3ErrorCode.PreconditionFailed,
        ObjectStoreErrorCode.NotModified => S3ErrorCode.NotModified,
        ObjectStoreErrorCode.EntityTooLarge => S3ErrorCode.EntityTooLarge,
        _ => S3ErrorCode.InternalError,
    };
}
