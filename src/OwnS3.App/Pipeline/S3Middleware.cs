using System.Diagnostics;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using OwnS3.App.Access;
using OwnS3.App.Routing;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.PeerAuth;
using OwnS3.Protocol.Requests;
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
    Handlers.ReplicationHandlers replicationHandlers,
    Handlers.ReplicationAdminHandlers replicationAdminHandlers,
    IEnumerable<Handlers.IOperationHandler> handlers)
{
    // Словарь диспетчеризации: операция → хендлер (21 хендлер протокольного контура).
    private readonly IReadOnlyDictionary<Routing.S3Operation, Handlers.IOperationHandler> _handlers =
        handlers.ToDictionary(h => h.Operation);
    public async Task InvokeAsync(HttpContext context)
    {
        // Шаг 0: служебные пути — точное совпадение, запрос уходит в endpoint'ы.
        var path = context.Request.Path;
        if (path == "/healthz"
            || path.StartsWithSegments("/s3/health")    // кластер-health: до роутинга 21 операции (канон 05 §5); сегментная граница — /s3/healthzz не матчится
            || (metricsOptions.Enabled && path == metricsOptions.Path))
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

                // Шаг 2.5: админ-грань /admin/* (канон 05 §7) — до роутинга 21 операций:
                // те же шаги подписи SigV4, права admin/root (прочим 403 AccessDenied).
                if (context.Request.Path.StartsWithSegments("/admin"))
                {
                    // Mesh-репликация (t35, глава 07 §2/§10): те же права admin/root;
                    // replication-ключ на админ-грани → 403 (A13-зеркало).
                    // Маршрутизация (t39): ресурсы списком + хвостовые сегменты
                    // targets/{name} и denylist/{name} (DELETE) — остальное 400.
                    if (context.Request.Path.StartsWithSegments("/admin/replication"))
                    {
                        var replAdminPath = context.Request.Path.Value ?? string.Empty;
                        var replAdminSegments = replAdminPath.Length > "/admin/replication/".Length
                            ? replAdminPath["/admin/replication/".Length..].Split('/')
                            : [];
                        var isAdminRepl = replAdminPath is "/admin/replication/targets"
                            or "/admin/replication/status"
                            or "/admin/replication/reconcile"
                            or "/admin/replication/sync-report"
                            or "/admin/replication/probe"
                            or "/admin/replication/join"
                            or "/admin/replication/denylist"
                            || (HttpMethods.IsDelete(method)
                                && replAdminSegments is ["targets", _] or ["denylist", _]);
                        if (!isAdminRepl)
                        {
                            await WriteErrorAsync(context, new S3Error(OwnS3.Protocol.Errors.S3ErrorCode.InvalidArgument,
                                Resource: context.Request.Path, RequestId: requestId, HostId: options.Value.HostId,
                                MessageOverride: "Unknown admin resource"));
                            return;
                        }
                        operationLabel = "admin-replication";
                        var replAdminModel = S3ModelFactory.Create(context);
                        var replAdminAuth = new S3Authenticator(
                            new SigV4HeaderVerifier(timeProvider), new PresignedRequestVerifier(timeProvider), registry);
                        var replAdminOutcome = replAdminAuth.Authenticate(context, replAdminModel, S3Operation.None);
                        if (replAdminOutcome.Error is not null)
                        {
                            await WriteErrorAsync(context, new S3Error(replAdminOutcome.Error.Code,
                                Resource: replAdminModel.RawPath, RequestId: requestId, HostId: options.Value.HostId,
                                MessageOverride: replAdminOutcome.Error.Message));
                            return;
                        }
                        if (replAdminOutcome.Identity!.Policy != Access.AccessPolicy.Admin)
                        {
                            await WriteErrorAsync(context, new S3Error(OwnS3.Protocol.Errors.S3ErrorCode.AccessDenied,
                                Resource: replAdminModel.RawPath, RequestId: requestId, HostId: options.Value.HostId));
                            return;
                        }
                        if (HttpMethods.IsPut(method) && replAdminPath == "/admin/replication/targets")
                            await replicationAdminHandlers.HandlePutTargetsAsync(context);
                        else if (HttpMethods.IsPost(method) && replAdminPath == "/admin/replication/join")
                            await replicationAdminHandlers.HandleJoinTriggerAsync(context);
                        else if (HttpMethods.IsDelete(method) && replAdminSegments is ["targets", var targetName])
                            await replicationAdminHandlers.HandleDeleteTargetAsync(context, targetName,
                                context.Request.Query["revoke"] == "true");
                        else if (HttpMethods.IsGet(method) && replAdminPath == "/admin/replication/denylist")
                            await replicationAdminHandlers.HandleDenyListAsync(context);
                        else if (HttpMethods.IsDelete(method) && replAdminSegments is ["denylist", var denyName])
                            await replicationAdminHandlers.HandleDenyRemoveAsync(context, denyName);
                        else if (HttpMethods.IsGet(method) && replAdminPath == "/admin/replication/status")
                            await replicationAdminHandlers.HandleStatusAsync(context);
                        else if (HttpMethods.IsPost(method) && replAdminPath == "/admin/replication/reconcile")
                            await replicationAdminHandlers.HandleReconcileAsync(context);
                        else if (HttpMethods.IsGet(method) && replAdminPath == "/admin/replication/sync-report")
                            await replicationAdminHandlers.HandleSyncReportAsync(context, requestId, options.Value.HostId);
                        else if (HttpMethods.IsPost(method) && replAdminPath == "/admin/replication/probe")
                            await replicationAdminHandlers.HandleProbeAsync(context);
                        else
                            await WriteErrorAsync(context, new S3Error(OwnS3.Protocol.Errors.S3ErrorCode.InvalidArgument,
                                Resource: replAdminModel.RawPath, RequestId: requestId, HostId: options.Value.HostId,
                                MessageOverride: "Method not allowed for admin resource"));
                        return;
                    }

                    var isAdminPath = context.Request.Path.Value is "/admin/storage" or "/admin/heal";
                    if (!isAdminPath)
                    {
                        await WriteErrorAsync(context, new S3Error(OwnS3.Protocol.Errors.S3ErrorCode.InvalidArgument,
                            Resource: context.Request.Path, RequestId: requestId, HostId: options.Value.HostId,
                            MessageOverride: "Unknown admin resource"));
                        return;
                    }

                    operationLabel = "admin";
                    var adminModel = S3ModelFactory.Create(context);
                    var adminAuthenticator = new S3Authenticator(
                        new SigV4HeaderVerifier(timeProvider), new PresignedRequestVerifier(timeProvider), registry);
                    var adminOutcome = adminAuthenticator.Authenticate(context, adminModel, S3Operation.None);
                    if (adminOutcome.Error is not null)
                    {
                        await WriteErrorAsync(context, new S3Error(adminOutcome.Error.Code,
                            Resource: adminModel.RawPath, RequestId: requestId, HostId: options.Value.HostId,
                            MessageOverride: adminOutcome.Error.Message));
                        return;
                    }

                    if (adminOutcome.Identity!.Policy != Access.AccessPolicy.Admin)
                    {
                        await WriteErrorAsync(context, new S3Error(OwnS3.Protocol.Errors.S3ErrorCode.AccessDenied,
                            Resource: adminModel.RawPath, RequestId: requestId, HostId: options.Value.HostId));
                        return;
                    }

                    if (HttpMethods.IsGet(method) && context.Request.Path.Value == "/admin/storage")
                        await Handlers.AdminHandlers.HandleStorageAsync(context);
                    else if (HttpMethods.IsPost(method) && context.Request.Path.Value == "/admin/heal")
                        await Handlers.AdminHandlers.HandleHealAsync(context);
                    else
                        await WriteErrorAsync(context, new S3Error(OwnS3.Protocol.Errors.S3ErrorCode.InvalidArgument,
                            Resource: adminModel.RawPath, RequestId: requestId, HostId: options.Value.HostId,
                            MessageOverride: "Method not allowed for admin resource"));
                    return;
                }

                // Шаг 2.4: replication-грань /replication/* (t35, глава 07 §6) — до роутинга
                // 21 операций. Две схемы аутентификации (t39, глава 07 §6) — выбор по
                // префиксу заголовка Authorization: OWNS3-PEER-* → peer-конвейер (ниже),
                // AWS4-HMAC-SHA256 → существующий SigV4-конвейер (S3Operation.None),
                // политика строго AccessPolicy.Replication (иначе 403 AccessDenied, включая
                // admin/root — A13), sourceName — по accessKey подписи из targets
                // (не числится соседом → 403, A13), затем диспетчеризация операций.
                if (context.Request.Path.StartsWithSegments("/replication"))
                {
                    operationLabel = "replication";
                    var replModel = S3ModelFactory.Create(context);

                    // Peer-конвейер (t39, глава 07 §6): схема OWNS3-PEER-ECC-P256-SHA256
                    // для key-записей — ДО S3Authenticator; SigV4-ветка ниже не тронута.
                    var replAuthorization = context.Request.Headers.Authorization.ToString();
                    if (replAuthorization.StartsWith("OWNS3-PEER-", StringComparison.Ordinal))
                    {
                        // Join-под-ветка (глава 07 §2): единственный запрос грани от ещё
                        // не числящегося пира — до-join аутентификация по сертификату.
                        if (HttpMethods.IsPost(method) && context.Request.Path.Value == "/replication/join")
                        {
                            await replicationHandlers.HandleJoinAsync(context, requestId, options.Value.HostId);
                            return;
                        }

                        // Deny-replace-под-ветка (глава 07 §2/§6): PUT /replication/denylist —
                        // второй до-join запрос грани: вызывающий — оркестратор, не обязан числиться
                        // соседом; аутентификация по сертификату вызывающего (как у join), ДО
                        // резолва пира по составу.
                        if (HttpMethods.IsPut(method) && context.Request.Path.Value == "/replication/denylist")
                        {
                            await replicationHandlers.HandleDenyReplaceAsync(context, requestId, options.Value.HostId);
                            return;
                        }

                        // Имя пира из Credential; null при любом malformed → 400
                        // AuthorizationHeaderMalformed (маппинг здесь — Protocol без
                        // Result-сигнатур, глава 07 §6).
                        var peerName = PeerRequestVerifier.CredentialOf(replModel);
                        if (peerName is null)
                        {
                            await WriteErrorAsync(context, new S3Error(S3ErrorCode.AuthorizationHeaderMalformed,
                                Resource: replModel.RawPath, RequestId: requestId, HostId: options.Value.HostId));
                            return;
                        }
                        // Резолв по составу: записи нет / не key → 403 AccessDenied
                        // («не числится соседом», зеркало SigV4-ветки).
                        if (!replicationHandlers.TryResolvePeer(peerName, out var peerTarget))
                        {
                            await WriteErrorAsync(context, new S3Error(S3ErrorCode.AccessDenied,
                                Resource: replModel.RawPath, RequestId: requestId, HostId: options.Value.HostId));
                            return;
                        }
                        // Верификация подписи публичным ключом key-записи (ECDsa P-256);
                        // PEM валидирован при вставке в состав — битый после ручной правки
                        // targets.json → отказ идентификации (403), не 500.
                        using var peerKey = System.Security.Cryptography.ECDsa.Create();
                        try
                        {
                            peerKey.ImportFromPem(peerTarget.PublicKeyPem!);
                        }
                        catch (Exception)
                        {
                            await WriteErrorAsync(context, new S3Error(S3ErrorCode.AccessDenied,
                                Resource: replModel.RawPath, RequestId: requestId, HostId: options.Value.HostId));
                            return;
                        }
                        var peerVerify = new PeerRequestVerifier(timeProvider).Verify(replModel, peerKey);
                        if (peerVerify is not PeerVerifyResult.Ok peerOk)
                        {
                            // Каталог кодов подписи (глава 03 §5): SignatureDoesNotMatch/
                            // RequestTimeTooSkewed/…; прочее — AccessDenied.
                            var peerCode = peerVerify is PeerVerifyResult.Fail peerFail
                                ? peerFail.Code
                                : S3ErrorCode.AccessDenied;
                            await WriteErrorAsync(context, new S3Error(peerCode,
                                Resource: replModel.RawPath, RequestId: requestId, HostId: options.Value.HostId));
                            return;
                        }
                        await DispatchReplicationAsync(context, replModel, peerOk.PeerName, requestId);
                        return;
                    }

                    var replAuthenticator = new S3Authenticator(
                        new SigV4HeaderVerifier(timeProvider), new PresignedRequestVerifier(timeProvider), registry);
                    var replOutcome = replAuthenticator.Authenticate(context, replModel, S3Operation.None);
                    if (replOutcome.Error is not null)
                    {
                        await WriteErrorAsync(context, new S3Error(replOutcome.Error.Code,
                            Resource: replModel.RawPath, RequestId: requestId, HostId: options.Value.HostId,
                            MessageOverride: replOutcome.Error.Message));
                        return;
                    }
                    if (replOutcome.Identity!.Policy != Access.AccessPolicy.Replication)
                    {
                        await WriteErrorAsync(context, new S3Error(OwnS3.Protocol.Errors.S3ErrorCode.AccessDenied,
                            Resource: replModel.RawPath, RequestId: requestId, HostId: options.Value.HostId));
                        return;
                    }
                    if (!replicationHandlers.TryResolveSource(replOutcome.Identity!.AccessKey, out var sourceName))
                    {
                        // Валидный replication-ключ, не числящийся соседом (глава 07 §6)
                        await WriteErrorAsync(context, new S3Error(OwnS3.Protocol.Errors.S3ErrorCode.AccessDenied,
                            Resource: replModel.RawPath, RequestId: requestId, HostId: options.Value.HostId));
                        return;
                    }

                    await DispatchReplicationAsync(context, replModel, sourceName, requestId);
                    return;
                }

                // Шаг 3: роутер 21 операции.
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
                    HostId = options.Value.HostId,
                });
            }
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

        // Диспетчеризация операций replication-грани (общая для SigV4- и peer-веток,
        // глава 07 §6): notifications/object/listing/ping; неизвестный путь — 400.
        async Task DispatchReplicationAsync(HttpContext ctx, S3RequestModel model, string sourceName, string reqId)
        {
            if (HttpMethods.IsPost(method) && ctx.Request.Path.Value == "/replication/notifications")
                await replicationHandlers.HandleNotificationsAsync(ctx, sourceName, reqId, options.Value.HostId);
            else if (HttpMethods.IsGet(method) && ctx.Request.Path.StartsWithSegments("/replication/object"))
            {
                // Парсинг {bucket}/{key} — от под-пути грани (префикс /replication/object срезается)
                var replSubPath = ctx.Request.Path.Value!["/replication/object".Length..];
                var replPath = OwnS3.Protocol.Requests.S3PathParser.Parse(replSubPath);
                if (replPath.Bucket is null || replPath.Key is null)
                {
                    await WriteErrorAsync(ctx, new S3Error(S3ErrorCode.InvalidArgument,
                        Resource: model.RawPath, RequestId: reqId, HostId: options.Value.HostId,
                        MessageOverride: "bucket and key required"));
                    return;
                }
                await replicationHandlers.HandleObjectFetchAsync(ctx, replPath.Bucket, replPath.Key,
                    reqId, options.Value.HostId);
            }
            else if (HttpMethods.IsGet(method) && ctx.Request.Path.Value == "/replication/listing")
                await replicationHandlers.HandleListingAsync(ctx, reqId, options.Value.HostId);
            else if (HttpMethods.IsGet(method) && ctx.Request.Path.Value == "/replication/ping")
                await replicationHandlers.HandlePingAsync(ctx);
            else
                await WriteErrorAsync(ctx, new S3Error(S3ErrorCode.InvalidArgument,
                    Resource: model.RawPath, RequestId: reqId, HostId: options.Value.HostId,
                    MessageOverride: "Unknown replication resource"));
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

}
