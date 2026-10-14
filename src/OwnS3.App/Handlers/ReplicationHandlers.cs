using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using OwnS3.App.Pipeline;
using OwnS3.Replication;
using OwnS3.Replication.Inbound;
using OwnS3.Replication.Lww;
using OwnS3.Replication.Transport;
using OwnS3.Storage;

namespace OwnS3.App.Handlers;

// Replication-грань (t35, глава 07 §6): приём уведомлений, фетч-отдача, листинг.
// Вызов — из S3Middleware ПОСЛЕ аутентификации (SigV4 или peer-схема OWNS3-PEER, t39)
// и проверки политики/состава; sourceName резолвится middleware'ом (accessKey —
// legacy-ветка, имя из Credential — key-ветка). Входящий join — HandleJoinAsync.
public sealed class ReplicationHandlers(ReplicationTargets targets, NotificationInbox inbox,
    IReplicaStore store, TombstoneStore tombstones, string hostId, ILogger<ReplicationHandlers> logger,
    JoinPeerService joinService)
{

    private static readonly JsonSerializerOptions Json = ReplicationTransportJson.Options;

    // POST /replication/notifications: JSON-батч → inbox.Handle → 200
    // {"acceptedToSeqNo": N} | 429 (очередь фетчей полна) | 400 (malformed JSON).
    public async Task HandleNotificationsAsync(HttpContext context, string sourceName,
        string requestId, string? errorHostId)
    {
        NotificationBatchRequest? batch;
        try
        {
            batch = await JsonSerializer.DeserializeAsync<NotificationBatchRequest>(
                context.Request.Body, Json, context.RequestAborted);
        }
        catch (JsonException)
        {
            batch = null;
        }
        if (batch is null || batch.Events is null)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(new { error = "malformed notification batch" },
                context.RequestAborted);
            return;
        }
        long? accepted;
        try
        {
            accepted = inbox.Handle(sourceName, batch);
        }
        catch (Exception ex) when (ex is ObjectStoreException or XlInvalidArgumentException)
        {
            // Доменный техотказ применения — трансляция как сегодня (middleware-ветка
            // без LogError): кворум → 503 + XML, длинный ключ → 400 + XML
            await S3ResultTranslator.WriteAsync(context, ex, requestId, errorHostId);
            return;
        }
        catch (Exception ex)
        {
            // Не-доменный — LogError (эквивалент catch-all middleware: сегодня логируется)
            // + 500 InternalError с XML-телом (тот же исход)
            logger.LogError(ex, "Приём уведомлений от {Source} упал", sourceName);
            await S3ResultTranslator.WriteAsync(context, ex, requestId, errorHostId);
            return;
        }
        if (accepted is null)
        {
            // Приёмный буфер переполнен — источник бэкоффит (глава 07 §6)
            context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
            return;
        }
        context.Response.StatusCode = StatusCodes.Status200OK;
        await context.Response.WriteAsJsonAsync(new NotificationAcceptedResponse(accepted.Value), Json,
            context.RequestAborted);
    }

    // GET /replication/object/{bucket}/{key+}: 200 + тело + x-owns3-repl-{modtime,sha256,
    // etag,origin} + Content-Type + x-amz-meta-* | 200 + x-owns3-repl-tombstone (без тела)
    // | 404 (ключ неизвестен — состояние приёмника не меняется).
    public async Task HandleObjectFetchAsync(HttpContext context, string bucket, string key,
        string requestId, string? errorHostId)
    {
        var stateResult = store.ReadKeyState(bucket, key);
        if (!stateResult.IsSuccess)
        {
            // Техотказ чтения (кворумные Insufficient*/длинный ключ) — НЕ «Missing»:
            // 503/400 + XML по каталогу (принцип 3 spec; канон 03 §5)
            await S3ResultTranslator.WriteAsync(context, stateResult.Error!, requestId, errorHostId);
            return;
        }
        if (stateResult.Value is ReplicaKeyLookup.Found found)
        {
            var opened = store.OpenObjectForReplication(bucket, key);
            if (!opened.IsSuccess)
            {
                // NoSuchKey-гонка удаления → 404 + XML NoSuchKey; кворумные Insufficient* →
                // 503 + XML; прочие → 500 — эквивалент демонтированной пары
                // «доменный catch + catch-all» middleware (канон 03 §5, принцип 2 spec)
                await S3ResultTranslator.WriteAsync(context, opened.Error!, requestId, errorHostId);
                return;
            }
            var (metadata, body) = opened.Value;
            context.Response.StatusCode = StatusCodes.Status200OK;
            context.Response.Headers["x-owns3-repl-modtime"] =
                metadata.LastModified.ToUnixTimeMilliseconds().ToString();
            context.Response.Headers["x-owns3-repl-sha256"] = found.State.ContentSha256Hex;
            context.Response.Headers["x-owns3-repl-etag"] = metadata.ETag;
            context.Response.Headers["x-owns3-repl-origin"] = hostId;
            if (!string.IsNullOrEmpty(metadata.ContentType))
                context.Response.ContentType = metadata.ContentType;
            foreach (var (name, value) in metadata.UserMetadata)
                context.Response.Headers[$"x-amz-meta-{name}"] = value;
            // Явная длина: потоковая отдача без Content-Length → chunked →
            // приёмник не знает длину для xl-коммита (глава 07 §6)
            context.Response.ContentLength = metadata.Size;
            await body.CopyToAsync(context.Response.Body, context.RequestAborted);
            body.Dispose();
            return;
        }
        var tombstone = tombstones.GetDeleteTime(bucket, key);
        if (tombstone is not null)
        {
            // Ключ удалён — tombstone-ответ без тела (глава 07 §6)
            context.Response.StatusCode = StatusCodes.Status200OK;
            context.Response.Headers["x-owns3-repl-tombstone"] = tombstone.Value.ToString();
            return;
        }
        // Ключа нет, tombstone-знание истекло — приёмник состояние не меняет
        context.Response.StatusCode = StatusCodes.Status404NotFound;
    }

    // GET /replication/listing?bucket=&marker=&max-keys=: постраничный листинг ключей
    // (reconcile/backfill, глава 07 §6/§9); max-keys дефолт 1000, потолок 1000.
    public async Task HandleListingAsync(HttpContext context, string requestId, string? errorHostId)
    {
        var bucket = context.Request.Query["bucket"].FirstOrDefault();
        if (string.IsNullOrEmpty(bucket))
        {
            // Листинг без bucket — состав бакетов (backfill reconcile, глава 07 §6/§9)
            var bucketList = store.ListBuckets();
            if (!bucketList.IsSuccess)
            {
                // Кворумный отказ листинга → 503 + XML (канон 06 §7, 03 §5)
                await S3ResultTranslator.WriteAsync(context, bucketList.Error!, requestId, errorHostId);
                return;
            }
            var buckets = bucketList.Value
                .Select(b => new BucketEntryDto(b.Name, b.CreationDate.ToUnixTimeMilliseconds())).ToList();
            context.Response.StatusCode = StatusCodes.Status200OK;
            await context.Response.WriteAsJsonAsync(new ReplicationBucketsResponse(buckets), Json,
                context.RequestAborted);
            return;
        }
        var marker = context.Request.Query["marker"].FirstOrDefault();
        var maxKeys = 1000;
        if (int.TryParse(context.Request.Query["max-keys"].FirstOrDefault(), out var requested))
            maxKeys = Math.Clamp(requested, 1, 1000);
        var page = store.ListReplicationEntries(bucket, string.IsNullOrEmpty(marker) ? null : marker,
            maxKeys);
        if (!page.IsSuccess)
        {
            // Кворумный отказ листинга → 503 + XML (канон 06 §7, 03 §5)
            await S3ResultTranslator.WriteAsync(context, page.Error!, requestId, errorHostId);
            return;
        }
        context.Response.StatusCode = StatusCodes.Status200OK;
        await context.Response.WriteAsJsonAsync(
            new ReplicationListingResponse(page.Value.Entries, page.Value.NextMarker), Json, context.RequestAborted);
    }

    // GET /replication/ping (глава 07 §6): 200 без тела — активная проба линка;
    // приёмник ничего не пишет и не применяет (auth/политика/резолв соседа —
    // конвейер S3Middleware, источник имени не используется).
    public Task HandlePingAsync(HttpContext context)
    {
        context.Response.StatusCode = StatusCodes.Status200OK;
        return Task.CompletedTask;
    }

    // Соседи, известные приёмнику (для резолва sourceName middleware'ом — §2).
    public bool TryResolveSource(string accessKey, out string sourceName)
    {
        var match = targets.Snapshot.FirstOrDefault(t => t.AccessKey == accessKey);
        sourceName = match?.Name ?? string.Empty;
        return match is not null;
    }

    // Резолв key-пира по имени из Credential (t39, глава 07 §6): запись состава
    // с этим именем и auth=key; отсутствует/не key → false (грань ответит 403).
    public bool TryResolvePeer(string name, out TargetRecord target)
    {
        targets.TryGet(name, out var found);
        target = found!;
        return found is { Auth: TargetRecord.AuthKey };
    }

    // POST /replication/join (t39, глава 07 §2/spec §4.4): входящая сторона handshake —
    // до-join аутентификация по сертификату X-OwnS3-Peer-Certificate и подписи запроса
    // (JoinPeerService.HandleIncoming); 200 {name, advertisedEndpoint, certificate} с
    // заголовком X-OwnS3-Peer-Signature (тело — та же строка, что подписана) |
    // 403 (отказ trust/CA/deny/подписи) | 409 (конфликт имени) | 400 (malformed).
    public async Task HandleJoinAsync(HttpContext context, string requestId, string? errorHostId)
    {
        // Модель — та же сборка, что в middleware (сырой путь/регистронезависимые
        // заголовки); тело верификатор не читает (хэш — из заголовка схемы).
        var joinModel = S3ModelFactory.Create(context);
        var certificateHeader = context.Request.Headers["X-OwnS3-Peer-Certificate"].FirstOrDefault();
        if (string.IsNullOrEmpty(certificateHeader))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(new { error = "X-OwnS3-Peer-Certificate header required" },
                context.RequestAborted);
            return;
        }
        JoinRequestBody? body;
        try
        {
            body = await JsonSerializer.DeserializeAsync<JoinRequestBody>(context.Request.Body, Json,
                context.RequestAborted);
        }
        catch (JsonException)
        {
            body = null;
        }
        if (string.IsNullOrWhiteSpace(body?.AdvertisedEndpoint))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(new { error = "malformed join body" }, context.RequestAborted);
            return;
        }
        // Advertised пира — до записи в состав (валидация ReplicationTargets бросила
        // бы ArgumentException из HandleIncoming → 500): мусорный URI из тела — 400.
        if (!Uri.TryCreate(body.AdvertisedEndpoint, UriKind.Absolute, out var peerEndpoint)
            || peerEndpoint.Scheme is not ("http" or "https")
            || string.IsNullOrEmpty(peerEndpoint.Host))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(
                new { error = $"invalid advertisedEndpoint «{body.AdvertisedEndpoint}» (ожидалось scheme://host:port)" },
                context.RequestAborted);
            return;
        }

        var outcome = joinService.HandleIncoming(joinModel, certificateHeader, body.AdvertisedEndpoint);
        if (outcome.IsSuccess)
        {
            var accept = outcome.Value;
            // Тело ответа — та же строка, что подписана в HandleIncoming: стабильная
            // camelCase-сериализация с тем же порядком полей, что JoinResponseBody.
            var responseBody = JsonSerializer.Serialize(
                new { name = accept.Name, advertisedEndpoint = accept.AdvertisedEndpoint,
                    certificate = accept.CertificateBase64 }, Json);
            context.Response.StatusCode = StatusCodes.Status200OK;
            context.Response.Headers["X-OwnS3-Peer-Signature"] = accept.SignatureBase64;
            await context.Response.WriteAsync(responseBody, context.RequestAborted);
            return;
        }
        switch (outcome.Error)
        {
            case JoinRefusedException refused:
                // Отказ проверки подписи запроса — код каталога подписи (глава 07 §6:
                // SignatureDoesNotMatch/RequestTimeTooSkewed/…, XML-ответ); прочие причины
                // отказа (trust/CA/deny) — AccessDenied в том же XML-формате грани.
                // Исключение — malformed Authorization: по каталогу §4.2/§6 это 400
                // (peer-ветка middleware отдаёт именно 400) — выравнивание веток грани.
                var code = refused.ErrorCode ?? OwnS3.Protocol.Errors.S3ErrorCode.AccessDenied;
                context.Response.StatusCode =
                    code == OwnS3.Protocol.Errors.S3ErrorCode.AuthorizationHeaderMalformed
                        ? StatusCodes.Status400BadRequest
                        : StatusCodes.Status403Forbidden;
                context.Response.ContentType = "application/xml";
                await context.Response.WriteAsync(OwnS3.Protocol.Xml.S3ErrorXmlWriter.Write(
                    new OwnS3.Protocol.Xml.S3Error(code, Resource: context.Request.Path,
                        RequestId: requestId, HostId: errorHostId, MessageOverride: refused.Message)),
                    context.RequestAborted);
                return;
            case JoinConflictException conflict:
                // Конфликт имени (другой публичный ключ) — 409 JSON (стиль join-контрактов).
                context.Response.StatusCode = StatusCodes.Status409Conflict;
                await context.Response.WriteAsJsonAsync(new { error = conflict.Message }, context.RequestAborted);
                return;
            default:
                // Неожиданный тип отказа — прежний путь catch-all middleware (500 XML).
                throw outcome.Error!;
        }
    }

    // Тело входящего join-запроса (глава 07 §2).
    private sealed record JoinRequestBody(string AdvertisedEndpoint);
}
