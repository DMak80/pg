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
// Вызов — из S3Middleware ПОСЛЕ аутентификации SigV4 и проверки политики строго
// Replication; sourceName резолвится middleware'ом по accessKey подписи из targets.
public sealed class ReplicationHandlers(ReplicationTargets targets, NotificationInbox inbox,
    IReplicaStore store, TombstoneStore tombstones, string hostId, ILogger<ReplicationHandlers> logger)
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
}
