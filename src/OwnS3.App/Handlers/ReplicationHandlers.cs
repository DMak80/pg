using System.Text.Json;
using Microsoft.AspNetCore.Http;
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
    IReplicaStore store, TombstoneStore tombstones, string hostId)
{
    private static readonly JsonSerializerOptions Json = ReplicationTransportJson.Options;

    // POST /replication/notifications: JSON-батч → inbox.Handle → 200
    // {"acceptedToSeqNo": N} | 429 (очередь фетчей полна) | 400 (malformed JSON).
    public async Task HandleNotificationsAsync(HttpContext context, string sourceName)
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
        var accepted = inbox.Handle(sourceName, batch);
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
    public async Task HandleObjectFetchAsync(HttpContext context, string bucket, string key)
    {
        var state = store.ReadKeyState(bucket, key);
        if (state is not null)
        {
            var opened = store.OpenObjectForReplication(bucket, key);
            context.Response.StatusCode = StatusCodes.Status200OK;
            context.Response.Headers["x-owns3-repl-modtime"] =
                opened.Metadata.LastModified.ToUnixTimeMilliseconds().ToString();
            context.Response.Headers["x-owns3-repl-sha256"] = state.ContentSha256Hex;
            context.Response.Headers["x-owns3-repl-etag"] = opened.Metadata.ETag;
            context.Response.Headers["x-owns3-repl-origin"] = hostId;
            if (!string.IsNullOrEmpty(opened.Metadata.ContentType))
                context.Response.ContentType = opened.Metadata.ContentType;
            foreach (var (name, value) in opened.Metadata.UserMetadata)
                context.Response.Headers[$"x-amz-meta-{name}"] = value;
            // Явная длина: потоковая отдача без Content-Length → chunked →
            // приёмник не знает длину для xl-коммита (глава 07 §6)
            context.Response.ContentLength = opened.Metadata.Size;
            await opened.Body.CopyToAsync(context.Response.Body, context.RequestAborted);
            opened.Body.Dispose();
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
    public async Task HandleListingAsync(HttpContext context)
    {
        var bucket = context.Request.Query["bucket"].FirstOrDefault();
        if (string.IsNullOrEmpty(bucket))
        {
            // Листинг без bucket — состав бакетов (backfill reconcile, глава 07 §6/§9)
            var buckets = store.ListBuckets()
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
        var entries = store.ListReplicationEntries(bucket, string.IsNullOrEmpty(marker) ? null : marker,
            maxKeys, out var nextMarker);
        context.Response.StatusCode = StatusCodes.Status200OK;
        await context.Response.WriteAsJsonAsync(new ReplicationListingResponse(entries, nextMarker),
            Json, context.RequestAborted);
    }

    // Соседи, известные приёмнику (для резолва sourceName middleware'ом — §2).
    public bool TryResolveSource(string accessKey, out string sourceName)
    {
        var match = targets.Snapshot.FirstOrDefault(t => t.AccessKey == accessKey);
        sourceName = match?.Name ?? string.Empty;
        return match is not null;
    }
}
