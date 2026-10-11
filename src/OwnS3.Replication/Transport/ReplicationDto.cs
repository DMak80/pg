using System.Text.Json;
using System.Text.Json.Serialization;
using OwnS3.Storage;

namespace OwnS3.Replication.Transport;

// Транспортные модели replication-грани (t35, глава 07 §6): JSON camelCase.
// type события — kebab-case ("object-put", …, конвертер ниже).

// Батч уведомлений источника: POST /replication/notifications.
public sealed record NotificationBatchRequest(string OriginHostId, IReadOnlyList<ReplicationEvent> Events);

// Ответ приёма: 200 {"acceptedToSeqNo": N} — ACK приёма, двигающий курсор источника.
public sealed record NotificationAcceptedResponse(long AcceptedToSeqNo);

public sealed record ReplicationListingRequest(string Bucket, string? Marker, int MaxKeys);

// Постраничный листинг ключей (reconcile/backfill).
public sealed record ReplicationListingResponse(IReadOnlyList<ReplicationListEntry> Entries, string? NextMarker);

// Состав бакетов соседа (листинг без bucket — backfill, глава 07 §9).
public sealed record ReplicationBucketsResponse(IReadOnlyList<BucketEntryDto> Buckets);
public sealed record BucketEntryDto(string Name, long CreationDateUnixMs);

// Исход фетча: 200-объект (Body + заголовки x-owns3-repl-*), 200-tombstone
// (TombstoneUnixMs, Body=null), 404 (Body=null, TombstoneUnixMs=null — ключ неизвестен).
public sealed record FetchResult(Stream? Body, long ContentLength, long ModTimeUnixMs, string ContentSha256,
    string ETag, string ContentType, IReadOnlyDictionary<string, string> UserMetadata, long? TombstoneUnixMs);

// JSON-опции транспорта: camelCase + kebab-case enum событий (совместимо с гранью).
public static class ReplicationTransportJson
{
    public static JsonSerializerOptions Options { get; } = Create();

    private static JsonSerializerOptions Create()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
        options.Converters.Add(new ReplicationEventTypeJsonConverter());
        return options;
    }
}

// ReplicationEventType ↔ "object-put"|"object-delete"|"bucket-create"|"bucket-delete"
// (канон главы 07 §3 — имена событий kebab-case в уведомлениях).
public sealed class ReplicationEventTypeJsonConverter : JsonConverter<ReplicationEventType>
{
    public override ReplicationEventType Read(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options)
    {
        var value = reader.GetString()
            ?? throw new JsonException("Пустое имя типа события репликации");
        return value switch
        {
            "object-put" => ReplicationEventType.ObjectPut,
            "object-delete" => ReplicationEventType.ObjectDelete,
            "bucket-create" => ReplicationEventType.BucketCreate,
            "bucket-delete" => ReplicationEventType.BucketDelete,
            _ => throw new JsonException($"Неизвестный тип события репликации: {value}"),
        };
    }

    public override void Write(Utf8JsonWriter writer, ReplicationEventType value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value switch
        {
            ReplicationEventType.ObjectPut => "object-put",
            ReplicationEventType.ObjectDelete => "object-delete",
            ReplicationEventType.BucketCreate => "bucket-create",
            ReplicationEventType.BucketDelete => "bucket-delete",
            _ => throw new JsonException($"Неизвестный тип события репликации: {value}"),
        });
}
