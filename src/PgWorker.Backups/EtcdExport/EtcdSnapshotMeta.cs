using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.Backups.EtcdExport;

/// <summary>Метаданные выгрузки слепка etcd (t08, spec §3.2): объект
/// etcd/snapshot-&lt;id&gt;.meta.json в bucket бэкапов. UploadedUnix —
/// фактическое время put'а объекта; TakenUnix — метка снятия из имени файла
/// (семантика «покрытия» статус-ключа — не здесь). Revision — ревизия etcd
/// на момент снятия (best-effort: не снялась — null, поле опускается).</summary>
public sealed record EtcdSnapshotMeta(
    string Sha256,
    long SizeBytes,
    long? Revision,
    long TakenUnix,
    long UploadedUnix,
    string Instance);

/// <summary>Чистые функции meta-JSON слепка: сериализация/толерантный парсинг
/// (паттерн EtcdSnapshotStatusJson).</summary>
public static class EtcdSnapshotMetaJson
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private sealed record Payload(
        [property: JsonPropertyName("sha256")] string Sha256,
        [property: JsonPropertyName("size_bytes")] long SizeBytes,
        [property: JsonPropertyName("revision")] long? Revision,
        [property: JsonPropertyName("taken_unix")] long TakenUnix,
        [property: JsonPropertyName("uploaded_unix")] long UploadedUnix,
        [property: JsonPropertyName("instance")] string Instance);

    /// <summary>Сериализация meta-JSON; revision=null — поле опускается
    /// (best-effort ревизии, слепок не виноват).</summary>
    public static string Serialize(EtcdSnapshotMeta meta)
        => JsonSerializer.Serialize(
            new Payload(meta.Sha256, meta.SizeBytes, meta.Revision, meta.TakenUnix, meta.UploadedUnix, meta.Instance),
            Json);

    /// <summary>Толерантный парсинг: битый JSON → null; отсутствующие поля
    /// обязательны — парсинг строго по схеме.</summary>
    public static EtcdSnapshotMeta? Parse(string raw)
    {
        try
        {
            var payload = JsonSerializer.Deserialize<Payload>(raw, Json);
            return payload is null
                ? null
                : new EtcdSnapshotMeta(
                    payload.Sha256, payload.SizeBytes, payload.Revision,
                    payload.TakenUnix, payload.UploadedUnix, payload.Instance);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
