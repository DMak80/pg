using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace EtcdWorker.Etcd;

/// <summary>Значение статус-ключа /etcdworker/snapshots (arch/etcdworker/00-etcd-contract.md): факт
/// последней выгрузки слепка etcd в S3 (перенос t08; поле enabled удалено —
/// выгрузка безусловна).</summary>
public sealed record EtcdSnapshotStatus(
    string State,               // "OK" | "FAILED"
    long? LastUploadedUnix,
    string? LastObject,
    string? LastSha256,
    long? SizeBytes,
    int? IntervalMin,
    string? Error)
{
    /// <summary>Выгрузка отстаёт: FAILED, либо ключа нет вовсе, либо новейший
    /// локальный слепок снят позже подтверждённого покрытия. Выгрузка
    /// безусловна (enabled-поля нет, arch/etcdworker/00-etcd-contract.md) — «нет ключа» = отстаёт при
    /// наличии локальных слепков.</summary>
    public static bool IsBehind(EtcdSnapshotStatus? status, long? latestLocalTakenUnix)
        => status is null
            ? latestLocalTakenUnix is not null
            : status.State == "FAILED"
              || (latestLocalTakenUnix is { } taken
                  && (status.LastUploadedUnix is not { } uploaded || taken > uploaded));

    /// <summary>Таймстемп имени слепка snapshot-&lt;yyyyMMdd-HHmmss&gt;.db (UTC,
    /// формат SnapshotJob); чужое имя → null.</summary>
    public static long? TakenUnixFromName(string fileName)
    {
        if (!fileName.StartsWith("snapshot-", StringComparison.Ordinal)
            || !fileName.EndsWith(".db", StringComparison.Ordinal))
            return null;
        var stamp = fileName.Substring("snapshot-".Length, fileName.Length - "snapshot-".Length - ".db".Length);
        return DateTimeOffset.TryParseExact(
            stamp, "yyyyMMdd-HHmmss", CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var taken)
            ? taken.ToUnixTimeSeconds()
            : null;
    }
}

/// <summary>Чистые функции статус-ключа: сериализация OK/FAILED (FAILED
/// сохраняет поля последнего успеха — оператор видит, насколько отстал),
/// толерантный парсинг. IsBehind/TakenUnixFromName — на record
/// EtcdSnapshotStatus (call-сайты зовут их от типа записи).</summary>
public static class EtcdSnapshotStatusJson
{
    public const string Key = "/etcdworker/snapshots";

    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private sealed record Payload(
        [property: JsonPropertyName("state")] string State,
        [property: JsonPropertyName("last_uploaded_unix")] long? LastUploadedUnix,
        [property: JsonPropertyName("last_object")] string? LastObject,
        [property: JsonPropertyName("last_sha256")] string? LastSha256,
        [property: JsonPropertyName("size_bytes")] long? SizeBytes,
        [property: JsonPropertyName("interval_min")] int? IntervalMin,
        [property: JsonPropertyName("error")] string? Error);

    /// <summary>Успешная выгрузка: state=OK, поля факта;
    /// coveredTakenUnix — метка ПОКРЫВАЕМОГО слепка (taken из имени),
    /// не now (§3.1/§3.6: фактическое время put'а — только meta.uploaded_unix).</summary>
    public static string Ok(long coveredTakenUnix, string lastObject, string lastSha256, long sizeBytes, int intervalMin)
        => Serialize(new Payload("OK", coveredTakenUnix, lastObject, lastSha256, sizeBytes, intervalMin, null));

    /// <summary>Неудача: state=FAILED + error; поля последнего успеха переносятся
    /// (null-поля lastOk опускаются — минимальный формат).</summary>
    public static string Failed(EtcdSnapshotStatus? lastOk, string error, int intervalMin)
        => Serialize(new Payload(
            "FAILED", lastOk?.LastUploadedUnix, lastOk?.LastObject, lastOk?.LastSha256,
            lastOk?.SizeBytes, intervalMin, error));

    /// <summary>Толерантный парсинг: битый JSON/поля → null (панель молчит,
    /// воркер перезапишет фактом); отсутствующие поля — null-компоненты.</summary>
    public static EtcdSnapshotStatus? Parse(string raw)
    {
        try
        {
            var payload = JsonSerializer.Deserialize<Payload>(raw, Json);
            if (payload?.State is not ("OK" or "FAILED"))
                return null;
            return new EtcdSnapshotStatus(
                payload.State, payload.LastUploadedUnix, payload.LastObject,
                payload.LastSha256, payload.SizeBytes, payload.IntervalMin, payload.Error);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string Serialize(Payload payload) => JsonSerializer.Serialize(payload, Json);
}
