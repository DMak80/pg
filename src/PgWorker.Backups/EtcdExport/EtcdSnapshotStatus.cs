using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.Backups.EtcdExport;

/// <summary>Значение статус-ключа /pgworker/etcd-snapshots (t08, arch/14 §3.3):
/// факт последней выгрузки слепка etcd в S3. Наблюдаемость; источник для
/// восстановления НЕ является (etcd мёртв — ключа нет). LastUploadedUnix —
/// семантика «покрытия» (§3.1): метка снятия последнего слепка, чьё содержимое
/// подтверждённо доставлено в S3 фактическим upload'ом — двигается каждым
/// успешным проходом sink'а.</summary>
public sealed record EtcdSnapshotStatus(
    bool Enabled,
    string State,               // "OK" | "FAILED"
    long? LastUploadedUnix,
    string? LastObject,
    string? LastSha256,
    long? SizeBytes,
    int? IntervalMin,
    string? Error)
{
    /// <summary>Выгрузка отстаёт: FAILED, либо ключа нет вовсе (включённая
    /// опция), либо новейший локальный слепок снят позже подтверждённого
    /// покрытия (возможен невыгруженный слепок). enabled=false — не отстаёт.
    /// Инвариант §3.5 п.1: каждый успешный проход sink'а продвигает
    /// last_uploaded_unix к метке обработанного слепка — «отстаёт» всегда
    /// означает «есть слепок новее покрытия», а не «давно не было put».</summary>
    public static bool IsBehind(EtcdSnapshotStatus? status, long? latestLocalTakenUnix)
        => status is null
           ? latestLocalTakenUnix is not null
           : status.Enabled
             && (status.State == "FAILED"
                 || (latestLocalTakenUnix is { } taken
                     && (status.LastUploadedUnix is not { } uploaded || taken > uploaded)));

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
    public const string Key = "/pgworker/etcd-snapshots";

    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private sealed record Payload(
        [property: JsonPropertyName("enabled")] bool Enabled,
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
        => Serialize(new Payload(true, "OK", coveredTakenUnix, lastObject, lastSha256, sizeBytes, intervalMin, null));

    /// <summary>Неудача: state=FAILED + error; поля последнего успеха переносятся
    /// (null-поля lastOk опускаются — минимальный формат).</summary>
    public static string Failed(EtcdSnapshotStatus? lastOk, string error, int intervalMin)
        => Serialize(new Payload(
            true, "FAILED", lastOk?.LastUploadedUnix, lastOk?.LastObject, lastOk?.LastSha256,
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
                payload.Enabled, payload.State, payload.LastUploadedUnix, payload.LastObject,
                payload.LastSha256, payload.SizeBytes, payload.IntervalMin, payload.Error);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string Serialize(Payload payload) => JsonSerializer.Serialize(payload, Json);
}
