using System.Text.Json;

namespace PgWorker.Backups;

/// <summary>Занятость хранилища установки — ключ /pgworker/backups/storage
/// (t06, arch/19 §4): глобальный, пишет ретенционный проход при изменении;
/// quota_bytes=0 → поля квоты опускаются, state=OK.</summary>
public sealed record StorageStatus(
    long UsedBytes, long QuotaBytes, double? UsedPercent, StorageState State, long UpdatedUnix);

// Сериализация/имена состояния ключа storage (по образцу BackupStatusJson).
public static class StorageStatusJson
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public static string Serialize(StorageStatus status)
    {
        var o = new Dictionary<string, object?>
        {
            ["used_bytes"] = status.UsedBytes,
            ["state"] = StateName(status.State),
            ["updated_unix"] = status.UpdatedUnix,
        };
        if (status.QuotaBytes > 0)
        {
            o["quota_bytes"] = status.QuotaBytes;
            o["used_percent"] = status.UsedPercent;
        }

        return JsonSerializer.Serialize(o, Options);
    }

    public static string StateName(StorageState state) => state switch
    {
        StorageState.Warn => "WARN",
        StorageState.Crit => "CRIT",
        _ => "OK",
    };
}
