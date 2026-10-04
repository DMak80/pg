using AdminPanel.Core;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

/// <summary>Толерантный парсер ключа /pgworker/etcd-snapshots (t08): битый
/// JSON/state → parseError-запись + null (правила молчат, толерантный читатель);
/// отсутствующие поля — null-компоненты.</summary>
public static class EtcdSnapshotsParser
{
    public static (EtcdSnapshotExportInfo? Info, IReadOnlyList<KeyParseError> Errors) Parse(Kv? kv)
    {
        if (kv is null)
            return (null, []);
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(kv.Value);
            var root = doc.RootElement;
            var enabled = root.TryGetProperty("enabled", out var e) && e.ValueKind == System.Text.Json.JsonValueKind.True;
            var state = JsonValues.ReadString(root, "state");
            var uploaded = JsonValues.ReadLong(root, "last_uploaded_unix");
            var lastObject = JsonValues.ReadString(root, "last_object");
            var sha = JsonValues.ReadString(root, "last_sha256");
            var size = JsonValues.ReadLong(root, "size_bytes");
            var interval = JsonValues.ReadInt(root, "interval_min");
            var error = JsonValues.ReadString(root, "error");
            if (state is not null and not ("OK" or "FAILED"))
                return (null, [new KeyParseError(kv.Key, $"неизвестный state статуса выгрузки: {state}")]);
            return (new EtcdSnapshotExportInfo(enabled, state, uploaded, lastObject, sha, size, interval, error), []);
        }
        catch (System.Text.Json.JsonException ex)
        {
            return (null, [new KeyParseError(kv.Key, $"битый JSON статуса выгрузки etcd-снапшотов: {ex.Message}")]);
        }
    }
}
