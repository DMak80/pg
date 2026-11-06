using AdminPanel.Core;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

/// <summary>Толерантный парсер ключа /etcdnode/snapshots (arch/etcdnode/00-etcdnode-contract.md):
/// битый JSON/state → parseError-запись + null (правила молчат); отсутствующие
/// поля — null-компоненты; поля enabled в формате нет (выгрузка безусловна).</summary>
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
            var state = JsonValues.ReadString(root, "state");
            var uploaded = JsonValues.ReadLong(root, "last_uploaded_unix");
            var lastObject = JsonValues.ReadString(root, "last_object");
            var sha = JsonValues.ReadString(root, "last_sha256");
            var size = JsonValues.ReadLong(root, "size_bytes");
            var interval = JsonValues.ReadInt(root, "interval_min");
            var error = JsonValues.ReadString(root, "error");
            if (state is not null and not ("OK" or "FAILED"))
                return (null, [new KeyParseError(kv.Key, $"unknown export status state: {state}")]);
            return (new EtcdSnapshotExportInfo(state, uploaded, lastObject, sha, size, interval, error), []);
        }
        catch (System.Text.Json.JsonException ex)
        {
            return (null, [new KeyParseError(kv.Key, $"broken etcdworker snapshots status JSON: {ex.Message}")]);
        }
    }
}
