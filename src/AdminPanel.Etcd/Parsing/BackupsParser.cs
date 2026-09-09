using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

/// <summary>
/// Результат разбора префикса бэкапов: кластеры + ошибки (кормят key-malformed,
/// тик не роняют) — толерантный парсер по образцу WorkJournalParser.
/// </summary>
public sealed record BackupsParseResult(
    IReadOnlyList<ClusterBackupsInfo> Clusters,
    IReadOnlyList<KeyParseError> Errors);

/// <summary>
/// Чистая функция: KV префикса /pgworker/backups/ → статусы WAL-потоков шардов
/// (arch/19 §4, adminpanel/02 §2.3.1). Разбираются ТОЛЬКО ключи
/// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/wal; формат value — snake_case JSON воркера
/// (t01/t03): state/slot/master_node/last_uploaded_unix обязательны,
/// lag_segments/error опциональны. Битые значения → errors, не исключение.
/// </summary>
public static class BackupsParser
{
    public static BackupsParseResult Parse(IReadOnlyList<Kv> kvs)
    {
        var shards = new Dictionary<string, Dictionary<string, WalStreamInfo?>>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            var segments = kv.Key.Split('/');
            // /pgworker/backups/<C>/<X>/wal — только wal-статусы (full/policy — t02+)
            if (segments.Length != 6 || segments[5] != "wal")
                continue;
            var cluster = segments[3];
            var shard = segments[4];
            if (cluster.Length == 0 || shard.Length == 0)
            {
                errors.Add(new(kv.Key, "пустое имя кластера/шарда в ключе"));
                continue;
            }

            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object)
                {
                    errors.Add(new(kv.Key, "значение не JSON-объект"));
                    continue;
                }

                var state = StateOf(String(root, "state"));
                var unix = Long(root, "last_uploaded_unix");
                if (state is null || unix is null)
                {
                    // незнакомое state/нет обязательного поля — запись не разбирается
                    errors.Add(new(kv.Key, "битый wal-статус (state/last_uploaded_unix)"));
                    continue;
                }

                if (!shards.TryGetValue(cluster, out var byShard))
                    shards[cluster] = byShard = [];
                byShard[shard] = new WalStreamInfo(
                    cluster, shard, state.Value,
                    String(root, "slot") ?? "",
                    String(root, "master_node") ?? "",
                    unix.Value,
                    Long(root, "lag_segments"),
                    String(root, "error"));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, "битый JSON: " + e.Message));
            }
        }

        return new BackupsParseResult(
            shards.Select(p => new ClusterBackupsInfo(p.Key, p.Value)).ToList(),
            errors);
    }

    private static WalStreamInfoState? StateOf(string? raw) => raw switch
    {
        "ACTIVE" => WalStreamInfoState.Active,
        "DEGRADED" => WalStreamInfoState.Degraded,
        "STOPPED" => WalStreamInfoState.Stopped,
        _ => null,
    };

    private static string? String(JsonElement root, string name)
        => root.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String
            ? v.GetString()
            : null;

    private static long? Long(JsonElement root, string name)
        => root.TryGetProperty(name, out var v)
           && (v.ValueKind == JsonValueKind.Number)
           && v.TryGetInt64(out var parsed)
            ? parsed
            : null;
}
