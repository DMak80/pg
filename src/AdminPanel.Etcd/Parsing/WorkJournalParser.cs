using System.Text.Json;
using AdminPanel.Core;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

/// <summary>
/// Результат разбора журналов процессов: записи + ошибки разбора (кормят
/// key-malformed, тик не роняют). Позиционный record — деконструкция
/// как у tuple.
/// </summary>
public sealed record WorkJournalParseResult(
    IReadOnlyList<WorkJournalInfo> Items,
    IReadOnlyList<KeyParseError> Errors);

/// <summary>
/// Чистая функция: KV префикса /pgworker/work/ → журналы процессов кластеров
/// (arch/adminpanel/02 §2.3.1). Формат value (arch/14 §3.3):
/// {"op","phase","instance","updated_unix","last_error", серия ретраев
/// "fail_count"/"fail_first_unix"/"retry_not_before_unix" — optional}.
/// Битый JSON → KeyParseError (толерантность, тик не роняют); Cluster = лист ключа.
/// </summary>
public static class WorkJournalParser
{
    public static WorkJournalParseResult Parse(IReadOnlyList<Kv> kvs)
    {
        var items = new List<WorkJournalInfo>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            var cluster = kv.Key[(kv.Key.LastIndexOf('/') + 1)..];
            if (cluster.Length == 0)
            {
                errors.Add(new(kv.Key, "пустое имя кластера в ключе"));
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

                if (!TryFact(root, "last_failover", kv.Key, out var failover, errors)
                    || !TryFact(root, "last_rebuild", kv.Key, out var rebuild, errors))
                    continue;

                items.Add(new WorkJournalInfo(
                    cluster,
                    String(root, "op") ?? "",
                    String(root, "phase") ?? "",
                    String(root, "instance") ?? "",
                    Long(root, "updated_unix") ?? 0,
                    String(root, "last_error"),
                    (int?)Long(root, "fail_count"),
                    Long(root, "fail_first_unix"),
                    Long(root, "retry_not_before_unix"),
                    failover,
                    rebuild));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"битый JSON: {e.Message}"));
            }
        }

        return new(items, errors);
    }

    private static string? String(JsonElement root, string name)
        => root.TryGetProperty(name, out var el) && el.ValueKind == JsonValueKind.String
            ? el.GetString()
            : null;

    private static long? Long(JsonElement root, string name)
        => root.TryGetProperty(name, out var el) && el.ValueKind == JsonValueKind.Number
            ? el.GetInt64()
            : null;

    // Факт-поле (last_failover/last_rebuild): отсутствует/null → факт null (старый
    // ключ); битое (не объект / нет обязательных shard/node/cause/detected_unix) —
    // false + parseError-запись (толерантность: тик не роняют, ключ не трогаем).
    private static bool TryFact(
        JsonElement root, string name, string key,
        out HaSupervisionInfo? fact, List<KeyParseError> errors)
    {
        fact = null;
        if (!root.TryGetProperty(name, out var el) || el.ValueKind == JsonValueKind.Null)
            return true;
        if (el.ValueKind != JsonValueKind.Object
            || !el.TryGetProperty("shard", out var shardEl) || shardEl.ValueKind != JsonValueKind.String
            || !el.TryGetProperty("node", out var nodeEl) || nodeEl.ValueKind != JsonValueKind.String
            || !el.TryGetProperty("cause", out var causeEl) || causeEl.ValueKind != JsonValueKind.String
            || !el.TryGetProperty("detected_unix", out var detectedEl) || detectedEl.ValueKind != JsonValueKind.Number)
        {
            errors.Add(new(key, $"поле {name} битое: ожидается объект shard/node/cause/detected_unix"));
            return false;
        }

        fact = new HaSupervisionInfo(
            shardEl.GetString()!, nodeEl.GetString()!, causeEl.GetString()!,
            detectedEl.GetInt64(),
            el.TryGetProperty("resolved_unix", out var resolved) && resolved.ValueKind == JsonValueKind.Number
                ? resolved.GetInt64() : null,
            el.TryGetProperty("duration_sec", out var duration) && duration.ValueKind == JsonValueKind.Number
                ? duration.GetInt64() : null);
        return true;
    }
}
