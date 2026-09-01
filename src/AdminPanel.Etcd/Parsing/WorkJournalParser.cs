using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

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

                items.Add(new WorkJournalInfo(
                    cluster,
                    String(root, "op") ?? "",
                    String(root, "phase") ?? "",
                    String(root, "instance") ?? "",
                    Long(root, "updated_unix") ?? 0,
                    String(root, "last_error"),
                    (int?)Long(root, "fail_count"),
                    Long(root, "fail_first_unix"),
                    Long(root, "retry_not_before_unix")));
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
}
