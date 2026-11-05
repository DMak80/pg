using System.Text.Json;
using AdminPanel.Core;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора эвакуаций: записи + ошибки разбора (кормят key-malformed,
// тик не роняют).
public sealed record EvacuationsParseResult(
    IReadOnlyList<EvacuationInfo> Items,
    IReadOnlyList<KeyParseError> Errors);

// Разбор range /pgworker/evacuations/ (u12; канон arch/pgworker/00-etcd-contract.md §3/§5 D): ключ
// /pgworker/evacuations/<C>/<X>, значение
// {"evacuated_unix","reason","buckets":{старый→новый},"state":"DONE|QUARANTINED",
// "returned_unix"?}. Толерантный парс: битый JSON / не объект / нет
// обязательных полей → parseError-запись, ключ не трогаем (домен воркера).
public static class EvacuationsParser
{
    public static EvacuationsParseResult Parse(IReadOnlyList<Kv> kvs)
    {
        var items = new List<EvacuationInfo>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            // /pgworker/evacuations/<C>/<X> → segments[3]=<C>, segments[4]=<X>.
            var segments = kv.Key.Split('/');
            if (segments.Length != 5 || segments[1] != "pgworker" || segments[2] != "evacuations"
                || segments[3].Length == 0 || segments[4].Length == 0)
                continue; // чужой/неканонический ключ — не наш домен

            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object)
                {
                    errors.Add(new(kv.Key, "evacuation value is not a JSON object"));
                    continue;
                }

                var evacuated = Long(root, "evacuated_unix");
                var reason = String(root, "reason");
                var state = String(root, "state");
                if (evacuated is null || reason is null || state is not ("DONE" or "QUARANTINED"))
                {
                    errors.Add(new(kv.Key,
                        "broken evacuation value (evacuated_unix/reason/state DONE|QUARANTINED required)"));
                    continue;
                }

                // buckets — объект «старый шард → новый шард»; отсутствует/битый
                // → parseError (план эвакуации без бакетной раскладки неполон).
                if (!root.TryGetProperty("buckets", out var bucketsEl)
                    || bucketsEl.ValueKind != JsonValueKind.Object)
                {
                    errors.Add(new(kv.Key, "broken evacuation value (buckets object required)"));
                    continue;
                }

                var buckets = new Dictionary<string, string>(StringComparer.Ordinal);
                var bucketsBroken = false;
                foreach (var b in bucketsEl.EnumerateObject())
                {
                    if (b.Value.ValueKind != JsonValueKind.String)
                    {
                        bucketsBroken = true;
                        break;
                    }

                    buckets[b.Name] = b.Value.GetString()!;
                }

                if (bucketsBroken)
                {
                    errors.Add(new(kv.Key, "broken evacuation buckets (string → string map)"));
                    continue;
                }

                items.Add(new EvacuationInfo(
                    segments[3], segments[4], evacuated.Value, reason,
                    buckets, state, Long(root, "returned_unix")));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"broken evacuation JSON: {e.Message}"));
            }
        }

        return new(
            items.OrderBy(e => e.Cluster, StringComparer.Ordinal)
                .ThenBy(e => e.Shard, StringComparer.Ordinal)
                .ToList(),
            errors);
    }

    private static string? String(JsonElement root, string name)
        => root.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String
            ? v.GetString()
            : null;

    private static long? Long(JsonElement root, string name)
        => root.TryGetProperty(name, out var v)
           && v.ValueKind == JsonValueKind.Number
           && v.TryGetInt64(out var parsed)
            ? parsed
            : null;
}
