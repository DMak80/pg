using System.Text.Json;
using AdminPanel.Core;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Точечный разбор ключа /pgworker/backups-disabled (u10; канон arch/19 §4):
// значение {"updated_unix","updated_by"?} → BackupsDisabledInfo. Ключа нет —
// null (бэкапы работают); битый JSON / не объект / нет updated_unix —
// KeyParseError (толерантный читатель, ключ не трогаем).
// Ключ НЕ входит в range /pgworker/backups/ (байтовое сравнение: '-' < '/'),
// читается отдельным точечным range-запросом pg-тика.
public static class BackupsDisabledParser
{
    public static (BackupsDisabledInfo? Info, KeyParseError? Error) Parse(Kv? kv)
    {
        if (kv is null)
            return (null, null);

        try
        {
            using var doc = JsonDocument.Parse(kv.Value);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return (null, new KeyParseError(kv.Key, "backups-disabled value is not a JSON object"));

            var updated = root.TryGetProperty("updated_unix", out var unix)
                          && unix.ValueKind == JsonValueKind.Number
                          && unix.TryGetInt64(out var value)
                ? value
                : (long?)null;
            if (updated is null)
                return (null, new KeyParseError(kv.Key, "broken backups-disabled value (updated_unix required)"));

            var by = root.TryGetProperty("updated_by", out var byEl)
                     && byEl.ValueKind == JsonValueKind.String
                ? byEl.GetString()
                : null;
            return (new BackupsDisabledInfo(updated.Value, by), null);
        }
        catch (JsonException e)
        {
            return (null, new KeyParseError(kv.Key, $"broken backups-disabled JSON: {e.Message}"));
        }
    }
}
