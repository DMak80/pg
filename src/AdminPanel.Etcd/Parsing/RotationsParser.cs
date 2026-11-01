using System.Text.Json;
using AdminPanel.Core;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора живых заявок ротаций pg: записи + ошибки разбора
// (кормят key-malformed, тик не роняют).
public sealed record PgRotationsParseResult(
    IReadOnlyList<PgRotationTicket> Tickets,
    IReadOnlyList<KeyParseError> Errors);

// Разбор range /pgworker/rotations/ (u07; ключ /pgworker/rotations/<C>, формат
// §9.8: {"requested_unix","requested_by"?}) → PgRotationTicket. Толерантный
// парс: битый JSON / нет requested_unix → parseError-запись (домен воркера
// не трогаем; Cluster = лист ключа).
public static class RotationsParser
{
    public static PgRotationsParseResult Parse(IReadOnlyList<Kv> kvs)
    {
        var tickets = new List<PgRotationTicket>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            // /pgworker/rotations/<C> → лист ключа — имя кластера.
            var cluster = kv.Key[(kv.Key.LastIndexOf('/') + 1)..];
            if (cluster.Length == 0)
            {
                errors.Add(new(kv.Key, "empty cluster name in rotations key"));
                continue;
            }

            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object)
                {
                    errors.Add(new(kv.Key, "rotation value is not a JSON object"));
                    continue;
                }

                var requested = root.TryGetProperty("requested_unix", out var unix)
                                && unix.ValueKind == JsonValueKind.Number
                                && unix.TryGetInt64(out var value)
                    ? value
                    : (long?)null;
                if (requested is null)
                {
                    errors.Add(new(kv.Key, "broken rotation value (requested_unix required)"));
                    continue;
                }

                var by = root.TryGetProperty("requested_by", out var byEl)
                         && byEl.ValueKind == JsonValueKind.String
                    ? byEl.GetString()
                    : null;
                tickets.Add(new PgRotationTicket(cluster, requested.Value, by));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"broken rotation JSON: {e.Message}"));
            }
        }

        return new(
            tickets.OrderBy(t => t.Cluster, StringComparer.Ordinal).ToList(),
            errors);
    }
}
