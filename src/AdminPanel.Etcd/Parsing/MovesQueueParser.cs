using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора очереди заявок /pgworker/moves/ (arch/02 §2.3.1).
public sealed record MovesParseResult(
    IReadOnlyList<MoveTicket> Tickets,
    IReadOnlyList<KeyParseError> Errors);

// Чистая функция: KV префикса /pgworker/moves/<C>/<bucket> → заявки. Битый JSON,
// неизвестный/отсутствующий op, неканонический ключ — KeyParseError (тик не роняют;
// ключ не трогаем — его отвергнет и удалит процесс PgWorker, arch/02 §7).
public static class MovesQueueParser
{
    public const string Prefix = "/pgworker/moves/";

    public static MovesParseResult Parse(IReadOnlyList<Kv> kvs)
    {
        var tickets = new List<MoveTicket>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            // "/pgworker/moves/<C>/<bucket>" → ["", "pgworker", "moves", <C>, <bucket>]
            var segments = kv.Key.Split('/');
            if (segments.Length != 5 || segments[3].Length == 0 || segments[4].Length == 0)
            {
                errors.Add(new(kv.Key, "ожидается /pgworker/moves/<cluster>/<bucket>"));
                continue;
            }

            var (cluster, leaf) = (segments[3], segments[4]);
            var bucketId = leaf.StartsWith("bucket_", StringComparison.Ordinal)
                           && int.TryParse(leaf["bucket_".Length..], out var id)
                ? id
                : (int?)null;
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object
                    || !root.TryGetProperty("op", out var op)
                    || op.ValueKind != JsonValueKind.String)
                {
                    errors.Add(new(kv.Key, "нет поля op"));
                    continue;
                }

                var opName = op.GetString()!;
                if (opName is not ("move" or "rollback" or "finalize" or "abort"))
                {
                    errors.Add(new(kv.Key, $"неизвестный op: '{opName}'"));
                    continue;
                }

                tickets.Add(new MoveTicket(
                    cluster, leaf, bucketId, opName,
                    GetString(root, "to"),
                    root.TryGetProperty("requested_unix", out var unix)
                        && unix.ValueKind == JsonValueKind.Number
                        ? unix.GetInt64()
                        : 0,
                    GetString(root, "requested_by")));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"битый JSON: {e.Message}"));
            }
        }

        return new(tickets, errors);
    }

    private static string? GetString(JsonElement root, string name)
        => root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;
}
