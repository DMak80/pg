using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Общая логика постановки заявок переездов (t07, arch/02 §9.7 п.3–5): чтение
// очереди напрямую у etcd, база requested_unix, txn-клэйм per key. Портировано
// из MoveBucketsHandler без изменения поведения (регресс — MovesApiTests).
internal static class MoveTickets
{
    // Живая заявка нашего кластера: поля для проверки идентичности (§9.7 п.3).
    internal sealed record Existing(string Op, string? To, string? OldShard, bool Force);

    // Снимок очереди: заявки кластера по leaf'ам + глобальный max requested_unix.
    internal sealed record Queue(IReadOnlyDictionary<string, Existing> Mine, long MaxUnix);

    // Канон тела заявки (arch/14 §3.3, snake_case): только заполненные поля
    // пишутся в JSON (WhenWritingNull; force:true пишется, false — опускается).
    internal sealed record TicketBody(
        [property: JsonPropertyName("op")] string Op,
        [property: JsonPropertyName("to")] string? To,
        [property: JsonPropertyName("old_shard")] string? OldShard,
        [property: JsonPropertyName("force")] bool? Force,
        [property: JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: JsonPropertyName("requested_by")] string RequestedBy);

    internal static readonly JsonSerializerOptions TicketJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    // Чтение префикса /pgworker/moves/ одним range (§9.7 п.3): заявки кластера
    // + глобальный max requested_unix (база упорядочивания, п.4). Битый JSON
    // скипаем — его отвергнет и удалит процесс переездов (arch/02 §7).
    public static async Task<Result<Queue>> ReadQueueAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, "/pgworker/moves/", ct));
        if (!range.IsSuccess)
            return Result<Queue>.Failed(range.Error!);

        var mine = new Dictionary<string, Existing>();
        long maxUnix = 0;
        foreach (var kv in range.Value)
        {
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                if (!root.TryGetProperty("op", out var op) || op.ValueKind != JsonValueKind.String)
                    continue; // заявка без op — не наша
                if (root.TryGetProperty("requested_unix", out var unix)
                    && unix.ValueKind == JsonValueKind.Number)
                    maxUnix = Math.Max(maxUnix, unix.GetInt64());

                var segments = kv.Key.Split('/');
                if (segments.Length != 5 || segments[3] != cluster || segments[4].Length == 0)
                    continue;
                string? ReadString(string name) => root.TryGetProperty(name, out var el)
                    && el.ValueKind == JsonValueKind.String
                    ? el.GetString()
                    : null;
                var force = root.TryGetProperty("force", out var f) && f.ValueKind == JsonValueKind.True;
                mine[segments[4]] = new Existing(op.GetString()!, ReadString("to"), ReadString("old_shard"), force);
            }
            catch (JsonException)
            {
                // битая заявка не участвует ни в идентичности, ни в базе
            }
        }

        return Result<Queue>.Success(new Queue(mine, maxUnix));
    }

    // Txn-клэйм per key (§9.7 п.5): compare NotExists + put — защита от
    // перезаписи чужой заявки между чтением и записью.
    public static Task<Result<TxnResult>> ClaimAsync(
        IEtcdGateway gateway, string[] endpoints, string key, string json, CancellationToken ct)
        => EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(endpoint,
            TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, json, null)]), ct));
}
