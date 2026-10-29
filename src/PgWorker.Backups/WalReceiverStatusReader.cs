using System.Text.Json;
using PgWorker.Etcd.Parsing;
using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.Backups;

/// <summary>Факты одного приёмника WAL из статус-ключа agent/&lt;N&gt; (
/// arch/19 §4): node/role/state + факты pool_exhausted/tli_jump/error. Роль и
/// состояние — строковые значения ключа ("master"/"sync"/"none",
/// "streaming"/...); парсинг DTO дублируется в Backups толерантно (Backups НЕ
/// ссылается на PgWorker.WalReceiver): битый JSON/не-объект/нет node или state —
/// запись пропускается (null), незнакомые поля игнорируются.</summary>
public sealed record WalReceiverAgentFacts(
    string Node, string? Role, string? State, bool PoolExhausted,
    WalGapState? TliJump, string? Error);

/// <summary>Читатель статус-ключей приёмников шарда (arch/19 §4):
/// Range /pgworker/backups/&lt;C&gt;/&lt;X&gt;/agent/ — failover-перебор
/// endpoints (образец WalStatusWriter.ReadAsync). Битые значения — пропуск
/// записи (толерантность к чужому/легаси-содержимому ключа).</summary>
public sealed class WalReceiverStatusReader(IEtcdGateway etcd, string[] endpoints)
{
    public async Task<Result<IReadOnlyList<WalReceiverAgentFacts>>> ReadAsync(
        string cluster, string shard, CancellationToken ct)
    {
        Result<IReadOnlyList<Kv>> last = default;
        foreach (var endpoint in endpoints)
        {
            var range = await etcd.RangeAsync(
                endpoint, $"/pgworker/backups/{cluster}/{shard}/agent/", ct);
            if (!range.IsSuccess)
            {
                last = range; // отказ — пробуем следующий endpoint (failover)
                continue;
            }

            return Result<IReadOnlyList<WalReceiverAgentFacts>>.Success(
                [.. range.Value.Select(kv => Parse(kv.Value)).OfType<WalReceiverAgentFacts>()]);
        }

        return Result<IReadOnlyList<WalReceiverAgentFacts>>.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    // Толерантный парсинг значения ключа: node/state обязательны; tli_jump —
    // непустые from/to + tli; pool_exhausted — только literal true.
    internal static WalReceiverAgentFacts? Parse(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return null;
            var node = root.TryGetProperty("node", out var n) && n.ValueKind == JsonValueKind.String
                ? n.GetString()
                : null;
            var state = root.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
                ? s.GetString()
                : null;
            if (string.IsNullOrEmpty(node) || string.IsNullOrEmpty(state))
                return null;
            var role = root.TryGetProperty("role", out var r) && r.ValueKind == JsonValueKind.String
                ? r.GetString()
                : null;
            var poolExhausted = root.TryGetProperty("pool_exhausted", out var p)
                && p.ValueKind == JsonValueKind.True;
            WalGapState? jump = null;
            if (root.TryGetProperty("tli_jump", out var j)
                && j.ValueKind == JsonValueKind.Object
                && j.TryGetProperty("from", out var from)
                && from.ValueKind == JsonValueKind.String
                && j.TryGetProperty("to", out var to)
                && to.ValueKind == JsonValueKind.String
                && j.TryGetProperty("tli", out var tliValue)
                && tliValue.TryGetUInt32(out var tli)
                && from.GetString() is { Length: > 0 } fromSeg
                && to.GetString() is { Length: > 0 } toSeg)
            {
                jump = new WalGapState(fromSeg, toSeg, tli);
            }

            var error = root.TryGetProperty("error", out var e) && e.ValueKind == JsonValueKind.String
                ? e.GetString()
                : null;
            return new WalReceiverAgentFacts(node!, role, state, poolExhausted, jump, error);
        }
        catch (JsonException)
        {
            return null; // битое/чужое значение — не факт
        }
    }
}
