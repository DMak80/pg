using System.Text.Json;

namespace PgWorker.Etcd.Backups;

// Значение ключа /pgworker/backups/<C>/<X>/wal-life (arch/backups/05 §1):
// {"chain_life":N,"bumped_unix":T,"reason":"shard-rebuilt|restore|foreign-chain"}.
// Отсутствие ключа = жизнь 0 (запись не создаётся). Пишет ТОЛЬКО воркер.
// Ключи JSON — snake_case вручную (паттерн BackupStatusJson/RestoreStatusJson:
// JsonSerializerDefaults.Web даёт camelCase имён свойств, контракт etcd —
// snake_case; панельный и воркерский парсеры читают одни и те же ключи).
public enum WalLifeReason { ShardRebuilt, Restore, ForeignChain }

public sealed record WalLifeState(uint ChainLife, long? BumpedUnix, WalLifeReason? Reason);

public static class WalLifeStateJson
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public static string Serialize(WalLifeState state)
    {
        var o = new Dictionary<string, object?>
        {
            ["chain_life"] = state.ChainLife,
        };
        if (state.BumpedUnix is { } bumped)
            o["bumped_unix"] = bumped;
        if (ReasonName(state.Reason) is { } reason)
            o["reason"] = reason;
        return JsonSerializer.Serialize(o, Json);
    }

    public static WalLifeState? TryParse(string? json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json ?? string.Empty);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("chain_life", out var lifeEl)
                || !lifeEl.TryGetInt64(out var life)
                || life is <= 0 or > uint.MaxValue)
                return null;
            long? bumpedUnix = root.TryGetProperty("bumped_unix", out var bEl)
                && bEl.ValueKind == JsonValueKind.Number
                && bEl.TryGetInt64(out var bumped)
                ? bumped
                : null;
            WalLifeReason? reason = root.TryGetProperty("reason", out var rEl)
                && rEl.ValueKind == JsonValueKind.String
                ? ReasonOf(rEl.GetString())
                : null;
            return new WalLifeState((uint)life, bumpedUnix, reason);
        }
        catch
        {
            return null; // битый JSON — толерантный читатель
        }
    }

    private static string? ReasonName(WalLifeReason? reason) => reason switch
    {
        WalLifeReason.ShardRebuilt => "shard-rebuilt",
        WalLifeReason.Restore => "restore",
        WalLifeReason.ForeignChain => "foreign-chain",
        _ => null,
    };

    private static WalLifeReason? ReasonOf(string? name) => name switch
    {
        "shard-rebuilt" => WalLifeReason.ShardRebuilt,
        "restore" => WalLifeReason.Restore,
        "foreign-chain" => WalLifeReason.ForeignChain,
        _ => null,
    };
}
