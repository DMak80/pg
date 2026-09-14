using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups;

/// <summary>Сборка JSON статуса WAL-потока (формат arch/19 §4 — 1:1 с каркасной
/// моделью WalStreamState t01) + put etcd-ключа /pgworker/backups/<C>/<X>/wal
/// ТОЛЬКО при изменении (идемпотентность: безделье не пишет). Оба метода —
/// failover по endpoints (continue-on-failure, итог — последний отказ; образец
/// WorkJournal.WithFailoverAsync). Пишет PgWorker под клэймом <C>; панель читает.</summary>
public sealed class WalStatusWriter(IEtcdGateway etcd, string[] endpoints)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,

        // кириллица error-сообщений в ключе etcd — читаемо, без \uXXXX-эскейпов
        // (значение — статусы/ошибки нашего writer'а, не HTML-контекст).
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    public static string ToJson(WalStreamState state) => JsonSerializer.Serialize(
        new WalStatusPayload(
            StateName(state.State), state.Slot, state.MasterNode,
            state.ChainStartSegment, state.LastReceivedSegment,
            state.LastUploadedSegment, state.LastUploadedUnix,
            state.LagSegments, state.Error), Json);

    public async Task<Result<WalStreamState?>> ReadAsync(string cluster, string shard, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, Key(cluster, shard), ct);
            if (!result.IsSuccess)
            {
                last = result; // отказ — пробуем следующий endpoint (failover)
                continue;
            }

            if (result.Value is not { } kv)
                return Result<WalStreamState?>.Success(null);
            return Parse(cluster, shard, kv.Value);
        }

        return Result<WalStreamState?>.Failed(last?.Error
            ?? new ApplicationException("нет живых endpoints etcd"));
    }

    public async Task<Result> WriteIfChangedAsync(
        string cluster, string shard, WalStreamState state, CancellationToken ct)
    {
        var payload = ToJson(state);
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var current = await etcd.GetAsync(endpoint, Key(cluster, shard), ct);
            if (!current.IsSuccess)
            {
                last = current; // отказ — failover на следующий endpoint
                continue;
            }

            if (current.Value is { } kv && kv.Value == payload)
                return Result.Success(); // без изменений — не пишем (частые тики)

            var put = await etcd.PutAsync(endpoint, Key(cluster, shard), payload, lease: null, ct);
            if (!put.IsSuccess)
            {
                last = put; // отказ — failover на следующий endpoint
                continue;
            }

            return Result.Success();
        }

        return Result.Failed(last?.Error ?? new ApplicationException("нет живых endpoints etcd"));
    }

    private static Result<WalStreamState?> Parse(string cluster, string shard, string raw)
    {
        try
        {
            var payload = JsonSerializer.Deserialize<WalStatusPayload>(raw, Json);
            if (payload is null || payload.State is null || payload.LastUploadedUnix is null)
                return Result<WalStreamState?>.Failed(new ApplicationException(
                    $"битый ключ /pgworker/backups/{cluster}/{shard}/wal"));
            return Result<WalStreamState?>.Success(new WalStreamState(
                StateOf(payload.State), payload.Slot ?? "", payload.MasterNode ?? "",
                payload.ChainStartSegment ?? "", payload.LastReceivedSegment ?? "",
                payload.LastUploadedSegment ?? "", payload.LastUploadedUnix,
                payload.LagSegments, payload.Error));
        }
        catch (JsonException e)
        {
            return Result<WalStreamState?>.Failed(new ApplicationException(
                $"битый JSON /pgworker/backups/{cluster}/{shard}/wal: {e.Message}", e));
        }
    }

    private static string Key(string cluster, string shard) => $"/pgworker/backups/{cluster}/{shard}/wal";

    private static string StateName(WalStreamStatus state) => state switch
    {
        WalStreamStatus.Active => "ACTIVE",
        WalStreamStatus.Degraded => "DEGRADED",
        WalStreamStatus.Stopped => "STOPPED",
        WalStreamStatus.Broken => "BROKEN",
        _ => "ACTIVE",
    };

    private static WalStreamStatus StateOf(string name) => name switch
    {
        "DEGRADED" => WalStreamStatus.Degraded,
        "STOPPED" => WalStreamStatus.Stopped,
        "BROKEN" => WalStreamStatus.Broken,
        _ => WalStreamStatus.Active,
    };

    // snake_case-поля ключа (архивный формат §4; писать/читать — только через ToJson/Parse)
    private sealed record WalStatusPayload(
        [property: JsonPropertyName("state")] string? State,
        [property: JsonPropertyName("slot")] string? Slot,
        [property: JsonPropertyName("master_node")] string? MasterNode,
        [property: JsonPropertyName("chain_start_segment")] string? ChainStartSegment,
        [property: JsonPropertyName("last_received_segment")] string? LastReceivedSegment,
        [property: JsonPropertyName("last_uploaded_segment")] string? LastUploadedSegment,
        [property: JsonPropertyName("last_uploaded_unix")] long? LastUploadedUnix,
        [property: JsonPropertyName("lag_segments")] long? LagSegments,
        [property: JsonPropertyName("error")] string? Error);
}
