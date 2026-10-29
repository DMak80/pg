using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.WalReceiver;

/// <summary>Роль источника приёмника (REST-проба своей ноды, arch/19 §3).</summary>
public enum WalReceiverRole { Master, Sync, None }

/// <summary>Состояние приёмника (arch/19 §4): waiting_* — ожидание условий,
/// paused — стоп-семантика, failed_permanent — несъёмное условие (причина в Error).</summary>
public enum WalReceiverState
{
    Starting, WaitingRole, WaitingPassword, WaitingPolicy, WaitingSlot,
    WaitingDisabled, Streaming, Paused, FailedPermanent,
}

/// <summary>Факт tli-прыжка приёмника (58P01): сегменты (From .. To] не будут
/// доставлены; живёт в статус-ключе до следующего прыжка.</summary>
public sealed record WalReceiverTliJump(string From, string To, uint Tli);

public sealed record WalReceiverAgentStatus(
    WalReceiverRole Role,
    WalReceiverState State,
    string? LastDeliveredSegment,
    long? LastDeliveredUnix,
    bool PoolExhausted,
    WalReceiverTliJump? TliJump,
    uint? HistoryMissingTli,
    string? Error,
    long UpdatedUnix);

public static class WalReceiverAgentStatusJson
{
    private static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>Значение статус-ключа: {"node","role","state",...} — arch/19 §4.</summary>
    public static string ToJson(string node, WalReceiverAgentStatus status) => JsonSerializer.Serialize(new
    {
        node,
        role = RoleName(status.Role),
        state = StateName(status.State),
        last_delivered_segment = status.LastDeliveredSegment,
        last_delivered_unix = status.LastDeliveredUnix,
        pool_exhausted = status.PoolExhausted,
        tli_jump = status.TliJump is { } jump
            ? new { from = jump.From, to = jump.To, tli = jump.Tli }
            : null,
        history_missing_tli = status.HistoryMissingTli,
        error = status.Error,
        updated_unix = status.UpdatedUnix,
    }, Options);

    /// <summary>null — значение не парсится (чужое/битое).</summary>
    public static (string Node, WalReceiverAgentStatus Status)? Parse(string json)
    {
        try
        {
            var dto = JsonSerializer.Deserialize<Dto>(json, Options);
            if (dto is null || dto.Node is null || dto.State is null)
                return null;
            return (dto.Node, new WalReceiverAgentStatus(
                RoleOf(dto.Role), StateOf(dto.State),
                dto.LastDeliveredSegment, dto.LastDeliveredUnix,
                dto.PoolExhausted ?? false,
                dto.TliJump is { From: { Length: > 0 }, To: { Length: > 0 }, Tli: not null } t
                    ? new WalReceiverTliJump(t.From, t.To, t.Tli.Value) : null,
                dto.HistoryMissingTli,
                dto.Error, dto.UpdatedUnix ?? 0));
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string RoleName(WalReceiverRole role) => role switch
    {
        WalReceiverRole.Master => "master",
        WalReceiverRole.Sync => "sync",
        _ => "none",
    };

    private static WalReceiverRole RoleOf(string? name) => name switch
    {
        "master" => WalReceiverRole.Master,
        "sync" => WalReceiverRole.Sync,
        _ => WalReceiverRole.None,
    };

    private static string StateName(WalReceiverState state) => state switch
    {
        WalReceiverState.Starting => "starting",
        WalReceiverState.WaitingRole => "waiting_role",
        WalReceiverState.WaitingPassword => "waiting_password",
        WalReceiverState.WaitingPolicy => "waiting_policy",
        WalReceiverState.WaitingSlot => "waiting_slot",
        WalReceiverState.WaitingDisabled => "waiting_disabled",
        WalReceiverState.Streaming => "streaming",
        WalReceiverState.Paused => "paused",
        _ => "failed_permanent",
    };

    private static WalReceiverState StateOf(string name) => name switch
    {
        "starting" => WalReceiverState.Starting,
        "waiting_role" => WalReceiverState.WaitingRole,
        "waiting_password" => WalReceiverState.WaitingPassword,
        "waiting_policy" => WalReceiverState.WaitingPolicy,
        "waiting_slot" => WalReceiverState.WaitingSlot,
        "waiting_disabled" => WalReceiverState.WaitingDisabled,
        "streaming" => WalReceiverState.Streaming,
        "paused" => WalReceiverState.Paused,
        _ => WalReceiverState.FailedPermanent,
    };

    private sealed record Dto(
        [property: JsonPropertyName("node")] string? Node,
        [property: JsonPropertyName("role")] string? Role,
        [property: JsonPropertyName("state")] string? State,
        [property: JsonPropertyName("last_delivered_segment")] string? LastDeliveredSegment,
        [property: JsonPropertyName("last_delivered_unix")] long? LastDeliveredUnix,
        [property: JsonPropertyName("pool_exhausted")] bool? PoolExhausted,
        [property: JsonPropertyName("tli_jump")] TliJumpDto? TliJump,
        [property: JsonPropertyName("history_missing_tli")] uint? HistoryMissingTli,
        [property: JsonPropertyName("error")] string? Error,
        [property: JsonPropertyName("updated_unix")] long? UpdatedUnix);

    private sealed record TliJumpDto(
        [property: JsonPropertyName("from")] string? From,
        [property: JsonPropertyName("to")] string? To,
        [property: JsonPropertyName("tli")] uint? Tli);
}
