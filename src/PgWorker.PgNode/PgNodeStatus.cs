using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.PgNode;

/// <summary>Значение статусного ключа /pgworker/nodes/&lt;C&gt;/&lt;X&gt;/&lt;N&gt;/status
/// (arch/14 §3.3, spec §3.6): машина агента — not_initialized → initializing
/// → ready | start_failed; applied_revision — mod_revision применённого
/// bootstrap-ключа (null до применения); components pg/doorman:
/// starting|ready|failed|stopped (+prepared — tool-путь). Пишет ТОЛЬКО агент
/// (lease TTL 15 c); панелью не разбирается.</summary>
public sealed record PgNodeStatus(
    string State,
    long? AppliedRevision,
    IReadOnlyDictionary<string, string> Components,
    long UpdatedUnix,
    string? Error = null)
{
    public const string NotInitialized = "not_initialized";
    public const string Initializing = "initializing";
    public const string Ready = "ready";
    public const string StartFailed = "start_failed";

    public static readonly IReadOnlyList<string> ComponentStates =
        ["starting", "ready", "failed", "stopped", "prepared"];

    public string ToJson() => JsonSerializer.Serialize(new Dto(
        State, AppliedRevision, Components, UpdatedUnix, Error), Options);

    private static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private sealed record Dto(
        [property: JsonPropertyName("state")] string State,
        [property: JsonPropertyName("applied_revision")] long? AppliedRevision,
        [property: JsonPropertyName("components")] IReadOnlyDictionary<string, string> Components,
        [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
        [property: JsonPropertyName("error")] string? Error);
}
