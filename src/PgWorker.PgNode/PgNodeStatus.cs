using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.PgNode;

/// <summary>Значение статусного ключа /pgworker/nodes/&lt;C&gt;/&lt;X&gt;/&lt;N&gt;/status
/// (arch/pgworker/00-etcd-contract.md §4, spec §3.6): машина агента — not_initialized → initializing
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

    /// <summary>Парсинг значения статусного ключа (MR-#11: txn-переход сессии
    /// обновляет in-memory _current разобранным значением). null — не парсится.</summary>
    public static PgNodeStatus? FromJson(string json)
    {
        try
        {
            var dto = JsonSerializer.Deserialize<Dto>(json, Options);
            return dto is null
                ? null
                : new PgNodeStatus(dto.State, dto.AppliedRevision,
                    dto.Components ?? new Dictionary<string, string>(),
                    dto.UpdatedUnix, dto.Error);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private sealed record Dto(
        [property: JsonPropertyName("state")] string State,
        [property: JsonPropertyName("applied_revision")] long? AppliedRevision,
        [property: JsonPropertyName("components")] IReadOnlyDictionary<string, string>? Components,
        [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
        [property: JsonPropertyName("error")] string? Error);
}
