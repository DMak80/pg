using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.Core.Templates;

/// <summary>Читатель статусного ключа агента PgNode (arch/14 §3.3; дубль
/// контракта агента — запрет цикла ссылок, прецедент ApiContracts):
/// воркер мониторит poll-тиком, панелью не разбирается.</summary>
public sealed record PgNodeStatusFact(
    string State,
    long? AppliedRevision,
    IReadOnlyDictionary<string, string>? Components,
    long UpdatedUnix,
    string? Error = null)
{
    public static string StatusKey(string cluster, string shard, string node)
        => $"/pgworker/nodes/{cluster}/{shard}/{node}/status";

    /// <summary>null — значение не парсится (чужой/битый ключ = статуса нет).</summary>
    public static PgNodeStatusFact? Parse(string json)
    {
        try
        {
            var dto = JsonSerializer.Deserialize<Dto>(json);
            return dto is { State: { Length: > 0 } }
                ? new PgNodeStatusFact(dto.State, dto.AppliedRevision,
                    dto.Components, dto.UpdatedUnix ?? 0, dto.Error)
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private sealed record Dto(
        [property: JsonPropertyName("state")] string? State,
        [property: JsonPropertyName("applied_revision")] long? AppliedRevision,
        [property: JsonPropertyName("components")] IReadOnlyDictionary<string, string>? Components,
        [property: JsonPropertyName("updated_unix")] long? UpdatedUnix,
        [property: JsonPropertyName("error")] string? Error);
}
