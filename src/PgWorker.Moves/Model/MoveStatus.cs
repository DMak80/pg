using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core;

namespace PgWorker.Moves;

/// <summary>
/// Статус переезда бакета — значение ключа /clusters/&lt;C&gt;/buckets/status/bucket_&lt;i&gt;.
/// Формат 1:1 со скриптами move-bucket.sh/abort-move.sh (spec §4.2, Д6):
/// двусторонняя совместимость (след C#-переезда разбирает скрипт и наоборот).
/// Нет ключа = бакет ACTIVE.
/// </summary>
public sealed record MoveStatus(
    [property: JsonPropertyName("bucket")] string Bucket,
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("owner")] string Owner,
    [property: JsonPropertyName("target")] string Target,
    [property: JsonPropertyName("started_unix")] long StartedUnix,
    [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
    [property: JsonPropertyName("phase")] string Phase)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public string Serialize() => JsonSerializer.Serialize(this, Json);

    /// <summary>Толерантный парсинг (образец — WorkJournal.ReadAsync): JsonException → Failed.</summary>
    public static Result<MoveStatus> Parse(string raw)
    {
        try
        {
            var status = JsonSerializer.Deserialize<MoveStatus>(raw, Json);
            return status is null
                ? Result<MoveStatus>.Failed(new ApplicationException("пустой статус-ключ"))
                : status;
        }
        catch (JsonException e)
        {
            return Result<MoveStatus>.Failed(new ApplicationException(
                $"битый статус-ключ переезда: {e.Message}", e));
        }
    }
}

/// <summary>
/// Элемент плана abort-уборки: «шард|тип|имя» (тип sub|slot|pub|schema) —
/// строка 1:1 с abort-move.sh (jq -Rn '[inputs]' по ARTIFACTS).
/// </summary>
[JsonConverter(typeof(AbortPlanItemConverter))]
public sealed record AbortPlanItem(string Shard, string Kind, string Name);

/// <summary>Конвертер «shard|kind|name» ↔ AbortPlanItem (формат скрипта).</summary>
public sealed class AbortPlanItemConverter : JsonConverter<AbortPlanItem>
{
    public override AbortPlanItem Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var raw = reader.GetString()
                  ?? throw new JsonException("элемент плана уборки — не строка");
        var parts = raw.Split('|');
        if (parts.Length != 3)
            throw new JsonException($"битый элемент плана уборки (ожидался 'шард|тип|имя'): '{raw}'");

        return new AbortPlanItem(parts[0], parts[1], parts[2]);
    }

    public override void Write(Utf8JsonWriter writer, AbortPlanItem value, JsonSerializerOptions options)
        => writer.WriteStringValue($"{value.Shard}|{value.Kind}|{value.Name}");
}

/// <summary>
/// Журнал abort-уборки — тот же статус-ключ с state=ABORTING (spec §6.5):
/// пишется ДО манипуляций с БД (journal-before-manipulations, P7); крах уборки
/// оставляет самодокументирующийся след, takeover продолжает с записанной фазы.
/// </summary>
public sealed record AbortJournal(
    [property: JsonPropertyName("bucket")] string Bucket,
    [property: JsonPropertyName("prev_state")] string PrevState,
    [property: JsonPropertyName("owner")] string Owner,
    [property: JsonPropertyName("target")] string Target,
    [property: JsonPropertyName("started_unix")] long StartedUnix,
    [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
    [property: JsonPropertyName("phase")] string Phase,
    [property: JsonPropertyName("last_error")] string? LastError,
    [property: JsonPropertyName("plan")] IReadOnlyList<AbortPlanItem> Plan,
    [property: JsonPropertyName("unreachable_shards")] IReadOnlyList<string> UnreachableShards)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>Сериализация с константой state=ABORTING (эквивалент journal_set скрипта).</summary>
    public string Serialize() => JsonSerializer.Serialize(new AbortJournalPayload(
        Bucket, MoveStates.Aborting, PrevState, Owner, Target,
        StartedUnix, UpdatedUnix, Phase, LastError, Plan, UnreachableShards), Json);

    /// <summary>Толерантный парсинг журнала уборки (state в payload игнорируется — всегда ABORTING).</summary>
    public static Result<AbortJournal> Parse(string raw)
    {
        try
        {
            var payload = JsonSerializer.Deserialize<AbortJournalPayload>(raw, Json);
            return payload is null
                ? Result<AbortJournal>.Failed(new ApplicationException("пустой журнал уборки"))
                : new AbortJournal(payload.Bucket, payload.PrevState, payload.Owner, payload.Target,
                    payload.StartedUnix, payload.UpdatedUnix, payload.Phase, payload.LastError,
                    payload.Plan, payload.UnreachableShards);
        }
        catch (JsonException e)
        {
            return Result<AbortJournal>.Failed(new ApplicationException(
                $"битый журнал уборки: {e.Message}", e));
        }
    }

    // Форма на проводе: добавляется state=ABORTING (пишется константой).
    private sealed record AbortJournalPayload(
        [property: JsonPropertyName("bucket")] string Bucket,
        [property: JsonPropertyName("state")] string State,
        [property: JsonPropertyName("prev_state")] string PrevState,
        [property: JsonPropertyName("owner")] string Owner,
        [property: JsonPropertyName("target")] string Target,
        [property: JsonPropertyName("started_unix")] long StartedUnix,
        [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
        [property: JsonPropertyName("phase")] string Phase,
        [property: JsonPropertyName("last_error")] string? LastError,
        [property: JsonPropertyName("plan")] IReadOnlyList<AbortPlanItem> Plan,
        [property: JsonPropertyName("unreachable_shards")] IReadOnlyList<string> UnreachableShards);
}
