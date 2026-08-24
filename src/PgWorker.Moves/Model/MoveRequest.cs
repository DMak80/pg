using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core;

namespace PgWorker.Moves;

/// <summary>Операция заявки на переезд (spec §4.1): строки как в скриптах.</summary>
public enum MoveOp
{
    Move,
    Rollback,
    Finalize,
    Abort,
}

/// <summary>
/// Заявка оператора на переезд/откат/уборку/отмену бакета — значение ключа
/// /pgworker/moves/&lt;C&gt;/bucket_&lt;i&gt; (spec §4.1, arch/14 §3.3).
/// Успех или перманентный отказ — заявку удаляет процесс; transient — живёт до успеха.
/// </summary>
public sealed record MoveRequest(
    string Bucket,
    MoveOp Op,
    string? To,
    [property: JsonPropertyName("old_shard")] string? OldShard,
    [property: JsonPropertyName("skip_reverse")] bool SkipReverse,
    [property: JsonPropertyName("resume")] bool Resume,
    [property: JsonPropertyName("force")] bool Force,
    [property: JsonPropertyName("requested_unix")] long RequestedUnix,
    [property: JsonPropertyName("requested_by")] string? RequestedBy)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>
    /// Толерантный парсинг тела заявки (образец — WorkJournal.ReadAsync):
    /// JsonException/неизвестный op → Result.Failed (заявка будет отвергнута процессом).
    /// </summary>
    public static Result<MoveRequest> Parse(string bucket, string raw)
    {
        try
        {
            var dto = JsonSerializer.Deserialize<MoveRequestDto>(raw, Json);
            if (dto?.Op is not { } op || !TryParseOp(op, out var parsed))
                return Result<MoveRequest>.Failed(new ApplicationException(
                    $"неизвестная операция заявки для {bucket}: '{dto?.Op}'"));

            return new MoveRequest(bucket, parsed, dto.To, dto.OldShard,
                dto.SkipReverse, dto.Resume, dto.Force, dto.RequestedUnix, dto.RequestedBy);
        }
        catch (JsonException e)
        {
            return Result<MoveRequest>.Failed(new ApplicationException(
                $"битая заявка /pgworker/moves/*/ {bucket}: {e.Message}", e));
        }
    }

    public string Serialize() => JsonSerializer.Serialize(new MoveRequestDto(
        OpToString(Op), To, OldShard, SkipReverse, Resume, Force, RequestedUnix, RequestedBy), Json);

    internal static bool TryParseOp(string op, out MoveOp parsed)
    {
        switch (op)
        {
            case "move":
                parsed = MoveOp.Move;
                return true;
            case "rollback":
                parsed = MoveOp.Rollback;
                return true;
            case "finalize":
                parsed = MoveOp.Finalize;
                return true;
            case "abort":
                parsed = MoveOp.Abort;
                return true;
            default:
                parsed = MoveOp.Move;
                return false;
        }
    }

    internal static string OpToString(MoveOp op) => op switch
    {
        MoveOp.Move => "move",
        MoveOp.Rollback => "rollback",
        MoveOp.Finalize => "finalize",
        MoveOp.Abort => "abort",
        _ => throw new ArgumentOutOfRangeException(nameof(op), op, "неизвестная операция заявки"),
    };

    // DTO-форма: op в JSON — строка (маппинг строки → MoveOp — в Parse).
    private sealed record MoveRequestDto(
        [property: JsonPropertyName("op")] string? Op,
        [property: JsonPropertyName("to")] string? To,
        [property: JsonPropertyName("old_shard")] string? OldShard,
        [property: JsonPropertyName("skip_reverse")] bool SkipReverse,
        [property: JsonPropertyName("resume")] bool Resume,
        [property: JsonPropertyName("force")] bool Force,
        [property: JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: JsonPropertyName("requested_by")] string? RequestedBy);
}
