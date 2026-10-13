namespace OwnS3.Replication.Transport;

// Исход фетча реплики (глава 07 §6) — терминальные варианты одного исхода
// (конвенция главы 01 §4): объект | tombstone | ключ неизвестен; транспортный
// отказ — Result.Failed (не вариант значения); отдельный DTO-дискриминатор не нужен.
public abstract record FetchOutcome
{
    /// <summary>200-объект: тело + заголовки x-owns3-repl-*/x-amz-meta-*.</summary>
    public sealed record Fetched(Stream Body, long ContentLength, long ModTimeUnixMs,
        string ContentSha256, string ETag, string ContentType,
        IReadOnlyDictionary<string, string> UserMetadata) : FetchOutcome;

    /// <summary>200-tombstone: ключ удалён у источника, тела нет.</summary>
    public sealed record Tombstone(long DeleteTimeUnixMs) : FetchOutcome;

    /// <summary>404: ключ неизвестен — состояние приёмника не меняется.</summary>
    public sealed record KeyNotFound() : FetchOutcome;
}
