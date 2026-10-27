namespace PgWorker.WalReceiver.Operations;

/// <summary>IMMUTABLE-снимок статуса операции бэкапов (arch/19 §2, spec t26 §3.2):
/// публикация — атомарная замена ссылки (паттерн LiveFacts); поля — контракт
/// GET-статусов (snake_case имена в JSON). Error — однострочная причина
/// (JSON-безопасность). Маркер транзиента скачивания — пара State=failed +
/// Phase=downloading (arch/19 §5); failed с phase verifying/recovering —
/// permanent.</summary>
public sealed record OperationSnapshot(
    string Id,
    string State,
    string Phase,
    string? WalStartSegment = null,
    long? SizeBytes = null,
    string? RestoredToLsn = null,
    string? SystemId = null,
    string? Error = null);

/// <summary>Константы состояний операций (spec t26 §3.2): running | succeeded |
/// failed; у полного дополнительно uploading (фаза фиксации); у tool-операций
/// дополнительно cancelled — операторская отмена (cancel API, внешний канал):
/// НЕ failed — отмена не суждение о данных (arch/19 §3.5/§5); снимок
/// персистентен — воркер-супервиз отличает отмену от vanished (404).</summary>
public static class OperationStates
{
    public const string Running = "running";
    public const string Uploading = "uploading";
    public const string Succeeded = "succeeded";
    public const string Failed = "failed";
    public const string Cancelled = "cancelled";
}

/// <summary>Константы фаз операций (spec t26 §3.2): полный — basebackup |
/// uploading; verify — downloading | verifying; restore — downloading |
/// recovering.</summary>
public static class OperationPhases
{
    public const string Basebackup = "basebackup";
    public const string Uploading = "uploading";
    public const string Downloading = "downloading";
    public const string Verifying = "verifying";
    public const string Recovering = "recovering";
}
