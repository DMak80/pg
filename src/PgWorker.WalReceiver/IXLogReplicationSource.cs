using Shared.Core;

namespace PgWorker.WalReceiver;

/// <summary>Отказ START_REPLICATION по 58P01 (сервер не может отдать WAL со
/// стартовой позиции: запрошенный сегмент срезан/не существует на запрошенном
/// таймлайне) — типизированный маркер наружу из транспорта; текст исходной
/// ошибки Postgres — в Message (диагностика stderr). transient-механика
/// tli-процедуры приёмника; матч по SqlState, не по тексту. Транспорт гасит
/// соединение при этом отказе — старт tli-процедуры требует свежего
/// OpenAsync.</summary>
public sealed class WalStartRefused(string serverError)
    : ApplicationException($"START_REPLICATION refused (58P01): {serverError}");

/// <summary>Идентификация сервера репликации (IDENTIFY_SYSTEM): текущий
/// таймлайн сервера и позиция записи WAL.</summary>
public sealed record ServerIdentity(uint Tli, ulong XLogPos);

/// <summary>Транспорт физической репликации (t27, arch/19 §3): реализация Npgsql —
/// Task 5; фейк — юнит-тесты. Семантика: Open — подключение + сверка
/// wal_segment_size (mismatch → Failed permanent); Start — START_REPLICATION
/// SLOT <slot> PHYSICAL <startLsn> со СТАРТОВОЙ позицией, отказ 58P01 — наружу
/// как WalStartRefused (соединение при отказе гасится), timeline — явный
/// TIMELINE команды (null — без TIMELINE, как раньше); Stream — XLogData
/// после успешного Start; SetConfirmed — standby status update (write=flush=
/// applied одной LSN — ЕДИНСТВЕННОЕ место подтверждения слота — после S3-put);
/// ReadTimelineHistory — TIMELINE_HISTORY протокола; null = рантайм не
/// экспонирует (fallback-контроль воркера, arch/19 §3).</summary>
public interface IXLogReplicationSource : IAsyncDisposable
{
    Task<Result> OpenAsync(CancellationToken ct);

    /// <summary>START_REPLICATION SLOT <slot> PHYSICAL <startLsn>; timeline —
    /// явный TIMELINE команды (null — без TIMELINE, как раньше). Отказ 58P01 —
    /// наружу как WalStartRefused (соединение при отказе гасится: следующий
    /// старт требует OpenAsync).</summary>
    Task<Result> StartReplicationAsync(string slot, ulong startLsn, uint? timeline, CancellationToken ct);

    /// <summary>IDENTIFY_SYSTEM по короткоживущему replication-соединению
    /// (образец ReadTimelineHistoryAsync: стрим-коннект после failed-старта уже
    /// погашен).</summary>
    Task<Result<ServerIdentity>> IdentifySystemAsync(CancellationToken ct);

    /// <summary>Доступен после успешного StartReplicationAsync; обрыв — исключение
    /// (transient-переподключение), конец потока — transient-переподключение.</summary>
    IAsyncEnumerable<XLogChunk> Stream { get; }

    Task<Result> SetConfirmedAsync(ulong confirmedLsn, CancellationToken ct);

    /// <summary>null — рантайм не экспонирует TIMELINE_HISTORY (маркер history_missing).</summary>
    Task<Result<byte[]?>> ReadTimelineHistoryAsync(uint tli, CancellationToken ct);
}
