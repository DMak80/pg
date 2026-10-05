using Shared.Core;

namespace PgWorker.WalReceiver;

/// <summary>Транспорт физической репликации (t27, arch/19 §3): реализация Npgsql —
/// Task 5; фейк — юнит-тесты. Семантика: Open — подключение + сверка
/// wal_segment_size (mismatch → Failed permanent); Start — START_REPLICATION
/// SLOT <slot> PHYSICAL <startLsn> со СТАРТОВОЙ позицией; Stream — XLogData
/// после успешного Start; SetConfirmed — standby status update (write=flush=
/// applied одной LSN — ЕДИНСТВЕННОЕ место подтверждения слота — после S3-put);
/// ReadTimelineHistory — TIMELINE_HISTORY протокола; null = рантайм не
/// экспонирует (fallback-контроль воркера, arch/19 §3).</summary>
public interface IXLogReplicationSource : IAsyncDisposable
{
    Task<Result> OpenAsync(CancellationToken ct);

    Task<Result> StartReplicationAsync(string slot, ulong startLsn, CancellationToken ct);

    /// <summary>Доступен после успешного StartReplicationAsync; обрыв — исключение
    /// (transient-переподключение), конец потока — transient-переподключение.</summary>
    IAsyncEnumerable<XLogChunk> Stream { get; }

    Task<Result> SetConfirmedAsync(ulong confirmedLsn, CancellationToken ct);

    /// <summary>null — рантайм не экспонирует TIMELINE_HISTORY (маркер history_missing).</summary>
    Task<Result<byte[]?>> ReadTimelineHistoryAsync(uint tli, CancellationToken ct);
}
