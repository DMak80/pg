using PgWorker.Core;

namespace PgWorker.Backups.Sql;

/// <summary>SQL-слой WAL-подсистемы к мастеру шарда (Npgsql, admin-DSN — билдер
/// ShardEndpoints.AdminDsn): зонд/ensure/recreate слота + LSN-зонд лага. Ретраи —
/// тиками процесса (transient), здесь их нет (образец IMoveSqlExecutor).</summary>
public interface IWalSqlExecutor
{
    /// <summary>Зонд слота: (Exists, WalStatus) из pg_replication_slots. Строки
    /// нет → (false, null); существующий слот с null-статусом (старые PG/edge) —
    /// трактуется вызывающим как живой: лечим только явный 'lost' (arch/19 §3).</summary>
    Task<Result<(bool Exists, string? WalStatus)>> SlotProbeAsync(string adminDsn, string slot, CancellationToken ct);

    /// <summary>Идемпотентно, self-contained (зонд внутри): отсутствует → create
    /// (immediate+reserved); существует и wal_status='lost' → drop+create; жив —
    /// не трогать. Вызывающим предварительный зонд не нужен.</summary>
    Task<Result> EnsureSlotAliveAsync(string adminDsn, string slot, CancellationToken ct);

    /// <summary>Пересоздание: drop (undefined_object — норм) + create
    /// immediate+reserved (тот же вызов, что ensure). Идемпотентен; ошибка —
    /// Result.Failed (transient, тик повторит).</summary>
    Task<Result> RecreateSlotAsync(string adminDsn, string slot, CancellationToken ct);

    /// <summary>Текущая позиция записи мастера: (pg_current_wal_lsn()::text, timeline_id).</summary>
    Task<Result<(string Lsn, int Tli)>> CurrentWalAsync(string adminDsn, CancellationToken ct);
}
