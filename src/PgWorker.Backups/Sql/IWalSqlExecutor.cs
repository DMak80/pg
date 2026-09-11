using PgWorker.Core;

namespace PgWorker.Backups.Sql;

/// <summary>SQL-слой WAL-подсистемы к мастеру шарда (Npgsql, admin-DSN — билдер
/// ShardEndpoints.AdminDsn): ensure слота + LSN-зонд лага. Ретраи — тиками
/// процесса (transient), здесь их нет (образец IMoveSqlExecutor).</summary>
public interface IWalSqlExecutor
{
    Task<Result<bool>> SlotExistsAsync(string adminDsn, string slot, CancellationToken ct);

    /// <summary>Идемпотентно: слота нет → pg_create_physical_replication_slot(slot, true)
    /// (immediate reserved, arch/19 §3).</summary>
    Task<Result> EnsureSlotAsync(string adminDsn, string slot, CancellationToken ct);

    /// <summary>Текущая позиция записи мастера: (pg_current_wal_lsn()::text, timeline_id).</summary>
    Task<Result<(string Lsn, int Tli)>> CurrentWalAsync(string adminDsn, CancellationToken ct);
}
