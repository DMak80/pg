namespace PgWorker.PgNode.Operations;

/// <summary>Пути операций внутри контейнера ноды (arch/backups/03-restore.md/§3.6/§5):
/// приёмник живёт в контейнере ноды, все операции пишут на data-volume ноды.
/// Pgdata — Spilo-layout PGDATA ноды (volume монтируется в
/// /home/postgres/pgdata, данные — pgroot/data, arch/pgworker/01-placement.md §1) — цель
/// restore-операции (шард демонтирован, volume пуст, §3.5). DrillRoot —
/// scratch-каталог дрилов (НЕ PGDATA ноды — прод-данные недостижимы,
/// §3.6). Staging — дефолт staging полного/verify (env PGW_BACKUP_STAGING_DIR
/// перекрывает, Program).</summary>
public static class NodePaths
{
    public const string Staging = "/home/postgres/pgdata/backup-staging";

    public const string Pgdata = "/home/postgres/pgdata/pgroot/data";

    public const string DrillRoot = "/home/postgres/pgdata/backup-drill";
}
