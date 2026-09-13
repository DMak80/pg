using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups.Supervisor;

/// <summary>Чистые решения сверок S3↔etcd (t07, arch/19 §4): без I/O,
/// юнит-тестируемы. Мгновенно удаляется только мусор, который восстановление
/// гарантированно не выберет: префикс full/&lt;id&gt;/ в S3 БЕЗ etcd-ключа
/// (выбора кандидатов restore не имеет); всё, что может иметь DR-ценность, —
/// реестр сирот + TTL (BackupOrphanSweeper).</summary>
public static class SupervisorPlanner
{
    /// <summary>S3-id префиксов full/&lt;id&gt;/ без etcd-ключа (мусор живого
    /// шарда). Ключ с любым state (вкл. FAILED/DELETING) — владелец есть:
    /// судьбу решают гигиена FAILED (t06) и ретенция, не супервизор.
    /// Порядок входа сохранён.</summary>
    public static IReadOnlyList<string> SelectUnownedFulls(
        IReadOnlyList<string> s3FullIds, IReadOnlyList<FullBackupState> etcdFulls)
    {
        var owned = etcdFulls.Select(f => f.Id).ToHashSet(StringComparer.Ordinal);
        return s3FullIds.Where(id => !owned.Contains(id)).ToList();
    }
}
