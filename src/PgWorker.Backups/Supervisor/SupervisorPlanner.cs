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

    /// <summary>Возрастной гейт мусора (arch/19 §4 «Гейты мусора»): самый старый
    /// объект unowned-префикса достиг порога GarbageMinAgeSec → мусор дозрел
    /// (заливка in-flight молода — снимок тика мог отстать от планировщика
    /// этого же тика). minAgeSec &lt;= 0 — гейт снят (мгновенное удаление).</summary>
    public static bool IsGarbageMatured(
        DateTimeOffset oldestObject, long minAgeSec, DateTimeOffset now)
        => minAgeSec <= 0 || now - oldestObject >= TimeSpan.FromSeconds(minAgeSec);
}
