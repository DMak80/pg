namespace PgWorker.Backups;

// Канонические имена подсистемы бэкапов (arch/19 §2/§4): ключи etcd пишет
// ТОЛЬКО воркер под клэймом <C>. t03 добавляет имя слота WAL-агента.
// Docker-имён джобов бэкапов больше нет (t26+): все операции исполняют
// приёмники WalReceiver нод (§2/§3.5/§3.6/§5).
public static class BackupNames
{
    public static string ClusterPrefix(string cluster) => $"/pgworker/backups/{cluster}/";

    public static string FullKey(string cluster, string shard, string id)
        => $"/pgworker/backups/{cluster}/{shard}/full/{id}";

    /// <summary>Ключ заявки восстановления шарда (t05, arch/19 §4):
    /// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/restore/&lt;id&gt;.</summary>
    public static string RestoreKey(string cluster, string shard, string id)
        => $"/pgworker/backups/{cluster}/{shard}/restore/{id}";

    /// <summary>Ключ дрилла шарда (reliability t02, arch/19 §4):
    /// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/drill — один на шард.</summary>
    public static string DrillKey(string cluster, string shard)
        => $"/pgworker/backups/{cluster}/{shard}/drill";

    /// <summary>Слот приёмника WAL шарда (t03, arch/19 §3): pgw_bkp_&lt;C&gt;_&lt;X&gt;;
    /// длиннее NAMEDATALEN(63) → pgw_bkp_ + sha1("&lt;C&gt;/&lt;X&gt;")[:16] (усечение без
    /// коллизий на практике; ensure слота — воркер, стрим — приёмник).</summary>
    public static string Slot(string cluster, string shard)
    {
        var full = $"pgw_bkp_{cluster}_{shard}";
        if (System.Text.Encoding.ASCII.GetByteCount(full) <= 63)
            return full;
        var hash = Convert.ToHexStringLower(
            System.Security.Cryptography.SHA1.HashData(
                System.Text.Encoding.UTF8.GetBytes($"{cluster}/{shard}")))[..16];
        return $"pgw_bkp_{hash}";
    }
}
