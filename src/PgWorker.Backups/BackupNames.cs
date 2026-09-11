namespace PgWorker.Backups;

// Канонические имена подсистемы бэкапов (arch/19 §2/§4): ключи etcd пишет
// ТОЛЬКО воркер под клэймом <C>; имена docker-объектов детерминированы —
// takeover-инстанс находит джоб по имени. t03 добавляет имя слота WAL-агента.
public static class BackupNames
{
    public static string ClusterPrefix(string cluster) => $"/pgworker/backups/{cluster}/";

    public static string FullKey(string cluster, string shard, string id)
        => $"/pgworker/backups/{cluster}/{shard}/full/{id}";

    public static string ContainerName(string cluster, string shard, string id)
        => $"pgw-backup-full-{cluster}-{shard}-{id}";

    public static string VolumeName(string cluster, string shard, string id)
        => $"pgw-backup-{cluster}-{shard}-{id}";

    // Префиксы чистки Deprovisioning (D2, t02).
    public static string JobContainerPrefix(string cluster) => $"pgw-backup-full-{cluster}-";

    public static string JobVolumePrefix(string cluster) => $"pgw-backup-{cluster}-";

    // t04: имена verify-джоба — детерминированы (супервиз takeover-инвариантен);
    // контейнер и volume — одно имя (volume ephemeral, чистится после итога).
    public static string VerifyContainerName(string cluster, string shard, string id)
        => $"pgw-backup-verify-{cluster}-{shard}-{id}";

    public static string VerifyVolumeName(string cluster, string shard, string id)
        => $"pgw-backup-verify-{cluster}-{shard}-{id}";

    public static string VerifyJobContainerPrefix(string cluster) => $"pgw-backup-verify-{cluster}-";

    /// <summary>Слот WAL-агента шарда (t03, arch/19 §3): pgw_bkp_&lt;C&gt;_&lt;X&gt;;
    /// длиннее NAMEDATALEN(63) → pgw_bkp_ + sha1("&lt;C&gt;/&lt;X&gt;")[:16] (усечение без
    /// коллизий на практике; имена контейнера/volume агента — BackupAgentNames).</summary>
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
