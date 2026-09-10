namespace PgWorker.Backups;

// Канонические имена подсистемы бэкапов (arch/19 §2/§4): ключи etcd пишет
// ТОЛЬКО воркер под клэймом <C>; имена docker-объектов детерминированы —
// takeover-инстанс находит джоб по имени.
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
}
