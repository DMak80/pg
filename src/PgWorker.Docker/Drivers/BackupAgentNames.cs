namespace PgWorker.Docker.Drivers;

/// <summary>Имена docker-объектов WAL-агентов бэкапов (arch/19 §3): контейнер
/// pgw-backup-wal-&lt;C&gt;-&lt;X&gt; + staging volume. ЕДИНСТВЕННЫЙ источник имён для
/// драйвера (ensure/remove/list) и WalStreamProcess (ContainerSpec).</summary>
public static class BackupAgentNames
{
    public static string Prefix(string cluster) => $"pgw-backup-wal-{cluster}-";

    public static string Container(string cluster, string shard) => $"pgw-backup-wal-{cluster}-{shard}";

    public static string Volume(string cluster, string shard) => $"pgw-backup-wal-{cluster}-{shard}-staging";
}
