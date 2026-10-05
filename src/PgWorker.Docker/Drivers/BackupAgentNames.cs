namespace PgWorker.Docker.Drivers;

/// <summary>Имена docker-объектов WAL-агентов бэкапов (t27, arch/19 §3): per-node
/// контейнеры pgw-backup-wal-&lt;C&gt;-&lt;X&gt;-&lt;N&gt; (агент на каждой ноде-источнике);
/// staging-тома больше нет (буфер сегмента — память приёмника). ЕДИНСТВЕННЫЙ
/// источник имён для драйвера (ensure/remove/list) и WalStreamProcess (ContainerSpec).</summary>
public static class BackupAgentNames
{
    public static string Prefix(string cluster) => $"pgw-backup-wal-{cluster}-";

    public static string Container(string cluster, string shard, string node)
        => $"pgw-backup-wal-{cluster}-{shard}-{node}";
}
