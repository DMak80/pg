using System;

namespace PgWorker.Etcd.Backups;

/// <summary>Ключ счётчика жизни цепочки WAL шарда (arch/backups/05 §1):
/// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/wal-life; отсутствие ключа = жизнь 0.</summary>
public static class WalLifeKeys
{
    public static string Key(string cluster, string shard)
        => $"/pgworker/backups/{cluster}/{shard}/wal-life";
}
