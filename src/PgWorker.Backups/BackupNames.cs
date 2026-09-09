using System.Security.Cryptography;
using System.Text;

namespace PgWorker.Backups;

/// <summary>Имена подсистемы WAL-бэкапов (arch/19 §3): слот pgw_bkp_&lt;C&gt;_&lt;X&gt;;
/// длиннее NAMEDATALEN(63) → pgw_bkp_ + sha1("&lt;C&gt;/&lt;X&gt;")[:16] (усечение без коллизий
/// на практике — канон §3; имя контейнера/volume — PgWorker.Docker.BackupAgentNames).</summary>
public static class BackupNames
{
    public static string Slot(string cluster, string shard)
    {
        var full = $"pgw_bkp_{cluster}_{shard}";
        if (Encoding.ASCII.GetByteCount(full) <= 63)
            return full;
        var hash = Convert.ToHexStringLower(
            SHA1.HashData(Encoding.UTF8.GetBytes($"{cluster}/{shard}")))[..16];
        return $"pgw_bkp_{hash}";
    }
}
