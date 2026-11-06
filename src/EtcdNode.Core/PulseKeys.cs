namespace EtcdNode.Core;

/// <summary>Канонический список пульсирующих ключей etcd
/// (arch/etcdnode/02-snapshots.md §3.1): временные метки живости сервисов,
/// перезаписываемые каждые 1–2 с; изменения ТОЛЬКО в этих ключах не
/// триггерят внеочередной DR-слепок. Пополнение списка — правка канона и
/// этого файла.</summary>
public static class PulseKeys
{
    /// <summary>Ключ — пульс: префикс /service/ (Patroni-DCS: leader,
    /// members, sync, initialize, config) ЛИБО мастер-ключ HA-контура
    /// /clusters/&lt;C&gt;/shards/&lt;X&gt;/master.</summary>
    public static bool IsPulse(string key)
        => key.StartsWith("/service/", StringComparison.Ordinal)
           || IsHaMasterKey(key);

    // /clusters/<C>/shards/<X>/master: split по '/' даёт 6 элементов с
    // ведущим пустым (["", "clusters", C, "shards", X, "master"]).
    private static bool IsHaMasterKey(string key)
    {
        var parts = key.Split('/');
        return parts.Length == 6
               && parts[1] == "clusters"
               && parts[3] == "shards"
               && parts[5] == "master";
    }
}
