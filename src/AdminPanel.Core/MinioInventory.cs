namespace AdminPanel.Core;

// Агрегация list-v2 bucket в дерево <C>/<X> (t08, arch/19 §5, AC2): чистая
// функция — вход «ключ + размер + lastModified», выход — дерево кластеров.
// Layout: <C>/<X>/full/<id>/… — файлы полного; <C>/<X>/wal/<segment> — сегмент;
// <C>/<X>/wal/<TLI>.history — timeline-история; короче <C>/<X>/… — корневой
// foreign-префикс (факт, без вердикта); прочее внутри шарда — только в его
// SizeBytes. Все сортировки — Ordinal.

/// <summary>Один объект bucket'а для агрегации (ключ, размер, lastModified).</summary>
public sealed record MinioObject(string Key, long SizeBytes, long LastModifiedUnix);

public sealed partial record MinioInventory
{
    public static MinioInventory Build(IReadOnlyList<MinioObject> objects)
    {
        long usedBytes = 0, objectCount = 0;
        var foreign = new SortedSet<string>(StringComparer.Ordinal);
        var shards = new Dictionary<(string Cluster, string Shard), ShardAcc>();

        foreach (var obj in objects)
        {
            usedBytes += obj.SizeBytes;
            objectCount++;

            var parts = obj.Key.Split('/');
            if (parts.Length < 3)
            {
                // корень вне формы <C>/<X>/… — foreign-факт (первый сегмент)
                if (parts.Length > 0 && parts[0].Length > 0)
                    foreign.Add(parts[0]);
                continue;
            }

            var acc = GetOrAdd(shards, (parts[0], parts[1]));
            acc.SizeBytes += obj.SizeBytes;

            if (parts[2] == "full" && parts.Length >= 4)
            {
                // файлы полного — агрегат size/count/max по 4+ сегментам пути
                if (!acc.Fulls.TryGetValue(parts[3], out var full))
                    acc.Fulls[parts[3]] = full = new FullAcc();
                full.SizeBytes += obj.SizeBytes;
                full.ObjectCount++;
                full.LastModifiedUnix = Math.Max(full.LastModifiedUnix, obj.LastModifiedUnix);
            }
            else if (parts[2] == "wal" && parts.Length >= 4)
            {
                // сегмент или timeline-история (суффикс .history — отдельный счётчик)
                acc.HasWal = true;
                acc.WalSizeBytes += obj.SizeBytes;
                acc.WalLastModifiedUnix = Math.Max(acc.WalLastModifiedUnix, obj.LastModifiedUnix);
                if (parts[3].EndsWith(".history", StringComparison.Ordinal))
                    acc.WalHistoryCount++;
                else
                    acc.WalSegmentCount++;
            }
            // прочее внутри шарда — учтено только в ShardAcc.SizeBytes
        }

        var clusters = shards
            .GroupBy(kv => kv.Key.Cluster, StringComparer.Ordinal)
            .OrderBy(g => g.Key, StringComparer.Ordinal)
            .Select(g => new MinioClusterNode(
                g.Key,
                g.Sum(kv => kv.Value.SizeBytes),
                g.OrderBy(kv => kv.Key.Shard, StringComparer.Ordinal)
                    .Select(kv => kv.Value.ToNode(g.Key, kv.Key.Shard))
                    .ToList()))
            .ToList();

        return new MinioInventory(
            clusters, usedBytes, objectCount,
            Buckets: [], foreign.ToList());
    }

    private static ShardAcc GetOrAdd(
        Dictionary<(string Cluster, string Shard), ShardAcc> map,
        (string Cluster, string Shard) key)
    {
        if (!map.TryGetValue(key, out var acc))
            map[key] = acc = new ShardAcc();
        return acc;
    }

    // Накопители одного прогона (мутируемые, наружу не выходят — итог records).
    private sealed class FullAcc
    {
        public long SizeBytes;
        public long ObjectCount;
        public long LastModifiedUnix;
    }

    private sealed class ShardAcc
    {
        public long SizeBytes;
        public Dictionary<string, FullAcc> Fulls { get; } = new(StringComparer.Ordinal);
        public bool HasWal;
        public long WalSegmentCount;
        public long WalHistoryCount;
        public long WalSizeBytes;
        public long WalLastModifiedUnix;

        public MinioShardNode ToNode(string cluster, string shard) => new(
            cluster,
            shard,
            SizeBytes,
            Fulls.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => new MinioFullNode(
                    kv.Key, kv.Value.SizeBytes, kv.Value.ObjectCount, kv.Value.LastModifiedUnix))
                .ToList(),
            HasWal
                ? new MinioWalNode(WalSegmentCount, WalHistoryCount, WalSizeBytes, WalLastModifiedUnix)
                : null);
    }
}
