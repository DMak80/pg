namespace AdminPanel.Core;

// Сверка S3-инвентаря с etcd-состоянием бэкапов (t08, spec §4.4, adminpanel/02
// §2.5): чистая функция над снапшотом, без IO — вычисляется в query на запрос.
// Панель ТОЛЬКО ПОКАЗЫВАЕТ расхождения: сироты находит и удаляет супервизор
// воркера (arch/19 §4), вердикты WAL не ставятся.

public static class MinioReconciler
{
    // Активные заявки без объектов — ожидаемо («идёт, объектов ещё нет»).
    private static bool IsEtcdActive(string state) =>
        state is "PLANNED" or "RUNNING" or "UPLOADING";

    public static BackupReconcileInfo Reconcile(
        MinioStorageInfo minio,
        IReadOnlyList<ClusterBackupsInfo> backups,
        IReadOnlyList<ClusterInfo> clusters,
        BackupOrphansInfo? orphanRegistry)
    {
        // Множество владельцев: имена кластеров × имена шардов (формы «C» и «C/X»).
        var owners = new HashSet<string>(StringComparer.Ordinal);
        foreach (var cluster in clusters)
        {
            owners.Add(cluster.Name);
            foreach (var shard in cluster.Shards)
                owners.Add($"{cluster.Name}/{shard.Name}");
        }

        var backupsByCluster = backups.ToDictionary(b => b.Cluster, StringComparer.Ordinal);
        var registry = orphanRegistry?.Orphans.ToDictionary(
            o => o.Prefix, StringComparer.Ordinal);

        var fulls = new List<BackupFullReconcile>();
        var wal = new List<BackupWalReconcile>();
        var orphans = new List<BackupOrphanPrefix>();

        // Объединение кластеров: S3-дерево ∪ etcd-Backups. Кластер, известный
        // только etcd (bucket/префикс стёрт, свежий кластер до первой загрузки),
        // обязан попасть в сверку: его полные — EtcdOnly/InProgress, wal — etcd-
        // факты («etcd-ключи без объектов» — ядро сверки, spec §1/§4.4).
        var treeByCluster = minio.Clusters.ToDictionary(c => c.Cluster, StringComparer.Ordinal);
        var clusterNames = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var clusterNode in minio.Clusters)
            clusterNames.Add(clusterNode.Cluster);
        foreach (var etcdCluster in backups)
            clusterNames.Add(etcdCluster.Cluster);

        foreach (var clusterName in clusterNames)
        {
            treeByCluster.TryGetValue(clusterName, out var clusterNode);
            backupsByCluster.TryGetValue(clusterName, out var etcdCluster);
            var shardNames = new SortedSet<string>(StringComparer.Ordinal);
            var anyOwnedTreeShard = false;

            // Обход S3-дерева: сироты-шарды + сбор имён шардов для объединения
            // (у etcd-only кластера дерева нет — сирот по определению нет).
            if (clusterNode is not null)
            {
                foreach (var shardNode in clusterNode.Shards)
                {
                    shardNames.Add(shardNode.Shard);
                    var owned = owners.Contains($"{clusterName}/{shardNode.Shard}");
                    anyOwnedTreeShard |= owned;
                    if (!owned)
                        orphans.Add(MakeOrphan(
                            $"{clusterName}/{shardNode.Shard}", "shard",
                            shardNode.SizeBytes, registry));
                }
            }

            // Объединение с etcd-шардами: полные и wal могут существовать без S3-узла.
            if (etcdCluster?.ShardsFulls != null)
                foreach (var name in etcdCluster.ShardsFulls.Keys)
                    shardNames.Add(name);
            if (etcdCluster?.Shards != null)
                foreach (var name in etcdCluster.Shards.Keys)
                    shardNames.Add(name);

            foreach (var shardName in shardNames)
            {
                var s3Shard = clusterNode?.Shards.FirstOrDefault(s => s.Shard == shardName);
                ReconcileFulls(clusterName, shardName, s3Shard, etcdCluster, fulls);
                ReconcileWal(clusterName, shardName, s3Shard, etcdCluster, wal);
            }

            // Кластер дерева целиком без владельца → дополнительно префикс <C>
            // (кластеровая запись — только если в дереве нет ни одного шарда-
            // владельца; шардовые префиксы уже выше).
            if (clusterNode is not null && !anyOwnedTreeShard && !owners.Contains(clusterName))
                orphans.Add(MakeOrphan(
                    clusterName, "cluster", clusterNode.SizeBytes, registry));
        }

        return new BackupReconcileInfo(
            fulls
                .OrderBy(f => f.Cluster, StringComparer.Ordinal)
                .ThenBy(f => f.Shard, StringComparer.Ordinal)
                .ThenBy(f => f.Id, StringComparer.Ordinal)
                .ToList(),
            orphans.OrderBy(o => o.Prefix, StringComparer.Ordinal).ToList(),
            wal
                .OrderBy(w => w.Cluster, StringComparer.Ordinal)
                .ThenBy(w => w.Shard, StringComparer.Ordinal)
                .ToList());
    }

    // Полные: объединение id S3 ∪ etcd, статус по правилам spec §4.4.
    private static void ReconcileFulls(
        string cluster, string shard, MinioShardNode? s3Shard,
        ClusterBackupsInfo? etcdCluster, List<BackupFullReconcile> into)
    {
        IReadOnlyList<BackupFullInfo>? etcdFulls = null;
        if (etcdCluster?.ShardsFulls?.TryGetValue(shard, out var list) == true)
            etcdFulls = list;

        var s3ById = s3Shard?.Fulls.ToDictionary(f => f.Id, StringComparer.Ordinal);
        var ids = new SortedSet<string>(StringComparer.Ordinal);
        if (s3ById != null)
            foreach (var id in s3ById.Keys)
                ids.Add(id);
        if (etcdFulls != null)
            foreach (var full in etcdFulls)
                ids.Add(full.Id);

        foreach (var id in ids)
        {
            MinioFullNode? s3 = s3ById?.TryGetValue(id, out var node) == true ? node : null;
            BackupFullInfo? etcd = null;
            if (etcdFulls != null)
            {
                foreach (var candidate in etcdFulls)
                    if (candidate.Id == id) { etcd = candidate; break; }
            }

            var status = (s3 is not null, etcd is not null) switch
            {
                (true, true) => etcd!.State == "DELETING"
                    ? BackupFullReconcileStatus.Deleting      // доводка ретенции
                    : BackupFullReconcileStatus.Ok,
                (true, false) => BackupFullReconcileStatus.S3Only,
                (false, true) => IsEtcdActive(etcd!.State)
                    ? BackupFullReconcileStatus.InProgress
                    : BackupFullReconcileStatus.EtcdOnly,
                _ => BackupFullReconcileStatus.EtcdOnly, // недостижимо: id из одной из сторон
            };

            into.Add(new BackupFullReconcile(
                cluster, shard, id, status,
                s3?.SizeBytes, s3?.ObjectCount, s3?.LastModifiedUnix,
                etcd?.State, etcd?.VerifyState, etcd?.SizeBytes));
        }
    }

    // WAL — только факты обеих сторон, без вердикта (spec §4.4).
    private static void ReconcileWal(
        string cluster, string shard, MinioShardNode? s3Shard,
        ClusterBackupsInfo? etcdCluster, List<BackupWalReconcile> into)
    {
        WalStreamInfo? etcdWal = null;
        if (etcdCluster?.Shards?.TryGetValue(shard, out var info) == true)
            etcdWal = info;
        var s3Wal = s3Shard?.Wal;
        if (s3Wal is null && etcdWal is null)
            return;

        into.Add(new BackupWalReconcile(
            cluster, shard,
            etcdWal?.LastUploadedSegment, etcdWal?.LastUploadedUnix,
            s3Wal?.LastSegment, s3Wal?.LastModifiedUnix));
    }

    // Сирота панели с джойном на реестр воркера по префиксу (Ordinal).
    private static BackupOrphanPrefix MakeOrphan(
        string prefix, string kind, long sizeBytes,
        Dictionary<string, BackupOrphanInfo>? registry)
    {
        BackupOrphanInfo? entry = null;
        registry?.TryGetValue(prefix, out entry);
        return new BackupOrphanPrefix(
            prefix, kind, sizeBytes,
            entry is not null, entry?.State, entry?.FirstSeenUnix);
    }
}
