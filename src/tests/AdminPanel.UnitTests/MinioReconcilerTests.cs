using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Сверка S3-инвентаря с etcd-состоянием бэкапов (t08, AC4/AC5): все ветви
// статусов полных, сироты с джойном на реестр воркера, wal-факты без вердикта.
public class MinioReconcilerTests
{
    // AAA: объекты полного + etcd COMPLETED → Ok с обоими фактами
    [Fact]
    public void Reconcile_OkFull_BothFacts()
    {
        // Arrange — S3-полный 20260913a и etcd-ключ того же id
        var minio = Minio(ClusterNode("demo", ShardNode("demo", "s1", fulls: [Full("20260913a")])));
        var backups = new[]
        {
            Backups("demo", fulls: Fulls("s1", new BackupFullInfo(
                "20260913a", "COMPLETED", null, 100, 200, 116, "OK", 201, null))),
        };
        var clusters = new[] { Cluster("demo", "s1") };

        // Act
        var result = MinioReconciler.Reconcile(minio, backups, clusters, null);

        // Assert — Ok, S3-факт и etcd-факт рядом
        var full = result.Fulls.Should().ContainSingle().Subject;
        full.Cluster.Should().Be("demo");
        full.Shard.Should().Be("s1");
        full.Id.Should().Be("20260913a");
        full.Status.Should().Be(BackupFullReconcileStatus.Ok);
        full.SizeBytes.Should().Be(116);
        full.ObjectCount.Should().Be(2);
        full.LastModifiedUnix.Should().Be(900);
        full.EtcdState.Should().Be("COMPLETED");
        full.VerifyState.Should().Be("OK");
        full.EtcdSizeBytes.Should().Be(116);
        result.OrphanPrefixes.Should().BeEmpty();
    }

    // AAA: S3-полный без etcd-ключа → S3Only (EtcdState null)
    [Fact]
    public void Reconcile_S3Only_NoEtcdKey()
    {
        // Arrange — объекты есть, ключа нет
        var minio = Minio(ClusterNode("demo", ShardNode("demo", "s1", fulls: [Full("gone")])));
        var clusters = new[] { Cluster("demo", "s1") };

        // Act
        var result = MinioReconciler.Reconcile(minio, [], clusters, null);

        // Assert
        var full = result.Fulls.Should().ContainSingle().Subject;
        full.Status.Should().Be(BackupFullReconcileStatus.S3Only);
        full.SizeBytes.Should().Be(116);
        full.EtcdState.Should().BeNull();
        full.VerifyState.Should().BeNull();
    }

    // AAA: etcd COMPLETED без объектов → EtcdOnly (SizeBytes null)
    [Fact]
    public void Reconcile_EtcdOnly_CompletedWithoutObjects()
    {
        // Arrange — ключ COMPLETED, в S3-шарде полных нет
        var minio = Minio(ClusterNode("demo", ShardNode("demo", "s1")));
        var backups = new[]
        {
            Backups("demo", fulls: Fulls("s1", new BackupFullInfo(
                "old", "COMPLETED", null, 100, 200, 500, "OK", 201, null))),
        };
        var clusters = new[] { Cluster("demo", "s1") };

        // Act
        var result = MinioReconciler.Reconcile(minio, backups, clusters, null);

        // Assert
        var full = result.Fulls.Should().ContainSingle().Subject;
        full.Status.Should().Be(BackupFullReconcileStatus.EtcdOnly);
        full.Id.Should().Be("old");
        full.SizeBytes.Should().BeNull();
        full.ObjectCount.Should().BeNull();
        full.EtcdState.Should().Be("COMPLETED");
        full.EtcdSizeBytes.Should().Be(500);
    }

    // AAA: активные PLANNED/RUNNING без объектов → InProgress, НЕ EtcdOnly
    [Fact]
    public void Reconcile_ActiveNoObjects_IsInProgress()
    {
        // Arrange — две активные заявки без объектов
        var minio = Minio(ClusterNode("demo", ShardNode("demo", "s1")));
        var backups = new[]
        {
            Backups("demo", fulls: Fulls("s1",
                new BackupFullInfo("p1", "PLANNED", null, 100, null, null, null, null, null),
                new BackupFullInfo("r1", "RUNNING", null, 100, null, null, null, null, null))),
        };
        var clusters = new[] { Cluster("demo", "s1") };

        // Act
        var result = MinioReconciler.Reconcile(minio, backups, clusters, null);

        // Assert — оба InProgress («идёт, объектов ещё нет»)
        result.Fulls.Should().HaveCount(2);
        result.Fulls.Should().OnlyContain(f => f.Status == BackupFullReconcileStatus.InProgress);
    }

    // AAA: DELETING-ключ + остатки объектов → Deleting (доводка ретенции)
    [Fact]
    public void Reconcile_Deleting_WithLeftovers()
    {
        // Arrange — ключ DELETING и объекты ещё на месте
        var minio = Minio(ClusterNode("demo", ShardNode("demo", "s1", fulls: [Full("dying")])));
        var backups = new[]
        {
            Backups("demo", fulls: Fulls("s1", new BackupFullInfo(
                "dying", "DELETING", null, 100, null, 116, null, null, null))),
        };
        var clusters = new[] { Cluster("demo", "s1") };

        // Act
        var result = MinioReconciler.Reconcile(minio, backups, clusters, null);

        // Assert
        result.Fulls.Should().ContainSingle().Which.Status
            .Should().Be(BackupFullReconcileStatus.Deleting);
    }

    // AAA: сирота-префикс с записью в реестре воркера → джойн фактов реестра
    [Fact]
    public void Reconcile_OrphanPrefix_WithRegistry()
    {
        // Arrange — ghost-кластера в /clusters/ нет; реестр знает ghost/s9
        var minio = Minio(ClusterNode("ghost", ShardNode("ghost", "s9", size: 50)));
        var clusters = new[] { Cluster("demo", "s1") };
        var registry = new BackupOrphansInfo(
            [new BackupOrphanInfo("ghost/s9", "shard", 50, 1, "OBSERVED")], 100);

        // Act
        var result = MinioReconciler.Reconcile(minio, [], clusters, registry);

        // Assert — сирота с джойном на реестр (плюс кластеровая запись ghost)
        var orphan = result.OrphanPrefixes.Should().Contain(o => o.Prefix == "ghost/s9").Subject;
        orphan.Kind.Should().Be("shard");
        orphan.SizeBytes.Should().Be(50);
        orphan.InWorkerRegistry.Should().BeTrue();
        orphan.RegistryState.Should().Be("OBSERVED");
        orphan.FirstSeenUnix.Should().Be(1);
    }

    // AAA: сирота без записи в реестре (и реестр null) → «панель видит, в реестре нет»
    [Fact]
    public void Reconcile_OrphanPrefix_NotInRegistry()
    {
        // Arrange — тот же ghost/s9, реестра нет вовсе
        var minio = Minio(ClusterNode("ghost", ShardNode("ghost", "s9", size: 50)));
        var clusters = new[] { Cluster("demo", "s1") };

        // Act
        var result = MinioReconciler.Reconcile(minio, [], clusters, null);

        // Assert
        var orphan = result.OrphanPrefixes.Should().Contain(o => o.Prefix == "ghost/s9").Subject;
        orphan.InWorkerRegistry.Should().BeFalse();
        orphan.RegistryState.Should().BeNull();
        orphan.FirstSeenUnix.Should().BeNull();
    }

    // AAA: запись реестра без S3-факта (воркер уже удалил) — НЕ сирота панели
    [Fact]
    public void Reconcile_RegistryEntry_WithoutS3_NotOrphan()
    {
        // Arrange — реестр помнит префикс, в S3-дереве его уже нет
        var minio = Minio(ClusterNode("demo", ShardNode("demo", "s1", fulls: [Full("b1")])));
        var clusters = new[] { Cluster("demo", "s1") };
        var registry = new BackupOrphansInfo(
            [new BackupOrphanInfo("ghost/s9", "shard", 50, 1, "DELETING")], 100);

        // Act
        var result = MinioReconciler.Reconcile(minio, [], clusters, registry);

        // Assert — расхождение увидит оператор по TTL-строке реестра, не панель
        result.OrphanPrefixes.Should().BeEmpty();
    }

    // AAA: wal-сверка — только факты etcd и S3 рядом, без вердикта
    [Fact]
    public void Reconcile_WalFacts_BothSides()
    {
        // Arrange — etcd отстал на сегмент: последний S3 ..02, etcd ..01
        var minio = Minio(ClusterNode("demo", ShardNode("demo", "s1",
            wal: new MinioWalNode(2, 0, 32, 950, "000000010000000000000002"))));
        var backups = new[]
        {
            Backups("demo", wal: ("s1", new WalStreamInfo(
                "demo", "s1", WalStreamInfoState.Active, "sub", "s1a", 900, null, null,
                "000000010000000000000001"))),
        };
        var clusters = new[] { Cluster("demo", "s1") };

        // Act
        var result = MinioReconciler.Reconcile(minio, backups, clusters, null);

        // Assert — оба факта в записи (вердикта нет — spec §4.4)
        var wal = result.Wal.Should().ContainSingle().Subject;
        wal.Cluster.Should().Be("demo");
        wal.Shard.Should().Be("s1");
        wal.EtcdLastSegment.Should().Be("000000010000000000000001");
        wal.EtcdLastUnix.Should().Be(900);
        wal.S3LastObject.Should().Be("000000010000000000000002");
        wal.S3LastModifiedUnix.Should().Be(950);
    }

    // AAA: шард живого кластера — не сирота, даже при полной сверке Ok/S3Only
    [Fact]
    public void Reconcile_ShardOfKnownCluster_NotOrphan()
    {
        // Arrange — demo/s1 с владельцем в /clusters/
        var minio = Minio(ClusterNode("demo", ShardNode("demo", "s1",
            fulls: [Full("b1")], wal: new MinioWalNode(1, 0, 16, 100, "000000010000000000000001"))));
        var clusters = new[] { Cluster("demo", "s1") };

        // Act
        var result = MinioReconciler.Reconcile(minio, [], clusters, null);

        // Assert — блок сирот пуст
        result.OrphanPrefixes.Should().BeEmpty();
    }

    // AAA (ревью Fix 1): кластер есть только в etcd, в S3 пусто (bucket/префикс
    // стёрт, свежий кластер до первой загрузки) → полные EtcdOnly и wal-факты
    // etcd попадают в сверку («etcd-ключи без объектов» — ядро цели грани)
    [Fact]
    public void Reconcile_EtcdOnlyCluster_NoS3Objects()
    {
        // Arrange — дерево MinIO пустое; etcd знает demo/s1: полный b1 + wal-цепочка
        var minio = Minio();
        var backups = new[]
        {
            Backups("demo",
                fulls: Fulls("s1", new BackupFullInfo(
                    "b1", "COMPLETED", null, 100, 200, 116, "OK", 201, null)),
                wal: ("s1", new WalStreamInfo(
                    "demo", "s1", WalStreamInfoState.Active, "sub", "s1a", 900, null, null,
                    "000000010000000000000001"))),
        };
        var clusters = new[] { Cluster("demo", "s1") };

        // Act
        var result = MinioReconciler.Reconcile(minio, backups, clusters, null);

        // Assert — EtcdOnly (ключ без объектов) + wal с etcd-фактом, S3-факт пуст;
        // владельец в /clusters/ есть — сироты нет
        var full = result.Fulls.Should().ContainSingle().Subject;
        full.Cluster.Should().Be("demo");
        full.Shard.Should().Be("s1");
        full.Id.Should().Be("b1");
        full.Status.Should().Be(BackupFullReconcileStatus.EtcdOnly);
        full.SizeBytes.Should().BeNull();
        full.ObjectCount.Should().BeNull();
        full.EtcdState.Should().Be("COMPLETED");
        full.VerifyState.Should().Be("OK");
        full.EtcdSizeBytes.Should().Be(116);
        var wal = result.Wal.Should().ContainSingle().Subject;
        wal.EtcdLastSegment.Should().Be("000000010000000000000001");
        wal.EtcdLastUnix.Should().Be(900);
        wal.S3LastObject.Should().BeNull();
        wal.S3LastModifiedUnix.Should().BeNull();
        result.OrphanPrefixes.Should().BeEmpty();
    }

    // ——— фикстуры ———

    private static MinioStorageInfo Minio(params MinioClusterNode[] clusters) => new(
        Configured: true, Endpoint: "http://minio:9000", Bucket: "pgworker-backups",
        Health: null, Buckets: ["pgworker-backups"],
        UsedBytes: clusters.Sum(c => c.SizeBytes), ObjectCount: 0,
        Clusters: clusters, ForeignPrefixes: [], UpdatedAtUnix: 1000,
        ConsecutiveFailures: 0, LastError: null);

    private static MinioClusterNode ClusterNode(string cluster, params MinioShardNode[] shards) => new(
        cluster, shards.Sum(s => s.SizeBytes), shards);

    private static MinioShardNode ShardNode(
        string cluster, string shard, long size = 116,
        MinioFullNode[]? fulls = null, MinioWalNode? wal = null) => new(
        cluster, shard, size, fulls ?? [], wal);

    private static MinioFullNode Full(string id) => new(id, 116, 2, 900);

    private static ClusterInfo Cluster(string name, params string[] shards) => new(
        name, name, 1, null, ClusterState.Active,
        shards.Select(s => new ShardInfo(s, "", [], null, null, null, null, null, [], null))
            .ToList(),
        [], []);

    private static ClusterBackupsInfo Backups(
        string cluster,
        IReadOnlyDictionary<string, IReadOnlyList<BackupFullInfo>>? fulls = null,
        (string Shard, WalStreamInfo Info)? wal = null) => new(
        cluster, null, new Dictionary<string, long?>(),
        Shards: wal is null
            ? null
            : new Dictionary<string, WalStreamInfo?> { [wal.Value.Shard] = wal.Value.Info },
        ShardsFulls: fulls);

    private static IReadOnlyDictionary<string, IReadOnlyList<BackupFullInfo>> Fulls(
        string shard, params BackupFullInfo[] items) =>
        new Dictionary<string, IReadOnlyList<BackupFullInfo>> { [shard] = items };
}
