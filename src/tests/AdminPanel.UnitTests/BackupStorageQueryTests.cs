using AdminPanel.Api.Inspection;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Гварды и мапперы REST-грани «Хранилище бэкапов» (t08, spec §4.6):
// двухступенчатый гвард prefix (форма + принадлежность), диапазон maxKeys,
// DTO configured=false, слитые сироты, джойн деталей шарда.
public class BackupStorageQueryTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 13, 12, 0, 0, TimeSpan.Zero);
    private const long NowUnix = 1788331200; // Now
    private const long TtlSec = 604800;      // дефолт OrphanTtlSec (7 сут)

    // ——— ступень 1 гварда prefix: форма — AAA ———

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("demo/s1/full/")]
    public void PrefixForm_Valid_FormsAccepted(string? prefix)
    {
        // Arrange/Act — пустой префикс или канонический первый сегмент
        var valid = BackupObjectsQuery.IsValidPrefixForm(prefix);

        // Assert
        valid.Should().BeTrue();
    }

    [Theory]
    [InlineData("Bad/x")]    // заглавная
    [InlineData("1demo/x")]  // цифра первой
    [InlineData("-demo/x")]  // дефис первой
    [InlineData("DEMO/x")]   // все заглавные
    public void PrefixForm_Invalid_FormsRejected(string prefix)
    {
        // Arrange/Act — первый сегмент вне ^[a-z][a-z0-9_]{0,62}$
        var valid = BackupObjectsQuery.IsValidPrefixForm(prefix);

        // Assert
        valid.Should().BeFalse();
    }

    // ——— ступень 2 гварда prefix: принадлежность — AAA ———

    // AAA: фикстура снапшота — кластер demo в etcd + S3-дерево demo/ghost-shard
    [Fact]
    public void PrefixKnown_ClusterOrTree_Accepted()
    {
        // Arrange
        var snapshot = SnapshotWithTree();

        // Act/Assert — кластер снапшота и сирота из дерева валидны
        BackupObjectsQuery.IsPrefixKnown("demo/s1/", snapshot).Should().BeTrue();
        BackupObjectsQuery.IsPrefixKnown("ghost-shard/s9/", snapshot).Should().BeTrue();
    }

    // AAA: freepath матчит форму, но отсутствует и в etcd, и в дереве → 400 (AC7)
    [Fact]
    public void PrefixKnown_Foreepath_Rejected()
    {
        // Arrange
        var snapshot = SnapshotWithTree();

        // Act
        var known = BackupObjectsQuery.IsPrefixKnown("freepath/", snapshot);

        // Assert
        known.Should().BeFalse();
    }

    // ——— гвард maxKeys — AAA ———

    [Fact]
    public void MaxKeys_Guards()
    {
        // Arrange/Act/Assert — null → дефолт 200; 1 и 1000 валидны; вне — нет
        BackupObjectsQuery.EffectiveMaxKeys(null).Should().Be(200);
        BackupObjectsQuery.IsValidMaxKeys(1).Should().BeTrue();
        BackupObjectsQuery.IsValidMaxKeys(1000).Should().BeTrue();
        BackupObjectsQuery.IsValidMaxKeys(0).Should().BeFalse();
        BackupObjectsQuery.IsValidMaxKeys(1001).Should().BeFalse();
        BackupObjectsQuery.IsValidMaxKeys(-5).Should().BeFalse();
    }

    // ——— маппер configured=false — AAA ———

    // AAA: снапшот без MinioStorage → DTO только Configured=false + причина
    [Fact]
    public void MapStorage_NotConfigured_Stub()
    {
        // Arrange
        var snapshot = TestSnapshots.Healthy(Now);

        // Act
        var dto = BackupStorageMappers.MapStorage(snapshot, TtlSec, NowUnix);

        // Assert
        dto.Configured.Should().BeFalse();
        dto.NotConfiguredReason.Should().Contain("AdminPanel:Backups:S3");
        dto.Clusters.Should().BeEmpty();
        dto.LiveUsedBytes.Should().BeNull();
        dto.InventoryUpdatedUnix.Should().Be(0);
    }

    // ——— маппер сводки — AAA ———

    // AAA: дерево + квота WARN + реестр сирот + панельная сверка → Orphans слиты
    [Fact]
    public void MapStorage_Summary_MergesOrphansAndQuota()
    {
        // Arrange — demo/s1 (владелец есть), ghost-shard/s9 (сирота в реестре)
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            MinioStorage = Minio(
                ClusterNode("demo", ShardNode("demo", "s1", fulls: [Full("b1")])),
                ClusterNode("ghost-shard", ShardNode("ghost-shard", "s9", size: 50))),
            BackupStorage = new BackupStorageInfo(152, 1000, 15.2, BackupStorageState.Warn, 900),
            BackupOrphans = new BackupOrphansInfo(
                [new BackupOrphanInfo("ghost/s9", "shard", 50, NowUnix - 100, "OBSERVED")], NowUnix),
        };

        // Act
        var dto = BackupStorageMappers.MapStorage(snapshot, TtlSec, NowUnix);

        // Assert — configured/квота WARN рядом с live; сироты слиты: панельная
        // (шард), панельная (кластер целиком), реестровая без S3-факта (TTL-строка)
        dto.Configured.Should().BeTrue();
        dto.LiveUsedBytes.Should().Be(166); // 116 (demo/s1) + 50 (ghost-shard/s9)
        dto.Etcd.Should().NotBeNull();
        dto.Etcd!.State.Should().Be("WARN");
        dto.Etcd.UsedBytes.Should().Be(152);

        var shardOrphan = dto.Orphans.Single(o => o.Prefix == "ghost-shard/s9");
        shardOrphan.Kind.Should().Be("shard");
        shardOrphan.InWorkerRegistry.Should().BeFalse(); // реестр знает ghost/s9, не ghost-shard/s9
        shardOrphan.FirstSeenUnix.Should().BeNull();

        dto.Orphans.Should().Contain(o => o.Prefix == "ghost-shard" && o.Kind == "cluster");

        var registryOnly = dto.Orphans.Single(o => o.Prefix == "ghost/s9");
        registryOnly.InWorkerRegistry.Should().BeTrue();
        registryOnly.RegistryState.Should().Be("OBSERVED");
        registryOnly.TtlLeftSec.Should().Be(TtlSec - 100).And.BeGreaterThan(0);
    }

    // AAA: запись реестра ДЛЯ существующего S3-сироты — джойн по Prefix с TTL
    [Fact]
    public void MapStorage_OrphanJoined_WithRegistryTtl()
    {
        // Arrange — префиксы совпадают: ghost-shard/s9 в дереве и в реестре
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            MinioStorage = Minio(
                ClusterNode("ghost-shard", ShardNode("ghost-shard", "s9", size: 50))),
            BackupOrphans = new BackupOrphansInfo(
                [new BackupOrphanInfo("ghost-shard/s9", "shard", 50, NowUnix - 100, "OBSERVED")], NowUnix),
        };

        // Act
        var dto = BackupStorageMappers.MapStorage(snapshot, TtlSec, NowUnix);

        // Assert — реестр OBSERVED, остаток TTL > 0 (кластеровая запись ghost-shard
        // отдельной строкой — кластер целиком без владельца)
        var orphan = dto.Orphans.Single(o => o.Prefix == "ghost-shard/s9");
        orphan.Kind.Should().Be("shard");
        orphan.InWorkerRegistry.Should().BeTrue();
        orphan.RegistryState.Should().Be("OBSERVED");
        orphan.FirstSeenUnix.Should().Be(NowUnix - 100);
        orphan.TtlLeftSec.Should().Be(TtlSec - 100).And.BeGreaterThan(0);
    }

    // ——— маппер деталей шарда — AAA ———

    // AAA: etcd full COMPLETED+verify OK + S3-факт → Reconcile="Ok"; PLANNED-restore — бейдж
    [Fact]
    public void MapShard_JoinsEtcdAndS3()
    {
        // Arrange — полный b1 с обеих сторон, restore PLANNED в фазе downloading
        var minio = Minio(ClusterNode("demo", ShardNode("demo", "s1", fulls: [Full("b1")])));
        var etcdCluster = new ClusterBackupsInfo(
            "demo", null, new Dictionary<string, long?>(),
            ShardsRestores: new Dictionary<string, IReadOnlyList<RestoreOperationInfo>>
            {
                ["s1"] = [new RestoreOperationInfo(
                    "demo", "s1", "r1", "PLANNED", null, NowUnix - 10, null, null, "downloading")],
            },
            ShardsFulls: Fulls("s1", new BackupFullInfo(
                "b1", "COMPLETED", null, 100, 200, 116, "OK", 201, null)));
        var reconcile = MinioReconciler.Reconcile(
            minio, [etcdCluster], [Cluster("demo", "s1")], null);

        // Act
        var dto = BackupStorageMappers.MapShard(
            "demo", "s1", minio.Clusters[0].Shards[0], etcdCluster, reconcile);

        // Assert — джойн фактов + бейдж активного restore (без кнопок)
        var full = dto.Fulls.Should().ContainSingle().Subject;
        full.Id.Should().Be("b1");
        full.Reconcile.Should().Be("Ok");
        full.EtcdState.Should().Be("COMPLETED");
        full.VerifyState.Should().Be("OK");
        full.SizeBytes.Should().Be(116);
        full.ObjectCount.Should().Be(2);
        dto.ActiveRestore.Should().NotBeNull();
        dto.ActiveRestore!.State.Should().Be("PLANNED");
        dto.ActiveRestore.Phase.Should().Be("downloading");
    }

    // ——— фикстуры ———

    private static EtcdSnapshot SnapshotWithTree() => TestSnapshots.Healthy(Now) with
    {
        MinioStorage = Minio(
            ClusterNode("demo", ShardNode("demo", "s1", fulls: [Full("b1")])),
            ClusterNode("ghost-shard", ShardNode("ghost-shard", "s9", size: 50))),
    };

    private static MinioStorageInfo Minio(params MinioClusterNode[] clusters) => new(
        Configured: true, Endpoint: "http://minio:9000", Bucket: "pgworker-backups",
        Health: null, Buckets: ["pgworker-backups"],
        UsedBytes: clusters.Sum(c => c.SizeBytes), ObjectCount: 0,
        Clusters: clusters, ForeignPrefixes: [], UpdatedAtUnix: NowUnix,
        ConsecutiveFailures: 0, LastError: null);

    private static MinioClusterNode ClusterNode(string cluster, params MinioShardNode[] shards) => new(
        cluster, shards.Sum(s => s.SizeBytes), shards);

    private static MinioShardNode ShardNode(
        string cluster, string shard, long size = 116,
        MinioFullNode[]? fulls = null) => new(
        cluster, shard, size, fulls ?? [], null);

    private static MinioFullNode Full(string id) => new(id, 116, 2, 900);

    private static ClusterInfo Cluster(string name, params string[] shards) => new(
        name, name, 1, null, ClusterState.Active,
        shards.Select(s => new ShardInfo(s, "", [], null, null, null, null, null, [], null))
            .ToList(),
        [], []);

    private static IReadOnlyDictionary<string, IReadOnlyList<BackupFullInfo>> Fulls(
        string shard, params BackupFullInfo[] items) =>
        new Dictionary<string, IReadOnlyList<BackupFullInfo>> { [shard] = items };
}
