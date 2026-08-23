using AdminPanel.Api.Inspection;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Мапперы кластерных DTO: чистые функции (spec §10.2) + standNodes (t08 spec §8).
public class ClustersMappersTests
{
    private static readonly DateTimeOffset Now = new(2026, 8, 22, 12, 0, 0, TimeSpan.Zero);
    private static readonly long NowUnix = Now.ToUnixTimeSeconds();

    [Fact]
    public void ClustersMapper_CountsShardsMastersMoves()
    {
        // Arrange: MovingCluster — 2 шарда (s2 без master), 3 не-ACTIVE бакета.

        // Act
        var summaries = ClustersMapper.Map([TestSnapshots.MovingCluster(Now)]);

        // Assert: счётчики UI-таблицы Clusters (arch/03 §3; spec §3.2).
        var summary = summaries.Should().ContainSingle().Subject;
        summary.Name.Should().Be("demo");
        summary.DbName.Should().Be("demo");
        summary.BucketsCount.Should().Be(16);
        summary.ShardsTotal.Should().Be(2);
        summary.ShardsWithMaster.Should().Be(1);
        summary.ActiveMoves.Should().Be(3);
        summary.Incomplete.Should().BeFalse();
    }

    [Fact]
    public void ClustersMapper_IncompleteFlagAndNullDbName()
    {
        // Arrange / Act
        var summaries = ClustersMapper.Map([TestSnapshots.GhostCluster()]);

        // Assert: incomplete-кластер в сводке — dbname null, флаг поднят (spec §3.2).
        var summary = summaries.Should().ContainSingle().Subject;
        summary.Incomplete.Should().BeTrue();
        summary.DbName.Should().BeNull();
    }

    [Fact]
    public void Map_NotInitializedCluster_SetsFlagAndCountsMovesAsRealOnly()
    {
        // Arrange: все бакеты NOT_INITIALIZED + один SYNCING; шард без master и dsn
        var shard = new ShardInfo("shard1", "", [], null, null, null, 2, null,
            [new NodeInfo("shard1a", "NOT_INITIALIZED"), new NodeInfo("shard1b", "NOT_INITIALIZED")], null);
        var cluster = new ClusterInfo("fresh", "fresh", 4, 1755900000, ClusterState.NotInitialized,
            [shard],
            [
                new BucketInfo(0, "shard1", BucketState.NotInitialized, null),
                new BucketInfo(1, "shard1", BucketState.NotInitialized, null),
                new BucketInfo(2, "shard1", BucketState.NotInitialized, null),
                new BucketInfo(3, "shard1", BucketState.Syncing,
                    new MoveInfo("shard1", "shard1", 1, 2, "copy", null)),
            ], []);

        // Act
        var dto = ClustersMapper.Map([cluster]).Single();

        // Assert: бейдж-флаг есть; activeMoves = только реальные переезды (spec t12 §3.6);
        // «без мастера» у не поднятого кластера — не деградация (arch/03 §2)
        dto.NotInitialized.Should().BeTrue();
        dto.ActiveMoves.Should().Be(1);
        dto.ShardsWithMaster.Should().Be(0);
    }

    [Fact]
    public void ClusterDetailsMapper_FullDto()
    {
        // Arrange
        var cluster = TestSnapshots.MovingCluster(Now);

        // Act
        var dto = ClusterDetailsMapper.Map(cluster, NowUnix, null, null, [], []);

        // Assert: config-константы + полные блоки (arch/03 §2).
        dto.Name.Should().Be("demo");
        dto.DbName.Should().Be("demo");
        dto.BucketsCount.Should().Be(16);
        dto.CreatedUnix.Should().Be(1755800000);
        dto.Incomplete.Should().BeFalse();
        dto.Shards.Should().HaveCount(2);
        var s1 = dto.Shards[0];
        s1.Dsn.Should().Contain("host=s1a,s1b");
        s1.Hosts.Should().Equal("s1a", "s1b");
        s1.ReplicasDeclared.Should().Be(1);
        s1.MasterAddress.Should().Be("s1a:5432");
        s1.MasterLeaseAlive.Should().BeTrue();
        s1.Runtime.Should().BeNull(); // t05: данных SQL-пробы нет (spec §3.14)
        dto.Shards[1].MasterLeaseAlive.Should().BeFalse();
        dto.Buckets.Should().HaveCount(16);
        dto.Heals.Should().HaveCount(2);
        dto.StandNodes.Should().BeEmpty(); // пустой реестр → пустой список (t08 spec §8)
    }

    [Fact]
    public void ClusterDetailsMapper_AgeSec_FromMoveAge()
    {
        // Arrange: SYNCING −30 / FROZEN −10 / ABORTING −5; ACTIVE — null (spec §3.7).
        var dto = ClusterDetailsMapper.Map(TestSnapshots.MovingCluster(Now), NowUnix, null, null, [], []);

        // Act — возрасты по id из DTO.
        var ages = dto.Buckets.ToDictionary(b => b.Id, b => b.AgeSec);

        // Assert
        ages[1].Should().Be(30);
        ages[2].Should().Be(10);
        ages[3].Should().Be(5);
        ages[0].Should().BeNull();
        dto.Buckets[0].Move.Should().BeNull();
        dto.Buckets[1].Move!.Target.Should().Be("s2");
        dto.Buckets[1].State.Should().Be("SYNCING");
    }

    [Fact]
    public void ClusterDetailsMapper_Filters_OwnerStateBothNull()
    {
        // Arrange: routing s1 — 8 бакетов (6 базис + SYNCING/FROZEN), s2 — 7, дыра — 1.
        var cluster = TestSnapshots.MovingCluster(Now);

        // Act / Assert: owner — точное совпадение; state — по enum; оба — пересечение (spec §3.9).
        ClusterDetailsMapper.Map(cluster, NowUnix, "s1", null, [], []).Buckets.Should().HaveCount(8);
        ClusterDetailsMapper.Map(cluster, NowUnix, "s1", BucketState.Syncing, [], []).Buckets
            .Should().ContainSingle().Which.Id.Should().Be(1);
        ClusterDetailsMapper.Map(cluster, NowUnix, null, BucketState.Active, [], []).Buckets.Should().HaveCount(13);
        ClusterDetailsMapper.Map(cluster, NowUnix, "nope", null, [], []).Buckets.Should().BeEmpty();
        ClusterDetailsMapper.Map(cluster, NowUnix, null, null, [], []).Buckets.Should().HaveCount(16);
    }

    [Fact]
    public void ClusterDetailsMapper_Heals_NewestFirst()
    {
        // Arrange: журнал — новые сверху; null-штамп в конец (spec §3.3).
        var cluster = TestSnapshots.MovingCluster(Now) with
        {
            Heals =
            [
                new HealRecord("bucket_9", "s1", "s2", "restore-heal", 100),
                new HealRecord("bucket_5", "s2", "s1", "restore-heal", 200),
                new HealRecord("bucket_7", "s1", "s1", "restore-heal", null),
            ],
        };

        // Act
        var dto = ClusterDetailsMapper.Map(cluster, NowUnix, null, null, [], []);

        // Assert
        dto.Heals.Select(h => h.Bucket).Should().Equal("bucket_5", "bucket_9", "bucket_7");
        dto.Heals[0].Was.Should().Be("s2");
        dto.Heals[2].TsUnix.Should().BeNull();
    }

    [Fact]
    public void ClusterDetailsMapper_RuntimeMapped_WhenPresent()
    {
        // Arrange: модель t03 → DTO arch/03 §2 — маппинг фиксируется до данных t06 (spec §3.14).
        var runtime = new ShardRuntime(
            "s1",
            [
                new ReplicationSlotInfo("slot_a", "logical", true, "lost", 1024, 5000),
                new ReplicationSlotInfo("slot_b", "logical", true, "reserved", 2048, 9000),
            ],
            [
                new StandbyInfo("s1b", "10.0.0.2", "streaming", "sync", 100),
                new StandbyInfo("s1c", "10.0.0.3", "streaming", "async", 200),
            ],
            [new SubscriptionInfo("sub_bucket_3", "0/100", "0/200", null)],
            ["bucket_0", "bucket_3"],
            false,
            null);
        var cluster = TestSnapshots.FullCluster() with
        {
            Shards =
            [
                new ShardInfo("s1", "host=s1a port=5432 dbname=demo user=postgres",
                    ["s1a"], 5432, "demo", "postgres", 1, "s1a:5432", [], runtime),
            ],
        };

        // Act
        var dto = ClusterDetailsMapper.Map(cluster, NowUnix, null, null, [], []);

        // Assert: standbiesSync — только sync/quorum; лаг слотов — max; lost — имена слотов.
        var mapped = dto.Shards.Single().Runtime.Should().NotBeNull().And.Subject.As<ShardRuntimeDto>();
        mapped.StandbiesSync.Should().Be(1);
        mapped.SlotsLagMaxBytes.Should().Be(9000);
        mapped.WalStatusLost.Should().Equal("slot_a");
        mapped.Subscriptions.Should().Equal("sub_bucket_3");
        mapped.BucketSchemas.Should().Equal("bucket_0", "bucket_3");
        mapped.Error.Should().BeNull();
    }

    [Fact]
    public void BucketStates_RoundTrip()
    {
        // Arrange / Act / Assert: enum ↔ строки канона; мусор не парсится (spec §3.8).
        BucketStates.Name(BucketState.Active).Should().Be("ACTIVE");
        BucketStates.Name(BucketState.Syncing).Should().Be("SYNCING");
        BucketStates.Name(BucketState.Frozen).Should().Be("FROZEN");
        BucketStates.Name(BucketState.Aborting).Should().Be("ABORTING");
        foreach (var (text, expected) in new (string, BucketState)[]
        {
            ("ACTIVE", BucketState.Active),
            ("SYNCING", BucketState.Syncing),
            ("FROZEN", BucketState.Frozen),
            ("ABORTING", BucketState.Aborting),
        })
        {
            BucketStates.TryParse(text, out var parsed).Should().BeTrue();
            parsed.Should().Be(expected);
        }

        BucketStates.TryParse("bogus", out _).Should().BeFalse();
        BucketStates.TryParse(null, out _).Should().BeFalse();
    }

    [Fact]
    public void ClusterDetailsMapper_StandNodes_MappedFromSnapshot()
    {
        // Arrange: стендовый топо-реестр глобален — передаётся в маппер отдельно от кластера (t08 spec §8).
        var nodes = new[] { new StandNode("node1", "10.0.0.5"), new StandNode("node2", null) };

        // Act
        var dto = ClusterDetailsMapper.Map(TestSnapshots.MovingCluster(Now), NowUnix, null, null, nodes, []);

        // Assert
        dto.StandNodes.Should().HaveCount(2);
        dto.StandNodes[0].Name.Should().Be("node1");
        dto.StandNodes[0].Address.Should().Be("10.0.0.5");
        dto.StandNodes[1].Name.Should().Be("node2");
        dto.StandNodes[1].Address.Should().BeNull();
    }
}
