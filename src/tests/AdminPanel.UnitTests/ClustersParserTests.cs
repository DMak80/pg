using AdminPanel.Core;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер /clusters/: полный demo-сид и вырожденные случаи (spec §10.1, arch/02 §7–8).
public class ClustersParserTests
{
    // Локальный конструктор Kv для ad-hoc-ключей (modRevision не важен парсеру).
    private static Kv Kv(string key, string value) => new(key, value, 1);

    [Fact]
    public void Parse_FullDemoSeed_BuildsClustersShardsBuckets()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-full.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        var demo = result.Clusters.Should().ContainSingle(c => c.Name == "demo").Subject;
        demo.DbName.Should().Be("demo");
        demo.BucketsCount.Should().Be(16);
        demo.CreatedUnix.Should().Be(1755800000);
        demo.Incomplete.Should().BeFalse();
        var s1 = demo.Shards.Should().ContainSingle(s => s.Name == "s1").Subject;
        s1.Dsn.Should().Be("host=s1a,s1b port=5432 dbname=demo user=postgres");
        s1.DsnHosts.Should().Equal("s1a", "s1b");
        s1.Port.Should().Be(5432);
        s1.DbName.Should().Be("demo");
        s1.User.Should().Be("postgres");
        s1.ReplicasDeclared.Should().Be(1);
        s1.MasterAddress.Should().Be("s1a:5432");
        s1.MasterLeaseAlive.Should().BeTrue();
        demo.Buckets.Should().HaveCount(16);
        demo.Buckets.Single(b => b.Id == 0).Owner.Should().Be("s1");
        demo.Buckets.Single(b => b.Id == 1).Owner.Should().Be("s2");
        result.Errors.Should().BeEmpty();
        result.UnknownKeyCount.Should().Be(0);
    }

    [Fact]
    public void Parse_StatusKeys_MapToMoveInfo()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-full.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        var demo = result.Clusters.Single();
        var syncing = demo.Buckets.Single(b => b.Id == 3);
        syncing.State.Should().Be(BucketState.Syncing);
        syncing.Move.Should().NotBeNull();
        syncing.Move!.Owner.Should().Be("s1");
        syncing.Move.Target.Should().Be("s2");
        syncing.Move.StartedUnix.Should().Be(1755900000);
        syncing.Move.UpdatedUnix.Should().Be(1755900600);
        syncing.Move.Phase.Should().Be("copy");
        demo.Buckets.Single(b => b.Id == 7).State.Should().Be(BucketState.Aborting);
        demo.Buckets.Single(b => b.Id == 7).Move!.LastError.Should().Be("receiver went away");
        demo.Buckets.Single(b => b.Id == 11).State.Should().Be(BucketState.Frozen);
        // отсутствие status-ключа = ACTIVE (arch/02 §2.1)
        var active = demo.Buckets.Single(b => b.Id == 0);
        active.State.Should().Be(BucketState.Active);
        active.Move.Should().BeNull();
    }

    [Fact]
    public void Parse_HealJournal_Collected()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-full.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        var heal = result.Clusters.Single().Heals.Should().ContainSingle(h => h.Bucket == "bucket_5").Subject;
        heal.Was.Should().Be("s2");
        heal.Now.Should().Be("s1");
        heal.Reason.Should().Be("restore-heal");
        heal.TsUnix.Should().Be(1755600000);
    }

    [Fact]
    public void Parse_MissingConfig_ClusterIncomplete()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-degenerate.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        var noconfig = result.Clusters.Should().ContainSingle(c => c.Name == "noconfig").Subject;
        noconfig.Incomplete.Should().BeTrue();
        noconfig.DbName.Should().BeNull();
        noconfig.BucketsCount.Should().Be(0);
        noconfig.Shards.Should().ContainSingle(s => s.Name == "y1");
        // бакеты incomplete-кластера — из фактических ключей (spec §3.7): routing/status-ключей нет → пусто
        noconfig.Buckets.Should().BeEmpty();
    }

    [Fact]
    public void Parse_ConfigWithoutCreatedUnix_NullCreatedUnix()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-degenerate.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        var broken = result.Clusters.Should().ContainSingle(c => c.Name == "broken").Subject;
        broken.CreatedUnix.Should().BeNull();
        // строковые числа толерантны (arch/02 §8)
        broken.BucketsCount.Should().Be(8);
        broken.DbName.Should().Be("broken");
    }

    [Fact]
    public void Parse_BrokenValues_ParseErrorsRecorded()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-degenerate.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        // битый JSON статус-ключа: ключ пропущен, ошибка зафиксирована
        result.Errors.Should().Contain(e => e.Key == "/clusters/demo2/buckets/status/bucket_1");
        // replicas не число: ReplicasDeclared=null + ошибка
        result.Errors.Should().Contain(e => e.Key == "/clusters/broken/shards/x1/replicas");
        var x1 = result.Clusters.Single(c => c.Name == "broken").Shards.Single(s => s.Name == "x1");
        x1.ReplicasDeclared.Should().BeNull();
        // пустой master: MasterAddress=null + ошибка (spec §6.1)
        result.Errors.Should().Contain(e => e.Key == "/clusters/demo2/shards/s1/master");
        var m = result.Clusters.Single(c => c.Name == "demo2").Shards.Single(s => s.Name == "s1");
        m.MasterAddress.Should().BeNull();
        // bucket_abc — нечисловой id
        result.Errors.Should().Contain(e => e.Key == "/clusters/demo2/buckets/routing/bucket_abc");
        // heal без поля "bucket": имя — суффикс ключа (spec §6.1)
        var healed = result.Clusters.Single(c => c.Name == "broken").Heals
            .Should().ContainSingle().Subject;
        healed.Bucket.Should().Be("bucket_2");
        healed.Reason.Should().Be("manual");
    }

    [Fact]
    public void Parse_OutOfRangeRouting_StillParsed()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-degenerate.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        // bucket_99 при N=4 остаётся в списке бакетов — детект out-of-range это алерт t04 (P18)
        var demo2 = result.Clusters.Single(c => c.Name == "demo2");
        demo2.Buckets.Single(b => b.Id == 99).Owner.Should().Be("s9");
        demo2.Buckets.Single(b => b.Id == 0).Owner.Should().Be("s1");
    }

    [Fact]
    public void Parse_UnknownKey_Counted()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-degenerate.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        result.UnknownKeyCount.Should().Be(1); // /clusters/demo2/surprise
    }

    [Fact]
    public void Parse_EmptyPrefix_EmptyResult()
    {
        // Arrange — /clusters/ не существует (пустой ответ range)
        // Act
        var result = ClustersParser.Parse([]);

        // Assert
        result.Clusters.Should().BeEmpty();
        result.Errors.Should().BeEmpty();
        result.UnknownKeyCount.Should().Be(0);
    }

    [Fact]
    public void Parse_ConfigStateNotInitialized_MapsToClusterState()
    {
        // Arrange
        var kvs = new[]
        {
            Kv("/clusters/fresh/config",
                """{"buckets":4,"dbname":"fresh","created_unix":1755900000,"state":"NOT_INITIALIZED"}"""),
        };

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert: state из config; отсутствие поля = Active (arch/02 §2.1).
        result.Clusters.Should().ContainSingle().Which.State.Should().Be(ClusterState.NotInitialized);
    }

    [Fact]
    public void Parse_ConfigStateDeleting_MapsToClusterState()
    {
        // Arrange: config после DELETE /api/clusters (arch/02 §9.4)
        var kvs = new[]
        {
            Kv("/clusters/dying/config",
                """{"buckets":4,"dbname":"dying","created_unix":1755900000,"state":"DELETING"}"""),
        };

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert: DELETING распознан; отсутствие поля = Active.
        result.Clusters.Should().ContainSingle().Which.State.Should().Be(ClusterState.Deleting);
    }

    [Fact]
    public void Parse_BucketStatusNotInitialized_MapsStateAndOwner()
    {
        // Arrange
        var kvs = new[]
        {
            Kv("/clusters/fresh/config", """{"buckets":1,"dbname":"fresh","state":"NOT_INITIALIZED"}"""),
            Kv("/clusters/fresh/buckets/routing/bucket_0", "shard1"),
            Kv("/clusters/fresh/buckets/status/bucket_0",
                """{"bucket":"bucket_0","state":"NOT_INITIALIZED","owner":"shard1","updated_unix":1755900000}"""),
        };

        // Act
        var bucket = ClustersParser.Parse(kvs).Clusters.Single().Buckets.Single();

        // Assert: NOT_INITIALIZED — не ACTIVE-по-умолчанию и не ошибка; owner сохранён.
        bucket.State.Should().Be(BucketState.NotInitialized);
        bucket.Owner.Should().Be("shard1");
        bucket.Move!.Owner.Should().Be("shard1");
        bucket.Move.Target.Should().BeNull();
        bucket.Move.UpdatedUnix.Should().Be(1755900000);
    }

    [Fact]
    public void Parse_ShardNodesState_MapsToNodeInfo()
    {
        // Arrange
        var kvs = new[]
        {
            Kv("/clusters/fresh/config", """{"buckets":1,"dbname":"fresh"}"""),
            Kv("/clusters/fresh/shards/shard1/replicas", "2"),
            Kv("/clusters/fresh/shards/shard1/nodes/shard1a/state", "NOT_INITIALIZED"),
            Kv("/clusters/fresh/shards/shard1/nodes/shard1b/state", "NOT_INITIALIZED"),
        };

        // Act
        var shard = ClustersParser.Parse(kvs).Clusters.Single().Shards.Single();

        // Assert: плановые ноды отсортированы по имени (arch/02 §9.1).
        shard.Nodes.Select(n => n.Name).Should().Equal("shard1a", "shard1b");
        shard.Nodes.Should().OnlyContain(n => n.State == "NOT_INITIALIZED");
    }
}
