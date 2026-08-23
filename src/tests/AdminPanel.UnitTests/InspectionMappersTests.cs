using AdminPanel.Api.Inspection;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Мапперы снапшот → DTO: чистые функции, тестируются напрямую (spec §10.3).
public class InspectionMappersTests
{
    private static readonly DateTimeOffset BuiltAt = new(2026, 8, 22, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void OverviewMapper_CountsEtcdAndAlerts()
    {
        // Arrange: 1 critical + 1 warning алерт, 3 живых endpoints из 3.
        var snapshot = TestSnapshots.Healthy(BuiltAt) with
        {
            Alerts =
            [
                new Alert("a:etcd", AlertSeverity.Critical, "a", "etcd", "m", null, null),
                new Alert("b:etcd", AlertSeverity.Warning, "b", "etcd", "m", null, null),
            ],
        };

        // Act
        var dto = OverviewMapper.Map(snapshot, BuiltAt + TimeSpan.FromSeconds(1), 3);

        // Assert
        dto.AlertsCritical.Should().Be(1);
        dto.AlertsWarning.Should().Be(1);
        dto.Etcd.Reachable.Should().BeTrue();
        dto.Etcd.EndpointsOk.Should().Be(3);
        dto.Etcd.EndpointsTotal.Should().Be(3);
        dto.SnapshotAgeMs.Should().Be(1000);
        dto.Stale.Should().BeFalse();
    }

    [Fact]
    public void OverviewMapper_StaleByTripleInterval_True()
    {
        // Arrange: возраст 12 c > порога 3×3 c (spec §3.15).
        var snapshot = TestSnapshots.Healthy(BuiltAt);

        // Act
        var dto = OverviewMapper.Map(snapshot, BuiltAt + TimeSpan.FromSeconds(12), 3);

        // Assert
        dto.Stale.Should().BeTrue();
        dto.SnapshotAgeMs.Should().Be(12000);
    }

    [Fact]
    public void OverviewMapper_NegativeAgeClampedToZero()
    {
        // Arrange: BuiltAtUtc в будущем (скачок часов) — возраст не отрицательный.
        var snapshot = TestSnapshots.Healthy(BuiltAt);

        // Act
        var dto = OverviewMapper.Map(snapshot, BuiltAt - TimeSpan.FromSeconds(5), 3);

        // Assert
        dto.SnapshotAgeMs.Should().Be(0);
    }

    [Fact]
    public void OverviewMapper_ClustersAndMovesFilled()
    {
        // Arrange: MovingCluster — 2 шарда (1 без master), 3 переезда (spec §10.3).
        var snapshot = TestSnapshots.Healthy(BuiltAt) with
        {
            Clusters = [TestSnapshots.MovingCluster(BuiltAt)],
        };

        // Act
        var dto = OverviewMapper.Map(snapshot, BuiltAt + TimeSpan.FromSeconds(1), 3);

        // Assert: заглушки t04 наполнены (spec §3.15 t04 → §1 t05).
        var cluster = dto.Clusters.Should().ContainSingle().Subject;
        cluster.Name.Should().Be("demo");
        cluster.Shards.Should().Be(2);
        cluster.Buckets.Should().Be(16);
        cluster.ActiveMoves.Should().Be(3);
        cluster.MasterlessShards.Should().Be(1);
        dto.ActiveMoves.Should().HaveCount(3);
        dto.ActiveMoves.Should().Contain(m => m.Cluster == "demo" && m.Bucket == 1
            && m.State == "SYNCING" && m.Owner == "s1" && m.Target == "s2");
    }

    [Fact]
    public void OverviewMapper_MovesAcrossClusters_Ordered()
    {
        // Arrange: два кластера — порядок кластеров снапшота, внутри по bucket id (spec §3.6).
        var snapshot = TestSnapshots.Healthy(BuiltAt) with
        {
            Clusters =
            [
                TestSnapshots.MovingCluster(BuiltAt),
                TestSnapshots.FullCluster() with
                {
                    Name = "other",
                    Buckets =
                    [
                        new BucketInfo(7, "s2", BucketState.Syncing,
                            new MoveInfo("s2", "s1", null, BuiltAt.ToUnixTimeSeconds() - 5, null, null)),
                        new BucketInfo(0, "s1", BucketState.Aborting,
                            new MoveInfo("s1", "s2", null, null, null, null)),
                    ],
                },
            ],
        };

        // Act
        var dto = OverviewMapper.Map(snapshot, BuiltAt, 3);

        // Assert: id по возрастанию внутри кластера; state-строки канона; nullable-поля как есть.
        string.Join("|", dto.ActiveMoves.Select(m => $"{m.Cluster}/{m.Bucket}"))
            .Should().Be("demo/1|demo/2|demo/3|other/0|other/7");
        dto.ActiveMoves[4].State.Should().Be("SYNCING");
        dto.ActiveMoves[4].UpdatedUnix.Should().Be(BuiltAt.ToUnixTimeSeconds() - 5);
        dto.ActiveMoves[3].UpdatedUnix.Should().BeNull();
    }

    [Fact]
    public void Map_NotInitializedCluster_StateNodesAndRequests()
    {
        // Arrange: снапшот с HaScope-заявкой (join по <C>-<X>)
        var shard = new ShardInfo("shard1", "", [], null, null, null, 2, null,
            [new NodeInfo("shard1a", "NOT_INITIALIZED")], null);
        var cluster = new ClusterInfo("fresh", "fresh", 1, null, ClusterState.NotInitialized,
            [shard], [new BucketInfo(0, "shard1", BucketState.NotInitialized, null)], []);
        var scopes = new List<HaScope>
        {
            new("fresh-shard1", "fresh", "shard1", true, null, null, false,
                "2", "8Gi", "100Gi", [], null),
        };

        // Act
        var dto = ClusterDetailsMapper.Map(cluster, nowUnix: 100, null, null, [], scopes);

        // Assert
        dto.State.Should().Be("NOT_INITIALIZED");
        dto.Shards.Single().Nodes.Single().Name.Should().Be("shard1a");
        var requests = dto.Shards.Single().Requests.Should().NotBeNull().And.Subject.As<NodeRequestsDto>();
        requests.Cpu.Should().Be("2");
        requests.Mem.Should().Be("8Gi");
        dto.Buckets.Single().State.Should().Be("NOT_INITIALIZED");
    }

    [Fact]
    public void Map_NotInitializedCluster_ZeroMasterlessAndNotInActiveMovesList()
    {
        // Arrange: 2 бакета без мастера, бакеты NOT_INITIALIZED
        var shard = new ShardInfo("shard1", "", [], null, null, null, 1, null, [], null);
        var cluster = new ClusterInfo("fresh", "fresh", 2, null, ClusterState.NotInitialized,
            [shard],
            [new BucketInfo(0, "shard1", BucketState.NotInitialized, null),
             new BucketInfo(1, "shard1", BucketState.NotInitialized, null)], []);
        var snapshot = TestSnapshots.Healthy(DateTimeOffset.UnixEpoch) with { Clusters = [cluster] };

        // Act
        var dto = OverviewMapper.Map(snapshot, DateTimeOffset.UnixEpoch, 3);

        // Assert: masterless=0 (ожидаемо), notInitialized=true; в activeMoves не попали
        dto.Clusters.Single().MasterlessShards.Should().Be(0);
        dto.Clusters.Single().NotInitialized.Should().BeTrue();
        dto.ActiveMoves.Should().BeEmpty();
    }

    [Fact]
    public void EtcdStatusMapper_ActiveFlag_OnlyForActiveEndpoint()
    {
        // Arrange: ActiveEndpoint = etcd1, endpoints etcd1..etcd3.
        var etcd = TestSnapshots.HealthyEtcd(BuiltAt);

        // Act
        var dto = EtcdStatusMapper.Map(etcd);

        // Assert
        dto.Endpoints.Should().HaveCount(3);
        dto.Endpoints.Should().OnlyContain(e => e.Active == (e.Url == "http://etcd1:2379"));
    }

    [Fact]
    public void EtcdStatusMapper_IsLeader_ByLeaderMemberIdOfAliveEndpoint()
    {
        // Arrange: лидер 42; member 42 и member 43.
        var etcd = TestSnapshots.HealthyEtcd(BuiltAt) with
        {
            Members =
            [
                new EtcdMember(42, "etcd1", ["http://p1"], ["http://c1"]),
                new EtcdMember(43, "etcd2", ["http://p2"], ["http://c2"]),
            ],
        };

        // Act
        var dto = EtcdStatusMapper.Map(etcd);

        // Assert: isLeader по совпадению id со статусом leader (arch/02 §2.4).
        dto.Members.Should().HaveCount(2);
        dto.Members.Single(m => m.Id == "42").IsLeader.Should().BeTrue();
        dto.Members.Single(m => m.Id == "43").IsLeader.Should().BeFalse();
    }

    [Fact]
    public void EtcdStatusMapper_IsLeader_FallsBackToDeadEndpointLeader()
    {
        // Arrange: живых нет; у первого (неживого) endpoint'а leader остался (spec §3.14).
        var etcd = TestSnapshots.HealthyEtcd(BuiltAt) with
        {
            Endpoints =
            [
                new EtcdEndpoint("http://etcd1:2379", false, null, null, null, 42, null, null, ["timeout"]),
                new EtcdEndpoint("http://etcd2:2379", false, null, null, null, null, null, null, ["timeout"]),
            ],
            Members =
            [
                new EtcdMember(42, "etcd1", ["http://p1"], ["http://c1"]),
                new EtcdMember(43, "etcd2", ["http://p2"], ["http://c2"]),
            ],
        };

        // Act
        var dto = EtcdStatusMapper.Map(etcd);

        // Assert: лидер определён по fallback — неживому endpoint'у с валидным leader.
        dto.Members.Single(m => m.Id == "42").IsLeader.Should().BeTrue();
        dto.Members.Single(m => m.Id == "43").IsLeader.Should().BeFalse();
    }

    [Fact]
    public void EtcdStatusMapper_IsLeader_NoLeaderAnywhere_AllFalse()
    {
        // Arrange: ни у одного endpoint'а нет leader (нет кворума — arch/01 §8).
        var etcd = TestSnapshots.HealthyEtcd(BuiltAt) with
        {
            Endpoints =
            [
                new EtcdEndpoint("http://etcd1:2379", true, 3.0, "3.5.21", 20480, null, 17, 3, []),
                new EtcdEndpoint("http://etcd2:2379", true, 4.0, "3.5.21", 20480, null, 17, 3, []),
            ],
            Members =
            [
                new EtcdMember(42, "etcd1", ["http://p1"], ["http://c1"]),
                new EtcdMember(43, "etcd2", ["http://p2"], ["http://c2"]),
            ],
        };

        // Act
        var dto = EtcdStatusMapper.Map(etcd);

        // Assert: лидер не определён — все IsLeader=false.
        dto.Members.Should().HaveCount(2).And.OnlyContain(m => !m.IsLeader);
    }

    [Fact]
    public void EtcdStatusMapper_MapsAlarmsQuorumLastRefresh()
    {
        // Arrange: мёртвый endpoint без leader → лидер не определён; alarm NOSPACE.
        var etcd = TestSnapshots.HealthyEtcd(BuiltAt) with
        {
            Alarms = [new EtcdAlarm(42, EtcdAlarmType.NoSpace)],
            QuorumSuspected = true,
        };

        // Act
        var dto = EtcdStatusMapper.Map(etcd);

        // Assert
        dto.Alarms.Should().ContainSingle().Which.Type.Should().Be("nospace");
        dto.QuorumSuspected.Should().BeTrue();
        dto.LastRefreshUtc.Should().Be(BuiltAt);
    }

    [Fact]
    public void AlertsMapper_SeverityLowercaseStrings()
    {
        // Arrange
        var alerts = new List<Alert>
        {
            new("a:1", AlertSeverity.Critical, "a", "1", "m", null, null),
            new("b:1", AlertSeverity.Warning, "b", "1", "m", null, null),
            new("c:1", AlertSeverity.Info, "c", "1", "m", null, null),
        };

        // Act
        var dto = AlertsMapper.Map(alerts);

        // Assert: строчный канон arch/03 §1 (spec §3.11).
        dto.Select(a => a.Severity).Should().Equal("critical", "warning", "info");
    }

    [Fact]
    public void AlertsMapper_PassesDetailsAndSinceUnix()
    {
        // Arrange
        var alert = new Alert(
            "k:t", AlertSeverity.Warning, "k", "t", "msg",
            new Dictionary<string, string> { ["reason"] = "битый JSON" }, 1755800000);

        // Act
        var dto = AlertsMapper.Map([alert]).Single();

        // Assert
        dto.Id.Should().Be("k:t");
        dto.Message.Should().Be("msg");
        dto.Details!["reason"].Should().Be("битый JSON");
        dto.SinceUnix.Should().Be(1755800000);
    }

    [Fact]
    public void AlertsMapper_Filters_SeverityKindBoth()
    {
        // Arrange
        var alerts = new List<Alert>
        {
            new("a:1", AlertSeverity.Critical, "a", "1", "m", null, null),
            new("b:1", AlertSeverity.Warning, "b", "1", "m", null, null),
            new("c:1", AlertSeverity.Warning, "c", "1", "m", null, null),
        };

        // Act / Assert
        AlertsMapper.ApplyFilters(alerts, AlertSeverity.Warning, null)
            .Should().HaveCount(2);
        AlertsMapper.ApplyFilters(alerts, null, "b")
            .Should().ContainSingle().Which.Kind.Should().Be("b");
        AlertsMapper.ApplyFilters(alerts, AlertSeverity.Warning, "c")
            .Should().ContainSingle().Which.Kind.Should().Be("c");
        AlertsMapper.ApplyFilters(alerts, null, null)
            .Should().HaveCount(3);
    }
}
