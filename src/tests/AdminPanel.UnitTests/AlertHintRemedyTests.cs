using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using AdminPanel.Core.Kafka;
using AdminPanel.Core.Kafka.KafkaAlerting;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Инвариант arch/03 §4.1 (task etcd-via-worker-api): КАЖДЫЙ kind каталога
// (pg + kafka) несёт непустые Hint/Remedy/RemedyText. Перечень kinds сверяется
// с прогоном движков по «каталожному» снапшоту с аномалиями всех видов.
public class AlertHintRemedyTests
{
    private static readonly DateTimeOffset Now = new(2026, 8, 31, 12, 0, 0, TimeSpan.Zero);
    private static readonly long NowUnix = Now.ToUnixTimeSeconds();

    // Полный каталог kinds pg-грани (arch/03 §4 + §4.1).
    private static readonly string[] PgKinds =
    [
        "bucket-lost", "bucket-no-routing", "bucket-out-of-range",
        "cluster-incomplete", "cluster-not-initialized",
        "etcd-alarm", "etcd-endpoint-down", "etcd-no-quorum", "etcd-unreachable",
        "ha-member-not-streaming", "inventory-mismatch", "key-malformed",
        "move-aborting", "move-flipped-status-stuck", "move-frozen-long", "move-stale",
        "probe-failed", "replica-lag-high",
        "shard-no-leader", "shard-no-master",
        "slot-invalidation-risk", "slot-lag-high", "slot-wal-lost",
        "snapshot-stale", "sync-standby-missing",
        "worker-api-unreachable",
    ];

    // Полный каталог kinds kafka-грани (arch/03 §7.4 + §4.1).
    private static readonly string[] KafkaKinds =
    [
        "kafka-broker-not-running", "kafka-cluster-not-initialized", "kafka-cluster-to-remove",
        "kafka-desired-stale", "kafka-endpoints-missing", "kafka-group-lag-high",
        "kafka-key-malformed", "kafka-lifecycle-stale", "kafka-reassignment-stale",
        "kafka-rebalance-pending", "kafka-rotation-pending",
        "kafka-topic-create-pending", "kafka-topic-delete-pending",
        "kafka-topic-missing-desired", "kafka-topic-under-replicated",
        "worker-api-unreachable",
    ];

    // «Каталожный» снапшот: по одной аномалии на каждый kind pg-каталога.
    private static EtcdSnapshot CatalogSnapshot()
    {
        var etcd = new EtcdStatus(
            false,
            [
                new EtcdEndpoint("http://etcd1:2379", false, null, null, null, null, null, null, ["timeout"]),
                new EtcdEndpoint("http://etcd2:2379", true, 1, "3.5.21", 1, 1, 1, 1, []),
            ],
            [], [new EtcdAlarm(42, EtcdAlarmType.NoSpace)],
            null, QuorumSuspected: true, Now, ConsecutiveFailures: 2);

        var lost = TestSnapshots.FullCluster() with
        {
            Name = "lost", DbName = "lost",
            Buckets = [new BucketInfo(0, "shard9", BucketState.Active, null)],
        };
        var outOfRange = TestSnapshots.FullCluster() with
        {
            Name = "oob", DbName = "oob", BucketsCount = 2,
            Buckets = [new BucketInfo(5, "s1", BucketState.Active, null)],
        };
        var runtime = TestSnapshots.FullCluster() with
        {
            Name = "runtime", DbName = "runtime",
            Shards =
            [
                TestSnapshots.FullCluster().Shards[0] with
                {
                    Runtime = TestSnapshots.ShardRuntimeOf("s1") with
                    {
                        BucketSchemas = ["bucket_0"],
                        Slots =
                        [
                            new ReplicationSlotInfo("move_bucket_3", "logical", true, "lost", 100L, 0L),
                            new ReplicationSlotInfo("move_bucket_7", "logical", true, "streaming", null, 512L * 1024 * 1024),
                        ],
                    },
                },
            ],
        };
        var notInitialized = TestSnapshots.FullCluster() with
        {
            Name = "fresh", DbName = "fresh", State = ClusterState.NotInitialized,
        };

        // Двигательная аномалия: stale-переезд (updated −700 c), FROZEN −70 c,
        // ABORTING безусловно, flipped (routing уже = target).
        var moving = TestSnapshots.MovingCluster(Now) with { Buckets = [.. TestSnapshots.MovingCluster(Now).Buckets] };
        moving = moving with
        {
            Buckets =
            [
                .. moving.Buckets,
                new BucketInfo(5, "s1", BucketState.Syncing, new MoveInfo("s1", "s2", NowUnix - 750, NowUnix - 700, "copy", null)),
                new BucketInfo(6, "s2", BucketState.Syncing, new MoveInfo("s1", "s2", NowUnix - 50, NowUnix - 10, "copy", null)),
                new BucketInfo(7, "s1", BucketState.Frozen, new MoveInfo("s1", "s2", NowUnix - 150, NowUnix - 100, "cutover-wait", null)),
            ],
        };

        return TestSnapshots.Healthy(Now) with
        {
            BuiltAtUtc = Now - TimeSpan.FromSeconds(100), // snapshot-stale (порог 3×3 c)
            Etcd = etcd,
            Clusters =
            [
                TestSnapshots.GhostCluster(),          // cluster-incomplete
                notInitialized,                        // cluster-not-initialized
                moving,                                // move-*/bucket-no-routing/shard-no-master
                lost,                                  // bucket-lost
                outOfRange,                            // bucket-out-of-range
                runtime,                               // slot-*/sync-standby/inventory
            ],
            HaScopes =
            [
                // not-streaming реплика (+лаг 17 MiB > 16 MiB порога) → оба kind
                new HaScope("runtime-s1", "runtime", "s1", true, "s1a", 738273634528L, true, null, null, null,
                [
                    new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, Now, null, null),
                    new HaMember("s1b", "s1b", 5432, "replica", "stopped", 1L, 17L * 1024 * 1024, Now, null, null),
                ], null),
                // matched-скоп без leader-ключа → shard-no-leader
                new HaScope("moving-s2", "moving", "s2", true, null, null, true, null, null, null, [], null),
            ],
            Probes = [new ProbeResult("moving-s1/s1a", "patroni", false, 5.0, "connection refused", Now)],
            ParseErrors = [new KeyParseError("/clusters/demo/buckets/status/bucket_9", "битый JSON")],
            PgWorkerEndpoints = [], // worker-api-unreachable (pg-грань)
        };
    }

    [Fact]
    public void EveryPgAlert_Kind_HasHintAndRemedy()
    {
        // Arrange — все правила каталога, каталожный снапшот аномалий
        var engine = new AlertEngine(AlertTestRules.All());

        // Act
        var alerts = engine.Evaluate(CatalogSnapshot(), previous: null, Now, refreshIntervalSeconds: 3);

        // Assert: покрыты ВСЕ kinds каталога (потеря kind'а — регресс) и каждый
        // алерт несёт непустые Hint/RemedyText (инвариант arch/03 §4.1).
        var kinds = alerts.Select(a => a.Kind).ToHashSet();
        kinds.Should().BeEquivalentTo(PgKinds, $"покрыты: {string.Join(", ", kinds.OrderBy(k => k))}");
        alerts.Should().NotBeEmpty();
        alerts.Should().OnlyContain(a => !string.IsNullOrWhiteSpace(a.Hint)
            && !string.IsNullOrWhiteSpace(a.RemedyText));
    }

    [Fact]
    public void WorkerApiUnreachable_Pg_CriticalWhenNoKeys()
    {
        // Arrange
        var engine = new AlertEngine([new WorkerApiUnreachableRule()]);

        // Act — ключи есть: алерта нет; ключей нет: critical
        var withKeys = engine.Evaluate(
            TestSnapshots.Healthy(Now) with
            {
                PgWorkerEndpoints = [new WorkerEndpoint("i1", "http://pgw:8080", NowUnix)],
            }, null, Now, 3);
        var withoutKeys = engine.Evaluate(TestSnapshots.Healthy(Now) with { PgWorkerEndpoints = [] }, null, Now, 3);

        // Assert
        withKeys.Should().BeEmpty();
        var alert = withoutKeys.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Kind.Should().Be("worker-api-unreachable");
        alert.Target.Should().Be("pgworker");
        alert.Hint.Should().Contain("lease-ключ");
    }

    [Fact]
    public void EveryKafkaAlert_Kind_HasHintAndRemedy()
    {
        // Arrange — каталожный kafka-снапшот: по одной аномалии на каждый kind
        var engine = new KafkaAlertEngine(Options.Create(new KafkaAlertsOptions()));
        var active = new KafkaClusterInfo(
            "events", KafkaClusterState.Active, 1, 1, 1, 12, 604800000, 1756500000,
            "host.docker.internal:16001",
            [new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40)],
            [
                new KafkaTopicInfo("ghost", 3, 1, 604800000, 1,
                    new TopicDesiredDto(null, 86400000, null, NowUnix - 30, "admin"), true, 1756500900, null),
                new KafkaTopicInfo("orders", 3, 1, 604800000, 1,
                    new TopicDesiredDto(6, null, null, NowUnix - 601, "admin"), false, 1756500900, null),
                new KafkaTopicInfo("payments", 3, 1, 604800000, 1, null, false, 1756500900, 5),
            ],
            LifecycleTickets:
            [
                new KafkaTopicLifecycleTicket("audit", "create", 12, 3, null, null, NowUnix - 30, "admin"),
                new KafkaTopicLifecycleTicket("legacy", "delete", null, null, null, null, NowUnix - 30, "admin"),
                new KafkaTopicLifecycleTicket("stuck", "create", 12, 3, null, null, NowUnix - 601, "admin"),
            ],
            Groups: [new KafkaGroupInfo("cg", "Stable", 2, 200000)]);
        var pending = new KafkaClusterInfo(
            "pending", KafkaClusterState.NotInitialized, 3, 3, 2, 12, 604800000, 1756500000, null, [], []);
        var removing = new KafkaClusterInfo(
            "dying", KafkaClusterState.ToRemove, 3, 3, 2, 12, 604800000, 1756500000, null, [], []);
        var noEndpoints = new KafkaClusterInfo(
            "blind", KafkaClusterState.Active, 1, 1, 1, 12, 604800000, 1756500000, null,
            [new KafkaBrokerInfo("broker1", "UNREACHABLE", "controller", 2m, 4, 40)], []);
        var snapshot = new KafkaSnapshot(
            Now, EtcdReachable: true, ConsecutiveFailures: 0,
            [active, pending, removing, noEndpoints],
            Rotations: [new KafkaRotationTicket("events", NowUnix - 10, "ops")],
            Rebalances: [new KafkaRebalanceTicket("events", NowUnix - 10, "ops")],
            Reassignments: [new KafkaReassignmentProgress("events", "drain", "broker1", 10, 10, NowUnix - 1000, null)],
            WorkerEndpoints: [], // worker-api-unreachable (kafka-грань)
            Probes: [], Alerts: [],
            ParseErrors: [new KeyParseError("/kafka/clusters/x/config", "bad")],
            UnknownKeyCount: 0);

        // Act
        var alerts = engine.Evaluate(snapshot, previous: null);

        // Assert: покрыты все kinds kafka-каталога; Hint/RemedyText непустые.
        var kinds = alerts.Select(a => a.Kind).ToHashSet();
        kinds.Should().BeEquivalentTo(KafkaKinds, $"покрыты: {string.Join(", ", kinds.OrderBy(k => k))}");
        alerts.Should().OnlyContain(a => !string.IsNullOrWhiteSpace(a.Hint)
            && !string.IsNullOrWhiteSpace(a.RemedyText));
        alerts.Should().Contain(a => a.Kind == "worker-api-unreachable" && a.Target == "kafkaworker"
            && a.Severity == AlertSeverity.Critical);
    }
}
