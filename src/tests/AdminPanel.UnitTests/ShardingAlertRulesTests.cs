using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Правила шардирования каталога 03 §4 (spec §4.2–4.3): напрямую на снапшот-фикстурах;
// механику двигателя (id/sinceUnix/сортировка) проверяет AlertEngineTests.
public class ShardingAlertRulesTests
{
    private static readonly DateTimeOffset Now = new(2026, 8, 22, 12, 0, 0, TimeSpan.Zero);
    private static readonly long NowUnix = Now.ToUnixTimeSeconds();

    // Дефолты каталога (600/60) — общий аргумент пороговых правил (Task 3).
    private static readonly IOptions<AlertsOptions> DefaultOptions = Options.Create(new AlertsOptions());

    // Оценка одного правила на снапшоте (spec §10.1).
    private static IReadOnlyList<Alert> Evaluate(IAlertRule rule, EtcdSnapshot snapshot)
        => [.. rule.Evaluate(snapshot, new AlertContext(null, Now, 3))];

    // Снапшот с заданными кластерами поверх здорового etcd-базиса.
    private static EtcdSnapshot Snapshot(params ClusterInfo[] clusters)
        => TestSnapshots.Healthy(Now) with { Clusters = [.. clusters] };

    [Fact]
    public void ShardNoMaster_MissingMasterWithDsn_Critical()
    {
        // Arrange: MovingCluster — s2 без master-ключа при живом dsn (P11).
        var rule = new ShardNoMasterRule();

        // Act
        var alerts = Evaluate(rule, Snapshot(TestSnapshots.MovingCluster(Now)));
        var clean = Evaluate(rule, Snapshot(TestSnapshots.FullCluster()));

        // Assert
        clean.Should().BeEmpty();
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Id.Should().Be("shard-no-master:demo/s2");
        alert.Details!["dsn"].Should().Contain("host=s2a");
    }

    [Fact]
    public void ShardNoMaster_IgnoredWhenNoDsn()
    {
        // Arrange: шард без dsn-ключа — писателя нет, ожидание lease неуместно (spec §4.2).
        var cluster = TestSnapshots.FullCluster() with
        {
            Shards = [new ShardInfo("s1", "", [], null, null, null, null, null, [], null)],
        };

        // Act
        var alerts = Evaluate(new ShardNoMasterRule(), Snapshot(cluster));

        // Assert
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void MoveAborting_AnyAborting_Warning()
    {
        // Arrange / Act: ABORTING безусловно, даже свежий (P7).
        var alerts = Evaluate(new MoveAbortingRule(), Snapshot(TestSnapshots.MovingCluster(Now)));

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Id.Should().Be("move-aborting:demo/bucket_3");
        alert.Details!["phase"].Should().Be("cleanup");
        alert.Details["lastError"].Should().Be("receiver went away");
        alert.Details["ageSeconds"].Should().Be("5");
    }

    [Fact]
    public void MoveFlipped_RoutingEqualsTarget_Warning()
    {
        // Arrange: routing уже указывает на target, но статус-ключ не снят (P7).
        var cluster = TestSnapshots.FullCluster() with
        {
            Buckets =
            [
                new BucketInfo(5, "s2", BucketState.Syncing,
                    new MoveInfo("s1", "s2", NowUnix - 100, NowUnix - 100, "copy", null)),
                new BucketInfo(6, "s1", BucketState.Syncing,
                    new MoveInfo("s1", "s2", NowUnix - 100, NowUnix - 100, "copy", null)),
            ],
        };

        // Act
        var alerts = Evaluate(new MoveFlippedStatusStuckRule(), Snapshot(cluster));

        // Assert: только бакет 5 — owner уже = target; бакет 6 — переезд ещё идёт.
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Id.Should().Be("move-flipped-status-stuck:demo/bucket_5");
        alert.Details!["owner"].Should().Be("s2");
        alert.Details["target"].Should().Be("s2");
    }

    [Fact]
    public void BucketLost_OwnerUnknownShard_Critical()
    {
        // Arrange: routing указывает на шард, которого нет (P23-а).
        var cluster = TestSnapshots.FullCluster() with
        {
            Buckets =
            [
                new BucketInfo(0, "s9", BucketState.Active, null),
                new BucketInfo(1, "s1", BucketState.Active, null),
            ],
        };

        // Act
        var alerts = Evaluate(new BucketLostRule(), Snapshot(cluster));

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Id.Should().Be("bucket-lost:demo/bucket_0");
        alert.Details!["owner"].Should().Be("s9");
    }

    [Fact]
    public void BucketNoRouting_HoleInRange_Warning()
    {
        // Arrange: бакет 5 из 0..15 без routing — дыра карты; вне диапазона и incomplete — не дыры.
        var holey = TestSnapshots.FullCluster() with
        {
            Buckets =
            [
                .. Enumerable.Range(0, 16).Select(i => new BucketInfo(i, i == 5 ? null : "s1", BucketState.Active, null)),
                new BucketInfo(99, null, BucketState.Active, null),
            ],
        };

        // Act
        var alerts = Evaluate(new BucketNoRoutingRule(), Snapshot(holey, TestSnapshots.GhostCluster()));

        // Assert: одна дыра 0..15; bucket_99 вне диапазона; у ghost (N=0) диапазон пуст (spec §3.13).
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Id.Should().Be("bucket-no-routing:demo/bucket_5");
        alert.Details!["bucketsCount"].Should().Be("16");
    }

    [Fact]
    public void BucketOutOfRange_RoutingBeyondN_Warning()
    {
        // Arrange: routing bucket_99 при N=16 (P18); в диапазоне и incomplete — чисто.
        var withExtra = TestSnapshots.FullCluster() with
        {
            Buckets =
            [
                .. Enumerable.Range(0, 16).Select(i => new BucketInfo(i, "s1", BucketState.Active, null)),
                new BucketInfo(99, "s1", BucketState.Active, null),
            ],
        };

        // Act
        var alerts = Evaluate(new BucketOutOfRangeRule(), Snapshot(withExtra, TestSnapshots.GhostCluster()));

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Id.Should().Be("bucket-out-of-range:demo/bucket_99");
        alert.Details!["bucketId"].Should().Be("99");
    }

    [Fact]
    public void Rules_TargetsContainClusterName()
    {
        // Arrange: одна и та же аномалия в двух кластерах — алерты различаются таргетом (spec §4.2).
        var a = TestSnapshots.FullCluster() with
        {
            Buckets = [new BucketInfo(0, "s9", BucketState.Active, null)],
        };
        var b = a with { Name = "other" };

        // Act
        var alerts = Evaluate(new BucketLostRule(), Snapshot(a, b));

        // Assert
        alerts.Should().HaveCount(2);
        alerts.Should().Contain(x => x.Target == "demo/bucket_0");
        alerts.Should().Contain(x => x.Target == "other/bucket_0");
    }

    [Fact]
    public void MoveStale_OlderThanThreshold_Warning()
    {
        // Arrange: 601 c — есть; ровно 600 и 599 — нет: порог каталога 600 (03 §4).
        var stale = TestSnapshots.FullCluster() with
        {
            Buckets =
            [
                new BucketInfo(3, "s1", BucketState.Syncing,
                    new MoveInfo("s1", "s2", NowUnix - 700, NowUnix - 601, "copy", null)),
                new BucketInfo(4, "s1", BucketState.Syncing,
                    new MoveInfo("s1", "s2", NowUnix - 700, NowUnix - 600, "copy", null)),
                new BucketInfo(5, "s1", BucketState.Syncing,
                    new MoveInfo("s1", "s2", NowUnix - 700, NowUnix - 599, "copy", null)),
            ],
        };

        // Act
        var alerts = Evaluate(new MoveStaleRule(DefaultOptions), Snapshot(stale));

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Id.Should().Be("move-stale:demo/bucket_3");
        alert.Details!["state"].Should().Be("SYNCING");
        alert.Details["ageSeconds"].Should().Be("601");
        alert.Details["thresholdSeconds"].Should().Be("600");
        alert.Details["updatedUnix"].Should().Be((NowUnix - 601).ToString());
    }

    [Fact]
    public void MoveStale_CustomThreshold_FromOptions()
    {
        // Arrange: порог реально читается из AdminPanel:Alerts (spec §3.11): 5 c вместо 600.
        var snapshot = Snapshot(TestSnapshots.FullCluster() with
        {
            Buckets =
            [
                new BucketInfo(3, "s1", BucketState.Syncing,
                    new MoveInfo("s1", "s2", NowUnix - 10, NowUnix - 6, "copy", null)),
            ],
        });

        // Act
        var custom = Evaluate(
            new MoveStaleRule(Options.Create(new AlertsOptions { StaleMoveSeconds = 5 })), snapshot);

        // Assert: возраст 6 > порога 5 — алерт с фактическим порогом в details.
        var alert = custom.Should().ContainSingle().Subject;
        alert.Details!["thresholdSeconds"].Should().Be("5");
    }

    [Fact]
    public void MoveStale_FallsBackToStartedUnix()
    {
        // Arrange: updated отсутствует — база started (spec §3.7).
        var snapshot = Snapshot(TestSnapshots.FullCluster() with
        {
            Buckets =
            [
                new BucketInfo(3, "s1", BucketState.Syncing,
                    new MoveInfo("s1", "s2", NowUnix - 700, null, "copy", null)),
            ],
        });

        // Act / Assert
        Evaluate(new MoveStaleRule(DefaultOptions), snapshot)
            .Should().ContainSingle().Which.Details!["updatedUnix"].Should().Be((NowUnix - 700).ToString());
    }

    [Fact]
    public void MoveStale_NoTimestamps_Skipped()
    {
        // Arrange: оба штампа отсутствуют — меры возраста нет, правило молчит (spec §4.2).
        var snapshot = Snapshot(TestSnapshots.FullCluster() with
        {
            Buckets = [new BucketInfo(3, "s1", BucketState.Syncing, new MoveInfo("s1", "s2", null, null, null, null))],
        });

        // Act / Assert
        Evaluate(new MoveStaleRule(DefaultOptions), snapshot).Should().BeEmpty();
    }

    [Fact]
    public void MoveFrozenLong_FrozenOlderThan60s_Critical()
    {
        // Arrange: FROZEN 61 c — порог 60 (cutover секундами, 03 §4); 59 c — чисто.
        var frozen = TestSnapshots.FullCluster() with
        {
            Buckets =
            [
                new BucketInfo(2, "s1", BucketState.Frozen,
                    new MoveInfo("s1", "s2", NowUnix - 100, NowUnix - 61, "cutover-wait", null)),
                new BucketInfo(8, "s1", BucketState.Frozen,
                    new MoveInfo("s1", "s2", NowUnix - 100, NowUnix - 59, "cutover-wait", null)),
            ],
        };

        // Act
        var alerts = Evaluate(new MoveFrozenLongRule(DefaultOptions), Snapshot(frozen));

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Id.Should().Be("move-frozen-long:demo/bucket_2");
        alert.Details!["ageSeconds"].Should().Be("61");
        alert.Details["thresholdSeconds"].Should().Be("60");
    }

    [Fact]
    public void ShardingScenario_AllFourAnomalies_ThroughFullEngine()
    {
        // Arrange: сценарий roadmap — протухший lease + зависший FROZEN + routing в никуда + дыра карты.
        var cluster = new ClusterInfo(
            "demo", "demo", 4, 1755800000, ClusterState.Active,
            [new ShardInfo("s1", "host=s1a port=5432 dbname=demo user=postgres",
                ["s1a"], 5432, "demo", "postgres", 1, null, [], null)],
            [
                new BucketInfo(0, "s1", BucketState.Active, null),
                new BucketInfo(1, null, BucketState.Active, null),
                new BucketInfo(2, "s9", BucketState.Active, null),
                new BucketInfo(3, "s1", BucketState.Frozen,
                    new MoveInfo("s1", "s2", NowUnix - 500, NowUnix - 100, "cutover-wait", null)),
            ],
            []);
        var snapshot = TestSnapshots.Healthy(Now) with { Clusters = [cluster] };
        var engine = new AlertEngine(AlertTestRules.All());

        // Act: previous без этого id → sinceUnix = unix оценки (механика t04 §3.4).
        var alerts = engine.Evaluate(snapshot, snapshot with { Alerts = [] }, Now, 3);

        // Assert: 3 critical (no-master, frozen-long, lost) + 1 warning (no-routing),
        // сортировка severity → kind (Ordinal); etcd-правила на здоровом базисе молчат.
        string.Join("|", alerts.Select(a => a.Kind))
            .Should().Be("bucket-lost|move-frozen-long|shard-no-master|bucket-no-routing");
        alerts.Should().OnlyContain(a => a.SinceUnix == NowUnix);
    }
}
