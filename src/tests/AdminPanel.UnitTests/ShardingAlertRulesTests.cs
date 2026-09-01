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

    [Fact]
    public void ClusterNotInitialized_Rule_FiresInfoAlert()
    {
        // Arrange
        var cluster = new ClusterInfo("fresh", "fresh", 1, null, ClusterState.NotInitialized, [], [], []);

        // Act
        var alerts = Evaluate(new ClusterNotInitializedRule(DefaultOptions), Snapshot(cluster));

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Info);
        alert.Kind.Should().Be("cluster-not-initialized");
        alert.Target.Should().Be("fresh");
    }

    // AAA: D2 — молодой NOT_INITIALIZED-кластер: info (нормальный жизненный цикл)
    [Fact]
    public void ClusterNotInitialized_YoungCluster_InfoAlert()
    {
        // Arrange: кластер заявлен 100 c назад (created_unix = NowUnix-100).
        var rule = new ClusterNotInitializedRule(Options.Create(new AlertsOptions { NotInitializedWarnSec = 900 }));
        var cluster = TestSnapshots.FullCluster() with
        {
            State = ClusterState.NotInitialized,
            CreatedUnix = NowUnix - 100,
        };

        // Act
        var alerts = Evaluate(rule, Snapshot(cluster));

        // Assert: молодой — info (нормальный жизненный цикл).
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Info);
    }

    // AAA: D2 — NOT_INITIALIZED дольше порога — эскалация до Warning
    [Fact]
    public void ClusterNotInitialized_StuckCluster_EscalatesToWarning()
    {
        // Arrange: NOT_INITIALIZED дольше порога 900 c (больше PatroniBootSec воркера 600).
        var rule = new ClusterNotInitializedRule(Options.Create(new AlertsOptions { NotInitializedWarnSec = 900 }));
        var cluster = TestSnapshots.FullCluster() with
        {
            State = ClusterState.NotInitialized,
            CreatedUnix = NowUnix - 901,
        };

        // Act
        var alerts = Evaluate(rule, Snapshot(cluster));

        // Assert: эскалация по возрасту.
        alerts.Single().Severity.Should().Be(AlertSeverity.Warning);
    }

    // AAA: D2 — created_unix отсутствует (старые init): fallback на возраст
    // previous-алерта по sinceUnix
    [Fact]
    public void ClusterNotInitialized_NoCreatedUnix_FallsBackToAlertAge()
    {
        // Arrange: created_unix отсутствует (старые init) — возраст по sinceUnix previous-алерта.
        var rule = new ClusterNotInitializedRule(Options.Create(new AlertsOptions { NotInitializedWarnSec = 900 }));
        var cluster = TestSnapshots.FullCluster() with { State = ClusterState.NotInitialized, CreatedUnix = null };
        var previous = Snapshot(cluster) with
        {
            Alerts = [new Alert("cluster-not-initialized:demo", AlertSeverity.Info, "cluster-not-initialized",
                "demo", "…", null, NowUnix - 1000, "hint", AlertRemedy.WorkerAuto, "remedy")],
        };
        var context = new AlertContext(previous, Now, 3);

        // Act
        var alerts = rule.Evaluate(Snapshot(cluster), context).ToList();

        // Assert: previous-алерт старше порога → Warning.
        alerts.Single().Severity.Should().Be(AlertSeverity.Warning);
    }

    // AAA: D3 — живой last_error provision + серия фейлов старше порога:
    // Warning с текстом ошибки воркера и деталями серии
    [Fact]
    public void ProvisionStuck_LiveErrorSeriesOldEnough_WarningWithLastErrorText()
    {
        // Arrange: журнал provision с серией фейлов старше порога 300 c.
        var rule = new ProvisionStuckRule(Options.Create(new AlertsOptions { ProvisionStuckSec = 300 }));
        var snapshot = Snapshot(TestSnapshots.FullCluster() with { State = ClusterState.NotInitialized }) with
        {
            PgWorkerWork = [new WorkJournalInfo("demo", "provision", "shard-provision", "w-1",
                UpdatedUnix: NowUnix - 10, LastError: "Patroni шарда demo-s1 не поднялся за бюджет 600 с",
                FailCount: 3, FailFirstUnix: NowUnix - 400, RetryNotBeforeUnix: NowUnix + 20)],
        };

        // Act
        var alerts = Evaluate(rule, snapshot);

        // Assert: warning с текстом ошибки воркера и деталями серии.
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("provision-stuck");
        alert.Target.Should().Be("demo");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Message.Should().Contain("не поднялся");
        alert.Details!["fail_count"].Should().Be("3");
    }

    // AAA: D3 — свежая серия или живого last_error нет: алерта нет (не мигает)
    [Fact]
    public void ProvisionStuck_FreshSeriesOrNoError_NoAlert()
    {
        // Arrange: (а) серия моложе порога; (б) last_error нет.
        var rule = new ProvisionStuckRule(Options.Create(new AlertsOptions { ProvisionStuckSec = 300 }));
        var fresh = Snapshot(TestSnapshots.FullCluster()) with
        {
            PgWorkerWork = [new WorkJournalInfo("demo", "provision", "planned", "w-1",
                NowUnix - 5, "boom", 1, NowUnix - 5, NowUnix)],
        };
        var healthy = Snapshot(TestSnapshots.FullCluster()) with
        {
            PgWorkerWork = [new WorkJournalInfo("demo", "provision", "waiting-patroni", "w-1",
                NowUnix - 5, null, null, null, null)],
        };

        // Act + Assert
        Evaluate(rule, fresh).Should().BeEmpty();
        Evaluate(rule, healthy).Should().BeEmpty();
    }

    [Fact]
    public void MoveStale_DoesNotFire_ForNotInitializedBuckets()
    {
        // Arrange: NOT_INITIALIZED со штампом старше порога (600 c)
        var cluster = new ClusterInfo("fresh", "fresh", 1, null, ClusterState.Active, [],
            [new BucketInfo(0, "shard1", BucketState.NotInitialized,
                new MoveInfo("shard1", null, null, 1, null, null))], []);

        // Act
        var alerts = Evaluate(new MoveStaleRule(DefaultOptions), Snapshot(cluster));

        // Assert: NOT_INITIALIZED — не переезд (arch/03 §4)
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void ShardNoLeader_DoesNotFire_ForNotInitializedClusterScope()
    {
        // Arrange: matched scope без leader, кластер fresh — NOT_INITIALIZED
        var cluster = new ClusterInfo("fresh", "fresh", 1, null, ClusterState.NotInitialized,
            [new ShardInfo("shard1", "", [], null, null, null, 2, null, [], null)], [], []);
        var scope = new HaScope("fresh-shard1", "fresh", "shard1", true, null, null, false,
            null, null, null, [], null);
        var snapshot = TestSnapshots.Healthy(Now) with { Clusters = [cluster], HaScopes = [scope] };

        // Act
        var alerts = Evaluate(new ShardNoLeaderRule(), snapshot);

        // Assert: лидера нет потому, что ноды не подняты (spec t12 §3.7)
        alerts.Should().BeEmpty();
    }
}
