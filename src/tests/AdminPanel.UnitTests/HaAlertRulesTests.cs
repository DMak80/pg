using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// HA-правила алертов t06 (spec §10.1): источники /service/ и Patroni-проба.
// SQL-правила (slot-*/sync/inventory) — в этом же файле, добавляются следующим таском.
public class HaAlertRulesTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    // Хелпер порогов: имя LagOptions — чтобы не конфликтовать с Microsoft.Extensions.Options.Options.
    private static AlertsOptions LagOptions(long replicaLag = 16L * 1024 * 1024)
        => new() { ReplicaLagBytes = replicaLag };

    [Fact]
    public void ShardNoLeader_MatchedNoLeader_Critical()
    {
        // Arrange: matched-скоп без leader-ключа.
        var snapshot = TestSnapshots.WithHaScopes(Now) with
        {
            HaScopes = [TestSnapshots.HaScopeDemo(Now) with { LeaderName = null }],
        };

        // Act
        var alerts = new ShardNoLeaderRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("shard-no-leader:demo-s1");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Details!.Keys.Should().Contain(["scope", "cluster", "shard"]);
    }

    [Fact]
    public void ShardNoLeader_WithLeader_NoAlert()
    {
        // Arrange — лидер есть.
        var snapshot = TestSnapshots.WithHaScopes(Now);

        // Act
        var alerts = new ShardNoLeaderRule().Evaluate(snapshot, Context()).ToList();

        // Assert: other-scope без лидера не считается — unmatched не алертится (spec §3.10).
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void HaMemberNotStreaming_ReplicaNotStreaming_Warning()
    {
        // Arrange: реплика в starting с успешной пробой.
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members =
                [
                    new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, Now, null, null),
                    new HaMember("s1b", "s1b", 5432, "replica", "starting", 1L, 10L, Now, null, null),
                ],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new HaMemberNotStreamingRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("ha-member-not-streaming:demo-s1/s1b");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Details!["expected"].Should().Be("streaming");
        alert.Details["state"].Should().Be("starting");
    }

    [Fact]
    public void HaMemberNotStreaming_MasterNotRunning_Warning()
    {
        // Arrange: мастер остановился.
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members = [new HaMember("s1a", "s1a", 5432, "master", "stopped", 1L, null, Now, null, null)],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new HaMemberNotStreamingRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        alerts.Should().ContainSingle().Which.Details!["expected"].Should().Be("running");
    }

    [Fact]
    public void HaMemberNotStreaming_UnknownRole_Skipped()
    {
        // Arrange: sync_standby — каталожного ожидания нет (spec §3.13).
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members = [new HaMember("s1c", "s1c", 5432, "sync_standby", "streaming", 1L, 0L, Now, null, null)],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new HaMemberNotStreamingRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void HaMemberNotStreaming_ProbeErrorOrMissing_Skipped()
    {
        // Arrange: в одном matched-скопе — здоровый мастер, член с упавшей пробой
        // (DCS state "crashed", ProbeError задан) и член до первого тика пробы
        // (ProbeAtUtc null): ни один не должен алертиться этим правилом.
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members =
                [
                    new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, Now, null, null),
                    new HaMember("err", "err", 5432, "replica", "crashed", null, null, Now, "connection refused", null),
                    new HaMember("cold", "cold", 5432, "replica", null, null, null, null, null, null),
                ],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new HaMemberNotStreamingRule().Evaluate(snapshot, Context()).ToList();

        // Assert: ошибка пробы — зона probe-failed; без пробы — данных нет (spec §3.13).
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void ReplicaLagHigh_AboveThreshold_Warning()
    {
        // Arrange: лаг s1b = 17 МБ > 16 МБ.
        var snapshot = TestSnapshots.WithHaScopes(Now);
        var rule = new ReplicaLagHighRule(Options.Create(LagOptions()));

        // Act
        var alerts = rule.Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("replica-lag-high:demo-s1/s1b");
        alert.Details!["lagBytes"].Should().Be((17L * 1024 * 1024).ToString());
        alert.Details["thresholdBytes"].Should().Be((16L * 1024 * 1024).ToString());
    }

    [Fact]
    public void ReplicaLagHigh_AtThreshold_NoAlert()
    {
        // Arrange: ровно порог — не «больше» (строгие сравнения каталога).
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members = [new HaMember("s1b", "s1b", 5432, "replica", "streaming", 1L, 16L * 1024 * 1024, Now, null, null)],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new ReplicaLagHighRule(Options.Create(LagOptions())).Evaluate(snapshot, Context()).ToList();

        // Assert
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void ReplicaLagHigh_CustomThreshold_FromOptions()
    {
        // Arrange: порог 100 байт из настроек.
        var snapshot = TestSnapshots.WithHaScopes(Now);
        var rule = new ReplicaLagHighRule(Options.Create(LagOptions(100)));

        // Act
        var alerts = rule.Evaluate(snapshot, Context()).ToList();

        // Assert: лаг 0 мастера не алертится, s1b (17 МБ) — да.
        alerts.Should().ContainSingle().Which.Target.Should().Be("demo-s1/s1b");
    }

    [Fact]
    public void ReplicaLagHigh_NoProbe_Silent()
    {
        // Arrange: член без проб (LagBytes null).
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members = [new HaMember("s1b", "s1b", 5432, "replica", "streaming", null, null, null, null, null)],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new ReplicaLagHighRule(Options.Create(LagOptions())).Evaluate(snapshot, Context()).ToList();

        // Assert: SQL/Patroni-алерты — только при включённых пробах (03 §4).
        alerts.Should().BeEmpty();
    }

    // ==== probe-failed (spec 2026-09-01 §3.1) ====

    [Fact]
    public void ProbeFailed_SqlFailed_Critical()
    {
        // Arrange: Active-кластер, SQL-проба шарда упала (timeout).
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Probes = [new ProbeResult("demo/s1", "sql", false, 4.0, "timeout", Now)],
        };

        // Act
        var alerts = new ProbeFailedRule().Evaluate(snapshot, Context()).ToList();

        // Assert: шард недоступен — critical; details несут ошибку и хосты DSN.
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("probe-failed:sql:demo/s1");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Details!["error"].Should().Be("timeout");
        alert.Details["dsnHosts"].Should().Be("s1a,s1b");
    }

    [Fact]
    public void ProbeFailed_PatroniOneMemberFailed_Warning()
    {
        // Arrange: один из двух членов matched-скопа упал, второй жив.
        var snapshot = TestSnapshots.WithHaScopes(Now) with
        {
            Probes = [new ProbeResult("demo-s1/s1a", "patroni", false, 1.0, "connection refused", Now)],
        };

        // Act
        var alerts = new ProbeFailedRule().Evaluate(snapshot, Context()).ToList();

        // Assert: одиночный член — warning (одна нода ≠ весь кластер).
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("probe-failed:patroni:demo-s1/s1a");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Details!["error"].Should().Be("connection refused");
    }

    [Fact]
    public void ProbeFailed_PatroniAllMembersFailed_SingleCriticalNoWarnings()
    {
        // Arrange: обе Patroni-пробы членов matched-скопа упали.
        var snapshot = TestSnapshots.WithHaScopes(Now) with
        {
            Probes =
            [
                new ProbeResult("demo-s1/s1a", "patroni", false, 1.0, "refused", Now),
                new ProbeResult("demo-s1/s1b", "patroni", false, 2.0, "timeout", Now),
            ],
        };

        // Act
        var alerts = new ProbeFailedRule().Evaluate(snapshot, Context()).ToList();

        // Assert: один critical на скоп; per-member warning не эмитятся —
        // один факт, один алерт (spec §1.3).
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("probe-failed:patroni-scope:demo-s1");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Details!["failed"].Should().Be("2");
        alert.Details["total"].Should().Be("2");
        alert.Details["cluster"].Should().Be("demo");
    }

    [Fact]
    public void ProbeFailed_LifecycleTargets_Suppressed()
    {
        // Arrange: пробы падают по NOT_INITIALIZED/TO_REMOVE-кластерам,
        // TO_REMOVE-шарду Active-кластера и их HA-скопам.
        var fresh = TestSnapshots.FullCluster() with
        {
            Name = "fresh", DbName = "fresh", State = ClusterState.NotInitialized,
        };
        var dying = TestSnapshots.FullCluster() with
        {
            Name = "dying", DbName = "dying", State = ClusterState.ToRemove,
        };
        var shardRemoving = TestSnapshots.FullCluster() with
        {
            Shards = [TestSnapshots.FullCluster().Shards.Single() with { State = ShardState.ToRemove }],
        };
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Clusters = [fresh, dying, shardRemoving],
            HaScopes =
            [
                TestSnapshots.HaScopeDemo(Now),
                TestSnapshots.HaScopeDemo(Now) with { Scope = "fresh-s1", Cluster = "fresh" },
                TestSnapshots.HaScopeDemo(Now) with { Scope = "dying-s1", Cluster = "dying" },
            ],
            Probes =
            [
                new ProbeResult("fresh/s1", "sql", false, 1.0, "refused", Now),
                new ProbeResult("dying/s1", "sql", false, 1.0, "refused", Now),
                new ProbeResult("demo/s1", "sql", false, 1.0, "refused", Now),
                new ProbeResult("fresh-s1/s1a", "patroni", false, 1.0, "refused", Now),
                new ProbeResult("fresh-s1/s1b", "patroni", false, 1.0, "refused", Now),
                new ProbeResult("dying-s1/s1a", "patroni", false, 1.0, "refused", Now),
                new ProbeResult("dying-s1/s1b", "patroni", false, 1.0, "refused", Now),
            ],
        };

        // Act
        var alerts = new ProbeFailedRule().Evaluate(snapshot, Context()).ToList();

        // Assert: подъём/демонтаж — не авария, lifecycle-цели не алертятся
        // (spec §1.4; прецедент — подавление shard-no-leader).
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void ProbeFailed_NoProbesOrDegenerateTargets_Silent()
    {
        // Arrange: проб нет вовсе; orphan-результат по несуществующему скопу;
        // вырожденные цели: Active-шард без DSN с упавшей sql-пробой и
        // matched-скоп Active-кластера без членов (spec §3.3).
        var degenerate = TestSnapshots.Healthy(Now) with
        {
            Clusters =
            [
                TestSnapshots.FullCluster() with
                {
                    Shards = [TestSnapshots.FullCluster().Shards.Single() with { DsnHosts = [] }],
                },
            ],
            HaScopes = [TestSnapshots.HaScopeDemo(Now) with { Members = [] }],
            Probes =
            [
                new ProbeResult("demo/s1", "sql", false, 1.0, "refused", Now),
                new ProbeResult("demo-s1/s1a", "patroni", false, 1.0, "refused", Now),
            ],
        };

        // Act
        var empty = new ProbeFailedRule().Evaluate(TestSnapshots.WithHaScopes(Now), Context()).ToList();
        var orphan = new ProbeFailedRule().Evaluate(TestSnapshots.WithHaScopes(Now) with
        {
            Probes = [new ProbeResult("ghost-s1/s1a", "patroni", false, 1.0, "refused", Now)],
        }, Context()).ToList();
        var degenerateAlerts = new ProbeFailedRule().Evaluate(degenerate, Context()).ToList();

        // Assert: без результатов (пробы выключены), по исчезнувшей цели и по
        // вырожденным целям (шард без DSN, скоп без членов) — тишина
        // (spec §2, §3.3): правило идёт от целей снапшота.
        empty.Should().BeEmpty();
        orphan.Should().BeEmpty();
        degenerateAlerts.Should().BeEmpty();
    }

    // ==== SQL-правила (spec §10.1 ч.2) ====

    private static EtcdSnapshot SnapshotWithRuntime(ShardRuntime runtime) => TestSnapshots.Healthy(Now) with
    {
        Clusters =
        [
            TestSnapshots.FullCluster() with
            {
                Shards = [TestSnapshots.FullCluster().Shards.Single() with { Runtime = runtime }],
            },
        ],
    };

    [Fact]
    public void SlotLagHigh_AboveThreshold_Warning()
    {
        // Arrange: слот с лагом 17 МБ (дефолт 16 МБ).
        var runtime = TestSnapshots.ShardRuntimeOf("s1") with
        {
            Slots = [new ReplicationSlotInfo("move_bucket_3", "logical", true, "active", null, 17L * 1024 * 1024)],
        };
        var snapshot = SnapshotWithRuntime(runtime);

        // Act
        var alerts = new SlotLagHighRule(Options.Create(LagOptions())).Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("slot-lag-high:demo/s1/move_bucket_3");
        alert.Details!["thresholdBytes"].Should().Be((16L * 1024 * 1024).ToString());
    }

    [Fact]
    public void SlotWalLost_LostSlot_Critical()
    {
        // Arrange: wal_status=lost — WAL срезан (P4).
        var snapshot = SnapshotWithRuntime(TestSnapshots.ShardRuntimeOf("s1"));

        // Act
        var alerts = new SlotWalLostRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Id.Should().Be("slot-wal-lost:demo/s1/move_bucket_3");
    }

    [Fact]
    public void SlotInvalidationRisk_BelowThreshold_Warning()
    {
        // Arrange: safe_wal_size 512 МБ < порога 1 GiB (P4, ДО среза).
        var snapshot = SnapshotWithRuntime(TestSnapshots.ShardRuntimeOf("s1"));

        // Act
        var alerts = new SlotInvalidationRiskRule(Options.Create(new AlertsOptions())).Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("slot-invalidation-risk:demo/s1/move_bucket_3");
        alert.Details!["safeWalSizeBytes"].Should().Be((512L * 1024 * 1024).ToString());
    }

    [Fact]
    public void SlotRules_NullSafeWalSizeAndErrorRuntime_Skipped()
    {
        // Arrange: safe_wal_size null (нет max_slot_wal_keep_size) — риска нет;
        // runtime с ошибкой и шард без runtime (пробы выключены) — SQL-алерты молчат
        // (03 §4, spec §3.7; null-runtime — регрессия гварда InventoryMismatchRule).
        var noRisk = TestSnapshots.ShardRuntimeOf("s1") with
        {
            Slots = [new ReplicationSlotInfo("move_bucket_3", "logical", true, "active", null, 100L)],
        };
        var errored = TestSnapshots.ShardRuntimeOf("s1") with { Error = "connect refused" };
        var ruleOptions = Options.Create(new AlertsOptions());

        // Act
        var riskOnNull = new SlotInvalidationRiskRule(ruleOptions).Evaluate(SnapshotWithRuntime(noRisk), Context()).ToList();
        var allOnError = new[]
        {
            new SlotLagHighRule(ruleOptions).Evaluate(SnapshotWithRuntime(errored), Context()),
            new SlotWalLostRule().Evaluate(SnapshotWithRuntime(errored), Context()),
            new SyncStandbyMissingRule().Evaluate(SnapshotWithRuntime(errored), Context()),
            new InventoryMismatchRule().Evaluate(SnapshotWithRuntime(errored), Context()),
        }.SelectMany(a => a).ToList();
        var allOnNoRuntime = new[]
        {
            new SlotLagHighRule(ruleOptions).Evaluate(TestSnapshots.Healthy(Now), Context()),
            new SlotWalLostRule().Evaluate(TestSnapshots.Healthy(Now), Context()),
            new SyncStandbyMissingRule().Evaluate(TestSnapshots.Healthy(Now), Context()),
            new InventoryMismatchRule().Evaluate(TestSnapshots.Healthy(Now), Context()),
        }.SelectMany(a => a).ToList();

        // Assert: Healthy — шард без Runtime (t03-фикстура): правила молчат, не падают.
        riskOnNull.Should().BeEmpty();
        allOnError.Should().BeEmpty();
        allOnNoRuntime.Should().BeEmpty();
    }

    [Fact]
    public void SyncStandbyMissing_MasterWithoutSync_Warning()
    {
        // Arrange: мастер (IsInRecovery false), standby только async (P8).
        var snapshot = SnapshotWithRuntime(TestSnapshots.ShardRuntimeOf("s1"));

        // Act
        var alerts = new SyncStandbyMissingRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("sync-standby-missing:demo/s1");
        alert.Details!["standbiesTotal"].Should().Be("1");
    }

    [Fact]
    public void SyncStandbyMissing_WithQuorum_NoAlert()
    {
        // Arrange: quorum-standby присутствует.
        var runtime = TestSnapshots.ShardRuntimeOf("s1") with
        {
            Standbies = [new StandbyInfo("s1b", "10.0.0.2", "streaming", "quorum", 0L)],
        };

        // Act
        var alerts = new SyncStandbyMissingRule().Evaluate(SnapshotWithRuntime(runtime), Context()).ToList();

        // Assert
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void InventoryMismatch_MissingAndExtraSchemas_Warning()
    {
        // Arrange: routing ждёт bucket_0..15 (схемы фикстуры — 16 шт.), но на шарде
        // нет bucket_15 и есть лишняя bucket_9 (в тесте подменяем инвентарь).
        var runtime = TestSnapshots.ShardRuntimeOf("s1") with
        {
            BucketSchemas = [.. Enumerable.Range(0, 15).Select(i => $"bucket_{i}"), "bucket_99"],
        };
        var snapshot = SnapshotWithRuntime(runtime);

        // Act
        var alerts = new InventoryMismatchRule().Evaluate(snapshot, Context()).ToList();

        // Assert: missing bucket_15, extra bucket_99 (сортировка стабильна).
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("inventory-mismatch:demo/s1");
        alert.Details!["missing"].Should().Be("bucket_15");
        alert.Details["extra"].Should().Be("bucket_99");
    }

    [Fact]
    public void InventoryMismatch_MovingBucketExcluded_NoAlert()
    {
        // Arrange: bucket_1 в SYNCING на target s2 — на s1 не ожидается, лишней не считается.
        var cluster = TestSnapshots.FullCluster() with
        {
            Buckets = [.. Enumerable.Range(0, 16).Select(i =>
                i == 1
                    ? new BucketInfo(1, "s2", BucketState.Syncing, new MoveInfo("s1", "s2", null, null, "copy", null))
                    : new BucketInfo(i, "s1", BucketState.Active, null))],
            Shards = [TestSnapshots.FullCluster().Shards.Single() with
            {
                Runtime = TestSnapshots.ShardRuntimeOf("s1") with
                {
                    BucketSchemas = [.. Enumerable.Range(0, 16).Where(i => i != 1).Select(i => $"bucket_{i}")],
                },
            }],
        };
        var snapshot = TestSnapshots.Healthy(Now) with { Clusters = [cluster] };

        // Act
        var alerts = new InventoryMismatchRule().Evaluate(snapshot, Context()).ToList();

        // Assert: переездные бакеты исключены с обеих сторон (spec §3.11).
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void InventoryMismatch_MovingBucketSchemaOnSourceAndTarget_NoAlert()
    {
        // Arrange: bucket_1 в SYNCING (s1→s2): схема ОСТАЁТСЯ на источнике s1 до finalize
        // и уже ПРИСУТСТВУЕТ на приёмнике s2 (M1 применил DDL) — «лишние» ни там, ни там.
        var source = TestSnapshots.FullCluster().Shards.Single() with
        {
            Name = "s1",
            Runtime = TestSnapshots.ShardRuntimeOf("s1") with
            {
                BucketSchemas = [.. Enumerable.Range(0, 16).Select(i => $"bucket_{i}")],
            },
        };
        var target = TestSnapshots.FullCluster().Shards.Single() with
        {
            Name = "s2",
            Runtime = TestSnapshots.ShardRuntimeOf("s2") with
            {
                BucketSchemas = ["bucket_1"],
            },
        };
        var cluster = TestSnapshots.FullCluster() with
        {
            Buckets = [.. Enumerable.Range(0, 16).Select(i =>
                i == 1
                    ? new BucketInfo(1, "s1", BucketState.Syncing, new MoveInfo("s1", "s2", null, null, "copy", null))
                    : new BucketInfo(i, "s1", BucketState.Active, null))],
            Shards = [source, target],
        };
        var snapshot = TestSnapshots.Healthy(Now) with { Clusters = [cluster] };

        // Act
        var alerts = new InventoryMismatchRule().Evaluate(snapshot, Context()).ToList();

        // Assert: инвентарь здорового переезда — не аномалия (регрессия стенда audit2).
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void InventoryMismatch_PostMoveLeftoverWithRbSubscription_NoAlert()
    {
        // Arrange: bucket_1 УЖЕ Active на новом владельце s2, но на старом шарде s1
        // до finalize остаются замороженная схема и подписка sub_bucket_1_rb —
        // окно rollback завершённого переезда (t01), не аномалия.
        var oldShard = TestSnapshots.FullCluster().Shards.Single() with
        {
            Name = "s1",
            Runtime = TestSnapshots.ShardRuntimeOf("s1") with
            {
                BucketSchemas = [.. Enumerable.Range(0, 16).Select(i => $"bucket_{i}")],
                Subscriptions = [new SubscriptionInfo("sub_bucket_1_rb", null, null, null)],
            },
        };
        var newShard = TestSnapshots.FullCluster().Shards.Single() with
        {
            Name = "s2",
            Runtime = TestSnapshots.ShardRuntimeOf("s2") with
            {
                BucketSchemas = ["bucket_1"],
            },
        };
        var cluster = TestSnapshots.FullCluster() with
        {
            Buckets = [.. Enumerable.Range(0, 16).Select(i =>
                i == 1
                    ? new BucketInfo(1, "s2", BucketState.Active, null)
                    : new BucketInfo(i, "s1", BucketState.Active, null))],
            Shards = [oldShard, newShard],
        };
        var snapshot = TestSnapshots.Healthy(Now) with { Clusters = [cluster] };

        // Act
        var alerts = new InventoryMismatchRule().Evaluate(snapshot, Context()).ToList();

        // Assert: bucket_1 на s1 — «лишняя» (routing → s2), но подавлена окном rollback.
        alerts.Should().BeEmpty("окно rollback до finalize — управляемое состояние");
    }

    [Fact]
    public void InventoryMismatch_PostMoveLeftoverWithoutRbSubscription_Alerts()
    {
        // Arrange: то же, но подписки sub_bucket_2_rb НЕТ — схема на старом шарде
        // осталась без окна rollback (finalize прошёл частично?) — сигнал нужен.
        var cluster = TestSnapshots.FullCluster() with
        {
            Buckets = [.. Enumerable.Range(0, 16).Select(i =>
                i == 2
                    ? new BucketInfo(2, "s2", BucketState.Active, null)
                    : new BucketInfo(i, "s1", BucketState.Active, null))],
            Shards = [TestSnapshots.FullCluster().Shards.Single() with
            {
                Name = "s1",
                Runtime = TestSnapshots.ShardRuntimeOf("s1") with
                {
                    BucketSchemas = [.. Enumerable.Range(0, 16).Select(i => $"bucket_{i}")],
                },
            }],
        };
        var snapshot = TestSnapshots.Healthy(Now) with { Clusters = [cluster] };

        // Act
        var alerts = new InventoryMismatchRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Details!["extra"].Should().Be("bucket_2", "схема без окна rollback — рассинхрон");
    }

    [Fact]
    public void HaRules_FullEngine_Scenario()
    {
        // Arrange: нет лидера + реплика не стримит + слот lost + нет sync-standby + проба падала.
        var runtime = TestSnapshots.ShardRuntimeOf("s1");
        var cluster = TestSnapshots.FullCluster() with
        {
            Shards = [TestSnapshots.FullCluster().Shards.Single() with { Runtime = runtime }],
        };
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                LeaderName = null,
                Members =
                [
                    new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, Now, null, null),
                    new HaMember("s1b", "s1b", 5432, "replica", "starting", 1L, 10L, Now, null, null),
                ],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Clusters = [cluster],
            HaScopes = scopes,
            Probes = [new ProbeResult("demo-s1/s1a", "patroni", false, 1.0, "boom", Now)],
        };
        var engine = new AlertEngine(AlertTestRules.All());

        // Act
        var alerts = engine.Evaluate(snapshot, null, Now, 3).ToList();

        // Assert: сортировка severity → kind (Ordinal): critical (shard-no-leader,
        // slot-wal-lost) → warning (ha-member-not-streaming, slot-invalidation-risk,
        // sync-standby-missing) → info (probe-failed). Слот фикстуры несёт
        // safe_wal_size 512 МБ < 1 GiB — risk-алерт входит в сценарий законно (6-й).
        // t04/t05-правила на этой фикстуре молчат.
        alerts.Select(a => a.Id).Should().ContainInOrder(
            "shard-no-leader:demo-s1",
            "slot-wal-lost:demo/s1/move_bucket_3",
            "ha-member-not-streaming:demo-s1/s1b",
            "slot-invalidation-risk:demo/s1/move_bucket_3",
            "sync-standby-missing:demo/s1",
            "probe-failed:patroni:demo-s1/s1a");
        alerts.Select(a => a.Id).Should().HaveCount(6);
    }

    private static AlertContext Context() => new(null, Now, 3);
}
