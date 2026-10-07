using AdminPanel.Core;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// ReliabilityCalculator (spec §3.4): чистая функция над снапшотом — RPO-блок
// (валидный полный, лаг WAL, сводный потенциал) и RTO-блок (факты/drill/restore).
public class ReliabilityCalculatorTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.FromUnixTimeSeconds(1_760_000_000);

    private static EtcdSnapshot Snapshot(
        IReadOnlyList<ClusterBackupsInfo>? backups = null,
        IReadOnlyList<WorkJournalInfo>? work = null,
        IReadOnlyList<ClusterInfo>? clusters = null)
        => new(
            Now,
            TestSnapshots.HealthyEtcd(Now),      // живой EtcdStatus (существующий helper)
            clusters ?? [], [], [], [],
            backups ?? [],                        // Backups
            [],                                   // PgWorkerEndpoints
            work ?? [],                           // PgWorkerWork
            [], [], [], [],                       // WorkerHealth, Probes, Alerts, ParseErrors
            0);

    [Fact]
    public void WalActive_RpoPotential_IsWalAge()
    {
        // Arrange: свежий полный (age 100с), wal ACTIVE с age 10с; policy-ключа нет
        var backups = new List<ClusterBackupsInfo>
        {
            new("shop", null,
                new Dictionary<string, long?> { ["s1"] = Now.ToUnixTimeSeconds() - 100 },
                Shards: new Dictionary<string, WalStreamInfo?>
                {
                    ["s1"] = new("shop", "s1", WalStreamInfoState.Active, "slot", "n1",
                        Now.ToUnixTimeSeconds() - 10, 3, null),
                }),
        };
        var clusters = new List<ClusterInfo> { Cluster("shop", "s1") };

        // Act
        var result = ReliabilityCalculator.Calculate(Snapshot(backups, clusters: clusters), Now);

        // Assert: RPO держится WAL-хвостом — потеряем возраст последней загрузки;
        // policy нет — порог = каталожный дефолт правила BackupFullStaleRule
        var rpo = result.Single().Shards.Single().Rpo!;
        rpo.Mode.Should().Be(RpoMode.Wal);
        rpo.RpoPotentialSec.Should().Be(10);
        rpo.FullAgeSec.Should().Be(100);
        rpo.WalLagSegments.Should().Be(3);
        rpo.ThresholdFullAgeSec.Should().Be(BackupFullStaleRule.DefaultMaxAgeSec);
    }

    [Fact]
    public void WalBroken_RpoPotential_FallsBackToFull_ThresholdFromPolicy()
    {
        // Arrange: цепочка WAL разорвана — потеряем весь хвост после полного;
        // порог — из ПОЛНОЙ policy кластера (Policy.FullMaxAgeSec), не из
        // ClusterBackupsInfo.FullMaxAgeSec и не дефолт
        var t = Now.ToUnixTimeSeconds();
        var backups = new List<ClusterBackupsInfo>
        {
            new("shop", null,
                new Dictionary<string, long?> { ["s1"] = t - 500 },
                Shards: new Dictionary<string, WalStreamInfo?>
                {
                    ["s1"] = new("shop", "s1", WalStreamInfoState.Broken, "slot", "n1", t - 5, 9, "err"),
                },
                Policy: new BackupsPolicyInfo(null, null, null, 86400, null, null)),
        };

        // Act
        var result = ReliabilityCalculator.Calculate(Snapshot(backups, clusters: [Cluster("shop", "s1")]), Now);

        // Assert
        var rpo = result.Single().Shards.Single().Rpo!;
        rpo.Mode.Should().Be(RpoMode.Full);
        rpo.RpoPotentialSec.Should().Be(500);
        rpo.ThresholdFullAgeSec.Should().Be(86400);
    }

    [Fact]
    public void NoBackupsPrefix_ModeOff()
    {
        // Arrange: префикс бэкапов пуст — подсистема не включена, секция молчит
        // Act
        var result = ReliabilityCalculator.Calculate(Snapshot(clusters: [Cluster("shop", "s1")]), Now);

        // Assert
        var rpo = result.Single().Shards.Single().Rpo!;
        rpo.Mode.Should().Be(RpoMode.Off);
        rpo.RpoPotentialSec.Should().BeNull();
    }

    [Fact]
    public void FullId_ValidCompletedNotFailedVerify()
    {
        // Arrange: FAILED-полный НОВЕЕ валидного — свежесть/источник только валидный
        var t = Now.ToUnixTimeSeconds();
        var backups = new List<ClusterBackupsInfo>
        {
            new("shop", null, new Dictionary<string, long?> { ["s1"] = t - 100 },
                ShardsFulls: new Dictionary<string, IReadOnlyList<BackupFullInfo>>
                {
                    ["s1"] = new List<BackupFullInfo>
                    {
                        new("b-old", "COMPLETED", null, t - 300, t - 100, 1, "OK", null, null),
                        new("b-new", "COMPLETED", null, t - 50, t - 40, 1, "FAILED", t - 30, "verify err"),
                    },
                }),
        };

        // Act
        var result = ReliabilityCalculator.Calculate(Snapshot(backups, clusters: [Cluster("shop", "s1")]), Now);

        // Assert: id валидного (verify ≠ FAILED); FAILED новее — не свежесть
        result.Single().Shards.Single().Rpo!.FullId.Should().Be("b-old");
    }

    [Fact]
    public void OpenFailoverFact_OngoingSecondsTick()
    {
        // Arrange: открытый failover (detected 200с назад) в work-ключе
        var work = new List<WorkJournalInfo>
        {
            new("shop", "supervise", "supervising", "i1", Now.ToUnixTimeSeconds(), null, null, null, null,
                new HaSupervisionInfo("s1", "n1", "elections", Now.ToUnixTimeSeconds() - 200, null, null), null),
        };

        // Act
        var result = ReliabilityCalculator.Calculate(Snapshot(work: work, clusters: [Cluster("shop", "s1")]), Now);

        // Assert: ongoing с тикающей длительностью now − detected
        var failover = result.Single().Shards.Single().Rto!.LastFailover!;
        failover.Ongoing.Should().BeTrue();
        failover.OngoingSec.Should().Be(200);
        failover.DurationSec.Should().BeNull();
    }

    [Fact]
    public void FactOfUndeclaredShard_NotShown()
    {
        // Arrange: факт шарда s9, живая декларация — только s1
        var work = new List<WorkJournalInfo>
        {
            new("shop", "supervise", "supervising", "i1", Now.ToUnixTimeSeconds(), null, null, null, null,
                new HaSupervisionInfo("s9", "n1", "elections", 1, 2, 1), null),
        };

        // Act
        var result = ReliabilityCalculator.Calculate(Snapshot(work: work, clusters: [Cluster("shop", "s1")]), Now);

        // Assert: факт пережил удаление шарда из декларации — не отображается
        result.Single().Shards.Should().ContainSingle().Which.Shard.Should().Be("s1");
        result.Single().Shards.Single().Rto!.LastFailover.Should().BeNull();
    }

    [Fact]
    public void OldKeyWithoutFacts_RtoEmpty()
    {
        // Arrange: work-ключ старого формата (без полей фактов)
        var work = new List<WorkJournalInfo>
        {
            new("shop", "supervise", "supervising", "i1", Now.ToUnixTimeSeconds(), null, null, null, null),
        };

        // Act/Assert: толерантность — RTO-блок пустой, не падает
        var rto = ReliabilityCalculator.Calculate(Snapshot(work: work, clusters: [Cluster("shop", "s1")]), Now)
            .Single().Shards.Single().Rto!;
        ((object?)rto.LastFailover).Should().BeNull();
        ((object?)rto.LastRebuild).Should().BeNull();
    }

    [Fact]
    public void ClosedRebuildFact_DurationFromFact()
    {
        // Arrange: закрытый rebuild — длительность фиксирует воркер
        var work = new List<WorkJournalInfo>
        {
            new("shop", "supervise", "supervising", "i1", Now.ToUnixTimeSeconds(), null, null, null, null,
                null, new HaSupervisionInfo("s1", "n2", "auto-dead", 1000, 1150, 150)),
        };

        // Act
        var rto = ReliabilityCalculator.Calculate(Snapshot(work: work, clusters: [Cluster("shop", "s1")]), Now)
            .Single().Shards.Single().Rto!;

        // Assert
        rto.LastRebuild!.Ongoing.Should().BeFalse();
        rto.LastRebuild.DurationSec.Should().Be(150);
    }

    [Fact]
    public void DrillAndRestore_DurationsFromSnapshot()
    {
        // Arrange: завершённый drill (400с), активный restore (от заявки 60с назад)
        var t = Now.ToUnixTimeSeconds();
        var backups = new List<ClusterBackupsInfo>
        {
            new("shop", null, new Dictionary<string, long?> { ["s1"] = t - 100 },
                ShardsDrills: new Dictionary<string, DrillInfo>
                {
                    ["s1"] = new("shop", "s1", "d1", "SUCCEEDED", "b1", t - 500, t - 100, null, null, null),
                },
                ShardsRestores: new Dictionary<string, IReadOnlyList<RestoreOperationInfo>>
                {
                    ["s1"] = new List<RestoreOperationInfo>
                    {
                        new("shop", "s1", "r1", "RUNNING", null, t - 60, t - 50, null, "recovering"),
                    },
                }),
        };

        // Act
        var rto = ReliabilityCalculator.Calculate(Snapshot(backups, clusters: [Cluster("shop", "s1")]), Now)
            .Single().Shards.Single().Rto!;

        // Assert: drill 400с; restore ongoing от RequestedUnix (включая очередь)
        rto.LastDrill!.DurationSec.Should().Be(400);
        rto.LastDrill.State.Should().Be("SUCCEEDED");
        rto.LastRestore!.OngoingSec.Should().Be(60);
        rto.LastRestore.DurationSec.Should().BeNull();
    }

    [Fact]
    public void RestoreFinished_DurationFromRequested()
    {
        // Arrange: терминальный restore: заявка 1000, завершён 1100
        var backups = new List<ClusterBackupsInfo>
        {
            new("shop", null, new Dictionary<string, long?> { ["s1"] = 900 },
                ShardsRestores: new Dictionary<string, IReadOnlyList<RestoreOperationInfo>>
                {
                    ["s1"] = new List<RestoreOperationInfo>
                    {
                        new("shop", "s1", "r1", "COMPLETED", null, 1000, 1005, 1100, null),
                    },
                }),
        };

        // Act
        var rto = ReliabilityCalculator.Calculate(Snapshot(backups, clusters: [Cluster("shop", "s1")]), Now)
            .Single().Shards.Single().Rto!;

        // Assert: честный операторский RTO — от ЗАЯВКИ (включая очередь)
        rto.LastRestore!.DurationSec.Should().Be(100);
    }

    private static ClusterInfo Cluster(string name, params string[] shards)
        => new(name, "db", 2, 1755900000, ClusterState.Active,
            [.. shards.Select(s => new ShardInfo(s, "host=1", ["host=1"], 1, "db", "u", null, null, [], null))],
            [], []);
}
