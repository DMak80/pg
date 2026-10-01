using PgWorker.Backups.Drill;
using PgWorker.Core.Model;
using PgWorker.Etcd.Parsing;

namespace PgWorker.UnitTests.Backups;

// Чистый отбор кандидата дрилла (reliability t02, arch/19 §3.6): один шард
// за проход — наименее свежий; гварды интервала/активного дрилла/cleaning/
// restore/ToRemove; без I/O.
public class DrillPlannerTests
{
    // --- Фабрики-хелперы (минимальные спеки/состояния для отбора) ---

    private static ShardSpec Shard(string name, bool toRemove = false) =>
        new(name, Replicas: 2, Dsn: null, Master: null, Nodes: [], ToRemove: toRemove);

    private static FullBackupState CompletedFull(string id) =>
        new(id, FullBackupStatus.Completed, "n1", BackupSourceRole.Replica,
            StartedUnix: 1760000000, FinishedUnix: 1760000100,
            WalStartSegment: "000000010000000000000001", SizeBytes: 1, Error: null,
            Verify: null);

    private static ShardBackups Backups(params FullBackupState[] fulls) =>
        new(fulls, Wal: null);

    private static RestoreOperationState ActiveRestore(string id) =>
        new(id, RestoreStatus.Running, "b1", "c/x", "latest", "n1", 1760000000, "api");

    private static DrillState Drill(DrillStatus state, long? finished = null, string? phase = null) =>
        new("id1", state, "b1", StartedUnix: 1760000000,
            FinishedUnix: finished, Phase: phase);

    // AAA: интервал ≤ 0 — запусков нет (выключено).
    [Fact]
    public void SelectCandidate_IntervalZero_ReturnsNull()
    {
        // Arrange
        var shards = new[] { Shard("s1") };
        var backups = new Dictionary<string, ShardBackups> { ["s1"] = Backups(CompletedFull("b1")) };
        var drills = new Dictionary<string, DrillState>();

        // Act
        var result = DrillPlanner.SelectCandidate(shards, backups, drills, intervalDays: 0, nowUnix: 1760900000);

        // Assert
        result.Should().BeNull();
    }

    // AAA: незавершённый дрилл кластера (RUNNING у любого шарда) блокирует новые.
    [Fact]
    public void SelectCandidate_ActiveDrill_ReturnsNull()
    {
        // Arrange
        var shards = new[] { Shard("s1"), Shard("s2") };
        var backups = new Dictionary<string, ShardBackups>
        {
            ["s1"] = Backups(CompletedFull("b1")),
            ["s2"] = Backups(CompletedFull("b2")),
        };
        var drills = new Dictionary<string, DrillState> { ["s2"] = Drill(DrillStatus.Running) };

        // Act
        var result = DrillPlanner.SelectCandidate(shards, backups, drills, intervalDays: 1, nowUnix: 1760900000);

        // Assert
        result.Should().BeNull();
    }

    // AAA: терминальный дрилл с phase=cleaning блокирует новые (снос не доведён).
    [Fact]
    public void SelectCandidate_UnclosedCleaning_ReturnsNull()
    {
        // Arrange
        var shards = new[] { Shard("s1"), Shard("s2") };
        var backups = new Dictionary<string, ShardBackups>
        {
            ["s1"] = Backups(CompletedFull("b1")),
            ["s2"] = Backups(CompletedFull("b2")),
        };
        var drills = new Dictionary<string, DrillState>
        {
            ["s2"] = Drill(DrillStatus.Succeeded, finished: 100, phase: "cleaning"),
        };

        // Act
        var result = DrillPlanner.SelectCandidate(shards, backups, drills, intervalDays: 1, nowUnix: 1760900000);

        // Assert
        result.Should().BeNull();
    }

    // AAA: ключа дрилла нет + есть COMPLETED-полный → готов немедленно (первый дрилл).
    [Fact]
    public void SelectCandidate_NoDrillKey_CompletedFull_Ready()
    {
        // Arrange
        var shards = new[] { Shard("s1") };
        var backups = new Dictionary<string, ShardBackups> { ["s1"] = Backups(CompletedFull("b1")) };
        var drills = new Dictionary<string, DrillState>();

        // Act
        var result = DrillPlanner.SelectCandidate(shards, backups, drills, intervalDays: 7, nowUnix: 1760000100);

        // Assert
        result.Should().Be("s1");
    }

    // AAA: с момента finished_unix последнего дрилла < interval_days — не готов.
    [Fact]
    public void SelectCandidate_RecentSuccess_WaitsInterval()
    {
        // Arrange
        var shards = new[] { Shard("s1") };
        var backups = new Dictionary<string, ShardBackups> { ["s1"] = Backups(CompletedFull("b1")) };
        var drills = new Dictionary<string, DrillState>
        {
            ["s1"] = Drill(DrillStatus.Succeeded, finished: 1760000000),
        };

        // Act — сутки ещё не прошли (86400 c = 1 день).
        var result = DrillPlanner.SelectCandidate(shards, backups, drills, intervalDays: 1, nowUnix: 1760000000 + 86400 - 1);

        // Assert
        result.Should().BeNull();
    }

    // AAA: now − finished ≥ interval_days×86400 → готов.
    [Fact]
    public void SelectCandidate_ExpiredInterval_Ready()
    {
        // Arrange
        var shards = new[] { Shard("s1") };
        var backups = new Dictionary<string, ShardBackups> { ["s1"] = Backups(CompletedFull("b1")) };
        var drills = new Dictionary<string, DrillState>
        {
            ["s1"] = Drill(DrillStatus.Succeeded, finished: 1760000000),
        };

        // Act
        var result = DrillPlanner.SelectCandidate(shards, backups, drills, intervalDays: 1, nowUnix: 1760000000 + 86400);

        // Assert
        result.Should().Be("s1");
    }

    // AAA: FAILED-дрилл тоже считается «последним» (готовность по finished_unix).
    [Fact]
    public void SelectCandidate_FailedDrill_CountsAsLast()
    {
        // Arrange
        var shards = new[] { Shard("s1") };
        var backups = new Dictionary<string, ShardBackups> { ["s1"] = Backups(CompletedFull("b1")) };
        var drills = new Dictionary<string, DrillState>
        {
            ["s1"] = Drill(DrillStatus.Failed, finished: 1760000000),
        };

        // Act — интервал не истёк → повторного запуска нет (FAILED отсчитывает интервал).
        var waiting = DrillPlanner.SelectCandidate(shards, backups, drills, intervalDays: 1, nowUnix: 1760000000 + 60);

        // Assert
        waiting.Should().BeNull();

        // Act — интервал истёк → шард снова готов.
        var ready = DrillPlanner.SelectCandidate(shards, backups, drills, intervalDays: 1, nowUnix: 1760000000 + 86400);

        // Assert
        ready.Should().Be("s1");
    }

    // AAA: активная restore-заявка шарда исключает шард (restore владеет ЖЦ).
    [Fact]
    public void SelectCandidate_ActiveRestore_ExcludesShard()
    {
        // Arrange
        var shard = new ShardBackups([CompletedFull("b1")], Wal: null, Restores: [ActiveRestore("r1")]);
        var shards = new[] { Shard("s1") };
        var backups = new Dictionary<string, ShardBackups> { ["s1"] = shard };
        var drills = new Dictionary<string, DrillState>();

        // Act
        var result = DrillPlanner.SelectCandidate(shards, backups, drills, intervalDays: 1, nowUnix: 1760900000);

        // Assert
        result.Should().BeNull();
    }

    // AAA: ToRemove-шард и шард без COMPLETED-полных — не кандидаты.
    [Fact]
    public void SelectCandidate_ToRemoveOrNoCompleted_Excluded()
    {
        // Arrange
        var shards = new[] { Shard("gone", toRemove: true), Shard("fresh") };
        var backups = new Dictionary<string, ShardBackups>
        {
            ["gone"] = Backups(CompletedFull("b1")),
            ["fresh"] = Backups(new FullBackupState("b2", FullBackupStatus.Failed, "n1",
                BackupSourceRole.Replica, 1760000000, 1760000100, null, null, null, null)),
        };
        var drills = new Dictionary<string, DrillState>();

        // Act
        var result = DrillPlanner.SelectCandidate(shards, backups, drills, intervalDays: 1, nowUnix: 1760900000);

        // Assert
        result.Should().BeNull();
    }

    // AAA: среди готовых выбирается наименее свежий (min finished_unix;
    // «никогда»=0 первым), tie-break по имени.
    [Fact]
    public void SelectCandidate_PicksLeastRecentlyDrilled()
    {
        // Arrange
        var shards = new[] { Shard("s3"), Shard("s2"), Shard("s1") };
        var backups = new Dictionary<string, ShardBackups>
        {
            ["s1"] = Backups(CompletedFull("b1")),
            ["s2"] = Backups(CompletedFull("b2")),
            ["s3"] = Backups(CompletedFull("b3")),
        };
        var drills = new Dictionary<string, DrillState>
        {
            ["s1"] = Drill(DrillStatus.Succeeded, finished: 1760000000),
            ["s2"] = Drill(DrillStatus.Failed, finished: 1750000000),
        };
        // s3 — дрилла не было («никогда» = 0).

        // Act
        var result = DrillPlanner.SelectCandidate(shards, backups, drills, intervalDays: 1, nowUnix: 1760900000);

        // Assert — «никогда» обслуживается первым.
        result.Should().Be("s3");
    }
}
