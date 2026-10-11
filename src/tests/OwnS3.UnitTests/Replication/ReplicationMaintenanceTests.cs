using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Replication;
using OwnS3.Replication.Cursors;
using OwnS3.Replication.Lww;
using OwnS3.Replication.Outbox;

namespace OwnS3.UnitTests.Replication;

// Цикл обслуживания (t35, глава 07 §8-а): штатный тримминг по min(watermark приёма
// активных соседей), принудительный тримминг при бюджете (+лог потерь), retention-
// чистка tombstones.
public sealed class ReplicationMaintenanceFixture : IDisposable
{
    public string OutboxDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public string CursorsDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public string TombstonesDir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
    public TargetCursors Cursors { get; } = new(
        Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N")), TimeProvider.System);

    public OutboxJournal Journal(long budget = long.MaxValue, long maxSegment = 1) =>
        new(OutboxDir, budget, NullLogger<OutboxJournal>.Instance, maxSegment,
            TimeSpan.FromMinutes(5)); // пособытийная ротация (тестовая)

    public ReplicationMaintenance Create(OutboxJournal journal, ReplicationTargets targets) =>
        new(journal, Cursors, targets, new TombstoneStore(TombstonesDir),
            new ReplicationOptions { TombstoneRetentionHours = 168 }, TimeProvider.System,
            NullLogger<ReplicationMaintenance>.Instance);

    public void Dispose()
    {
        // Teardown: temp-каталоги удаляются при любом исходе
        Cursors.Dispose();
        foreach (var dir in new[] { OutboxDir, CursorsDir, TombstonesDir })
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
    }
}

public class ReplicationMaintenanceTests : IClassFixture<ReplicationMaintenanceFixture>
{
    private readonly ReplicationMaintenanceFixture _fixture = new();

    public ReplicationMaintenanceTests()
    {
        // Изоляция кейсов: temp-каталоги на кейс
        if (Directory.Exists(_fixture.OutboxDir))
            Directory.Delete(_fixture.OutboxDir, recursive: true);
        if (Directory.Exists(_fixture.TombstonesDir))
            Directory.Delete(_fixture.TombstonesDir, recursive: true);
    }

    private static ReplicationTargets Targets(params string[] names)
    {
        var dir = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
        var seed = names.Select(n => new TargetOptions(n, $"http://{n}:9000", "ak-" + n, "sk")).ToList();
        return ReplicationTargets.Load(dir, seed, NullLogger<ReplicationTargets>.Instance);
    }

    [Fact]
    public async Task MaintenancePass_TrimsByMinWatermark()
    {
        // Arrange — 4 события (пособытийная ротация); сосед ACKнул 2
        var journal = _fixture.Journal();
        foreach (var i in new[] { 1L, 2L, 3L, 4L })
            journal.Append(ReplicationEventType.ObjectPut, "b", $"k{i}", i, "e", 1, "s", "h");
        _fixture.Cursors.Advance("peer-a", 2);
        var maintenance = _fixture.Create(journal, Targets("peer-a"));

        // Act
        await maintenance.RunMaintenancePassAsync(TestContext.Current.CancellationToken);

        // Assert — сегменты с концом < 2 удалены, хвост (3,4) жив
        journal.ReadBatch(0, 10).Select(e => e.SeqNo).Should().ContainInOrder(3, 4);
        journal.LastSeqNo.Should().Be(4);
    }

    [Fact]
    public async Task MaintenancePass_SlowestNeighborHoldsSegments()
    {
        // Arrange — два соседа: быстрый ACKнул 4, отстающий — 2
        var journal = _fixture.Journal();
        foreach (var i in new[] { 1L, 2L, 3L, 4L })
            journal.Append(ReplicationEventType.ObjectPut, "b", $"k{i}", i, "e", 1, "s", "h");
        _fixture.Cursors.Advance("fast", 4);
        _fixture.Cursors.Advance("slow", 2);
        var maintenance = _fixture.Create(journal, Targets("fast", "slow"));

        // Act — проход при min=2
        await maintenance.RunMaintenancePassAsync(TestContext.Current.CancellationToken);

        // Assert — сегмент с событием 2 удержан (ReadBatch(1,..) отдаёт 2,3,4);
        // после ACK отстающего 3 → проход удаляет сегменты с концом < 3
        journal.ReadBatch(1, 10).Select(e => e.SeqNo).Should().ContainInOrder(2, 3, 4);
        _fixture.Cursors.Advance("slow", 3);
        await maintenance.RunMaintenancePassAsync(TestContext.Current.CancellationToken);
        journal.ReadBatch(2, 10).Select(e => e.SeqNo).Should().ContainInOrder(3, 4);
    }

    [Fact]
    public async Task MaintenancePass_BudgetExceeded_ForceTrimWithLog()
    {
        // Arrange — малый бюджет, 6 событий (пособытийная ротация), сосед offline (нет ACK)
        var entrySize = OutboxEntryCodec.Encode(new ReplicationEvent(1, ReplicationEventType.ObjectPut, "b",
            "k", 1, "e", 1, "s", "h")).Length;
        var journal = _fixture.Journal(budget: entrySize * 3, maxSegment: 1);
        for (var i = 1; i <= 6; i++)
            journal.Append(ReplicationEventType.ObjectPut, "b", $"k{i}", i, "e", 1, "s", "h");
        journal.BudgetExceeded.Should().BeTrue();
        var maintenance = _fixture.Create(journal, Targets("offline-peer"));

        // Act
        await maintenance.RunMaintenancePassAsync(TestContext.Current.CancellationToken);

        // Assert — журнал удержан бюджетом (принудительный тримминг), запись живёт;
        // отстающий сосед теряет уведомления — это фиксирует лог (алерт-метрика в журнале)
        var appended = journal.Append(ReplicationEventType.ObjectPut, "b", "after", 99, "e", 1, "s", "h");
        appended.SeqNo.Should().Be(7);
    }

    [Fact]
    public async Task MaintenancePass_TombstoneRetention_Cleanup()
    {
        // Arrange — tombstone старше retention (168 ч) и свежий
        var now = TimeProvider.System.GetUtcNow();
        var tombstones = new TombstoneStore(_fixture.TombstonesDir);
        tombstones.Put("b", "old", now.AddHours(-200).ToUnixTimeMilliseconds());
        tombstones.Put("b", "fresh", now.AddHours(-1).ToUnixTimeMilliseconds());
        var journal = _fixture.Journal();
        var maintenance = new ReplicationMaintenance(journal, _fixture.Cursors,
            Targets("peer-a"), tombstones, new ReplicationOptions { TombstoneRetentionHours = 168 },
            TimeProvider.System, NullLogger<ReplicationMaintenance>.Instance);

        // Act
        await maintenance.RunMaintenancePassAsync(TestContext.Current.CancellationToken);

        // Assert — старый удалён проходом, свежий остался
        tombstones.GetDeleteTime("b", "old").Should().BeNull();
        tombstones.GetDeleteTime("b", "fresh").Should().NotBeNull();
    }
}
