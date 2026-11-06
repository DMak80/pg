using EtcdNode.Core;

namespace EtcdNode.UnitTests;

// Решение тика агента (arch/etcdnode/02-snapshots.md §3, процесс
// c-snapshot-export.md C2): плановый слепок раз в IntervalMin, внеочередной —
// только важное изменение + выгрузка старше MinIntervalSec.

public class SnapshotScheduleTests
{
    private const long Now = 1_000_000;

    [Fact]
    public void NeverTaken_Planned()
    {
        // Arrange: никогда не снимали
        // Act + Assert: плановый слепок немедленно
        SnapshotSchedule.ShouldTake(
                nowUnix: Now, lastTakeUnix: null, lastUploadedUnix: null,
                currentRevision: 10, baselineRevision: null,
                diffImportant: false, intervalMin: 360, minIntervalSec: 300)
            .Should().BeTrue();
    }

    [Fact]
    public void IntervalElapsed_Planned()
    {
        // Arrange: последний слепок 21600 с назад (360 мин)
        // Act + Assert: плановый слепок положен
        SnapshotSchedule.ShouldTake(
                nowUnix: Now, lastTakeUnix: Now - 21600, lastUploadedUnix: Now - 21600,
                currentRevision: 10, baselineRevision: 5,
                diffImportant: false, intervalMin: 360, minIntervalSec: 300)
            .Should().BeTrue();
    }

    [Fact]
    public void Quiet_NoBaselineGrowth_False()
    {
        // Arrange: снимали недавно, revision не растёт
        // Act + Assert: внеочередной не нужен
        SnapshotSchedule.ShouldTake(
                nowUnix: Now, lastTakeUnix: Now - 60, lastUploadedUnix: Now - 60,
                currentRevision: 100, baselineRevision: 100,
                diffImportant: false, intervalMin: 360, minIntervalSec: 300)
            .Should().BeFalse();
    }

    [Fact]
    public void NoBaseline_Important_Take()
    {
        // Arrange: эталонной карты нет (первый тик/смена лидера), выгрузка была давно
        // Act + Assert: снимаем немедленно (RPO-консервативно)
        SnapshotSchedule.ShouldTake(
                nowUnix: Now, lastTakeUnix: Now - 60, lastUploadedUnix: Now - 1000,
                currentRevision: 100, baselineRevision: null,
                diffImportant: false, intervalMin: 360, minIntervalSec: 300)
            .Should().BeTrue();
    }

    [Fact]
    public void RevisionGrown_DiffImportant_Take()
    {
        // Arrange: revision вырос, дифф нашёл важное, выгрузка 400 с назад
        // Act + Assert: внеочередной слепок (≥ MinIntervalSec)
        SnapshotSchedule.ShouldTake(
                nowUnix: Now, lastTakeUnix: Now - 60, lastUploadedUnix: Now - 400,
                currentRevision: 105, baselineRevision: 100,
                diffImportant: true, intervalMin: 360, minIntervalSec: 300)
            .Should().BeTrue();
    }

    [Fact]
    public void RevisionGrown_DiffNotImportant_False()
    {
        // Arrange: revision вырос только пульсами — дифф не важен
        // Act + Assert: внеочередного нет
        SnapshotSchedule.ShouldTake(
                nowUnix: Now, lastTakeUnix: Now - 60, lastUploadedUnix: Now - 400,
                currentRevision: 105, baselineRevision: 100,
                diffImportant: false, intervalMin: 360, minIntervalSec: 300)
            .Should().BeFalse();
    }

    [Fact]
    public void Important_TooSoonAfterUpload_False()
    {
        // Arrange: важное изменение есть, но выгрузка была 100 с назад (< 300)
        // Act + Assert: слишком рано — ждём
        SnapshotSchedule.ShouldTake(
                nowUnix: Now, lastTakeUnix: Now - 60, lastUploadedUnix: Now - 100,
                currentRevision: 105, baselineRevision: 100,
                diffImportant: true, intervalMin: 360, minIntervalSec: 300)
            .Should().BeFalse();
    }

    [Fact]
    public void Important_ExactlyAtMinInterval_Take()
    {
        // Arrange: важное изменение есть, выгрузка ровно 300 с назад
        // Act + Assert: граница «≥ MinIntervalSec» — снимаем
        SnapshotSchedule.ShouldTake(
                nowUnix: Now, lastTakeUnix: Now - 60, lastUploadedUnix: Now - 300,
                currentRevision: 105, baselineRevision: 100,
                diffImportant: true, intervalMin: 360, minIntervalSec: 300)
            .Should().BeTrue();
    }

    [Fact]
    public void Important_NeverUploaded_Take()
    {
        // Arrange: важное изменение, но выгрузки никогда не было
        // Act + Assert: снимаем
        SnapshotSchedule.ShouldTake(
                nowUnix: Now, lastTakeUnix: Now - 60, lastUploadedUnix: null,
                currentRevision: 105, baselineRevision: 100,
                diffImportant: true, intervalMin: 360, minIntervalSec: 300)
            .Should().BeTrue();
    }
}
