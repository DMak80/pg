using EtcdWorker.Core;

namespace EtcdWorker.UnitTests.App;

// Расписание снапшотов — чистый предикат SnapshotSchedule.ShouldTake:
// плановый (никогда не снимали или интервал истёк) либо внеочередной
// (никогда не загружали, либо ревизия выросла и throttle minIntervalSec выдержан).
public class SnapshotScheduleTests
{
    // Вызов с фиксированными интервалами: плановый — 10 мин (600 с), throttle — 300 с.
    private static bool Should(long now, long? lastTake, long? lastUploaded, long revision, long? exported) =>
        SnapshotSchedule.ShouldTake(now, lastTake, lastUploaded, revision, exported, intervalMin: 10, minIntervalSec: 300);

    [Fact]
    public void NeverTaken_Planned()
    {
        // Arrange — снапшот никогда не снимался; загрузки и ревизии «в порядке»,
        // чтобы true мог объяснить только плановый повод
        long now = 1_000_000;
        long? lastTake = null;
        long? lastUploaded = now - 10;
        const long revision = 7;
        const long exported = 7;

        // Act
        var should = Should(now, lastTake, lastUploaded, revision, exported);

        // Assert — плановый: снимаем немедленно
        should.Should().BeTrue();
    }

    [Fact]
    public void WithinInterval_NoChanges_False()
    {
        // Arrange — недавно снимали (60 с < 600 с), ревизия не менялась,
        // последняя загрузка свежая (10 с < 300 с)
        long now = 1_000_000;
        long? lastTake = now - 60;
        long? lastUploaded = now - 10;
        const long revision = 7;
        const long exported = 7;

        // Act
        var should = Should(now, lastTake, lastUploaded, revision, exported);

        // Assert — ни планового, ни внеочередного повода нет
        should.Should().BeFalse();
    }

    [Fact]
    public void WithinInterval_RevisionGrewAndUploadStale_True()
    {
        // Arrange — внутри планового интервала, но ревизия выросла (8 > 7),
        // а последняя загрузка застарела (ровно 300 с — throttle выдержан)
        long now = 1_000_000;
        long? lastTake = now - 60;
        long? lastUploaded = now - 300;
        const long revision = 8;
        const long exported = 7;

        // Act
        var should = Should(now, lastTake, lastUploaded, revision, exported);

        // Assert — внеочередной снапшот нужен
        should.Should().BeTrue();
    }

    [Fact]
    public void WithinInterval_RevisionGrewButUploadFresh_False()
    {
        // Arrange — ревизия выросла (8 > 7), но загрузка свежая
        // (299 с < 300 с — throttle ещё не выдержан)
        long now = 1_000_000;
        long? lastTake = now - 60;
        long? lastUploaded = now - 299;
        const long revision = 8;
        const long exported = 7;

        // Act
        var should = Should(now, lastTake, lastUploaded, revision, exported);

        // Assert — ждём истечения minIntervalSec
        should.Should().BeFalse();
    }

    [Fact]
    public void NoUploadEver_True()
    {
        // Arrange — внутри планового интервала, ревизия выросла,
        // но ни одного снапшота ещё не загружали
        long now = 1_000_000;
        long? lastTake = now - 60;
        long? lastUploaded = null;
        const long revision = 8;
        const long exported = 7;

        // Act
        var should = Should(now, lastTake, lastUploaded, revision, exported);

        // Assert — внеочередной: грузим безусловно
        should.Should().BeTrue();
    }

    [Fact]
    public void IntervalElapsed_True()
    {
        long now = 1_000_000;

        // Arrange — граница планового: ровно intervalMin*60 = 600 с с последнего снятия,
        // изменений нет — внеочередной повод исключён
        const long revision = 7;
        const long exported = 7;
        long? lastTake = now - 600;
        long? lastUploaded = now - 300;

        // Act
        var plannedBoundary = Should(now, lastTake, lastUploaded, revision, exported);

        // Assert — ровно на границе интервала снимаем
        plannedBoundary.Should().BeTrue();

        // Arrange — граница внеочередного: внутри планового интервала (60 с < 600 с),
        // ревизия выросла, загрузка ровно minIntervalSec = 300 с назад
        long? recentTake = now - 60;
        long? boundaryUploaded = now - 300;
        const long grewRevision = 8;

        // Act
        var adhocBoundary = Should(now, recentTake, boundaryUploaded, grewRevision, exported);

        // Assert — ровно на границе throttle внеочередной разрешён
        adhocBoundary.Should().BeTrue();
    }
}
