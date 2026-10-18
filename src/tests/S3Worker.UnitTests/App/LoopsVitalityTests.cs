using S3Worker.App;

namespace S3Worker.UnitTests.App;

// Перечень циклов S3Worker для watchdog: все 3 цикла, единый порог сноса
// = Watchdog:Multiplier × Watchdog:CheckIntervalSec (30 c при дефолтах),
// null-отметки до старта циклов.
public sealed class LoopsVitalityTests
{
    private static readonly FixedOptionsMonitor Options = new(new S3WorkerOptions
    {
        Loops = new LoopsOptions { ScanIntervalSec = 5, KeepaliveSec = 5, SnapshotIntervalMin = 360 },
    });

    [Fact]
    public void Snapshot_AllThreeLoops_StaleAfterFromWatchdogOptions()
    {
        // Arrange: дефолты Loops (scan=5, keepalive=5, snapshot=360 мин)
        var health = new HealthState(TimeProvider.System);
        var sut = new S3WorkerLoopsVitality(Options, health);

        // Act
        var beats = sut.Snapshot();

        // Assert: StaleAfter = Multiplier × CheckIntervalSec у ВСЕХ циклов
        beats.Select(b => b.Name).Should().Equal("reconcile", "keepalive", "snapshot");
        beats.Should().OnlyContain(b => b.StaleAfter == TimeSpan.FromSeconds(30));
        beats.Should().OnlyContain(b => b.LastActivityAt == null); // циклы ещё не тикали
    }

    [Fact]
    public void Snapshot_ThresholdIndependentOfLoopIntervals()
    {
        // Arrange: E2E-параметры scan=1/keepalive=1 — порог от опций watchdog
        var options = new FixedOptionsMonitor(new S3WorkerOptions
        {
            Loops = new LoopsOptions { ScanIntervalSec = 1, KeepaliveSec = 1, SnapshotIntervalMin = 360 },
        });
        var sut = new S3WorkerLoopsVitality(options, new HealthState(TimeProvider.System));

        // Act
        var beats = sut.Snapshot();

        // Assert: порог 2×15=30 c, не от интервалов циклов
        beats.Should().OnlyContain(b => b.StaleAfter == TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void Snapshot_CustomThreshold_FromWatchdogOptions()
    {
        // Arrange: Multiplier=3, CheckIntervalSec=20 → порог 60 c
        var options = new FixedOptionsMonitor(new S3WorkerOptions
        {
            Loops = new LoopsOptions
            {
                Watchdog = new Shared.Core.Hosting.WatchdogOptions { Enabled = true, Multiplier = 3, CheckIntervalSec = 20 },
            },
        });
        var sut = new S3WorkerLoopsVitality(options, new HealthState(TimeProvider.System));

        // Act
        var beats = sut.Snapshot();

        // Assert
        beats.Should().OnlyContain(b => b.StaleAfter == TimeSpan.FromSeconds(60));
    }

    [Fact]
    public void Snapshot_SleepPulse_CountsAsActivity()
    {
        // Arrange: snapshot-пульс сна (активность без тика) — виталити берёт позднейший
        var clock = new FixedTimeProvider { Utc = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero) };
        var health = new HealthState(clock);
        health.MarkSnapshotActivity();
        var sut = new S3WorkerLoopsVitality(Options, health);

        // Act
        var beats = sut.Snapshot();

        // Assert — pulse засчитан активностью snapshot, остальные null
        beats.First(b => b.Name == "snapshot").LastActivityAt.Should().NotBeNull();
        beats.First(b => b.Name == "reconcile").LastActivityAt.Should().BeNull();
    }
}
