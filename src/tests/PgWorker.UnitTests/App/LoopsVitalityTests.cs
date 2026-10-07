using PgWorker.App;

namespace PgWorker.UnitTests.App;

// Перечень циклов PgWorker для watchdog: все 4 цикла, пороги = формулы
// LoopStaleness × Watchdog:Multiplier (дефолт 2), null-отметки до старта циклов.
public sealed class LoopsVitalityTests
{
    private static readonly FixedOptionsMonitor Options = new(new PgWorkerOptions
    {
        Loops = new LoopsOptions { ScanIntervalSec = 5, KeepaliveSec = 5, SnapshotIntervalMin = 360 },
    });

    [Fact]
    public void Snapshot_AllFourLoops_WithThresholds()
    {
        // Arrange
        var health = new HealthState(TimeProvider.System);
        var sut = new PgWorkerLoopsVitality(Options, health);

        // Act
        var beats = sut.Snapshot();

        // Assert: перечень §4.6 — 4 цикла, пороги ×2 от порогов healthz
        beats.Select(b => b.Name).Should().Equal("reconcile", "keepalive", "snapshot", "orphan-sweep");
        beats.First(b => b.Name == "reconcile").StaleAfter.Should().Be(TimeSpan.FromSeconds(60));
        beats.First(b => b.Name == "orphan-sweep").StaleAfter.Should().Be(TimeSpan.FromSeconds(60));
        beats.First(b => b.Name == "snapshot").StaleAfter
            .Should().Be(TimeSpan.FromSeconds((3 * Math.Max(5, 60 * 360) + 15) * 2));
        beats.Should().OnlyContain(b => b.LastTickAt == null); // циклы ещё не тикали
    }

    [Fact]
    public void Snapshot_PassesHealthStateTicks()
    {
        // Arrange
        var health = new HealthState(TimeProvider.System);
        health.MarkReconcileTick(ok: true, claimsHeld: 0);
        var sut = new PgWorkerLoopsVitality(Options, health);

        // Act
        var beats = sut.Snapshot();

        // Assert: отметки HealthState — единственный источник живости
        beats.First(b => b.Name == "reconcile").LastTickAt.Should().NotBeNull();
        beats.First(b => b.Name == "keepalive").LastTickAt.Should().BeNull();
    }
}
