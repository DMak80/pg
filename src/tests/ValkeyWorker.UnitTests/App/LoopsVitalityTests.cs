using ValkeyWorker.App;

namespace ValkeyWorker.UnitTests.App;

// Перечень циклов ValkeyWorker для watchdog: все 3 цикла, пороги = формулы
// LoopStaleness × Watchdog:Multiplier (дефолт 2), null-отметки до старта циклов.
public sealed class LoopsVitalityTests
{
    private static readonly FixedOptionsMonitor Options = new(new ValkeyWorkerOptions
    {
        Loops = new LoopsOptions { ScanIntervalSec = 5, KeepaliveSec = 5, SnapshotIntervalMin = 360 },
    });

    [Fact]
    public void Snapshot_AllThreeLoops_WithThresholds()
    {
        // Arrange
        var health = new HealthState(TimeProvider.System);
        var sut = new ValkeyWorkerLoopsVitality(Options, health);

        // Act
        var beats = sut.Snapshot();

        // Assert: перечень §4.6 — 3 цикла, пороги ×2 от порогов healthz
        beats.Select(b => b.Name).Should().Equal("reconcile", "keepalive", "snapshot");
        beats.First(b => b.Name == "reconcile").StaleAfter.Should().Be(TimeSpan.FromSeconds(60));
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
        var sut = new ValkeyWorkerLoopsVitality(Options, health);

        // Act
        var beats = sut.Snapshot();

        // Assert: отметки HealthState — единственный источник живости
        beats.First(b => b.Name == "reconcile").LastTickAt.Should().NotBeNull();
        beats.First(b => b.Name == "keepalive").LastTickAt.Should().BeNull();
    }
}
