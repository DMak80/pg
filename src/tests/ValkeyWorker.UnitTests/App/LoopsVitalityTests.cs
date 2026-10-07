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
        beats.Should().OnlyContain(b => b.LastActivityAt == null); // циклы ещё не тикали
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
        beats.First(b => b.Name == "reconcile").LastActivityAt.Should().NotBeNull();
        beats.First(b => b.Name == "keepalive").LastActivityAt.Should().BeNull();
        health.Snapshot().LastReconcileActivity.Should().NotBeNull(); // MarkReconcileTick обновляет и тик, и активность
    }

    [Fact]
    public void Snapshot_ReconcileActivity_UpdatedWithoutTick()
    {
        // Arrange: долгая итерация — только прогресс-отметка, тика нет
        var health = new HealthState(TimeProvider.System);
        health.MarkReconcileActivity();
        var sut = new ValkeyWorkerLoopsVitality(Options, health);

        // Act
        var beats = sut.Snapshot();

        // Assert: активность свежая (watchdog не firing), тик остался null
        // (healthz loops-alive по тикам — Degraded, HTTP-семантика не меняется)
        beats.First(b => b.Name == "reconcile").LastActivityAt.Should().NotBeNull();
        health.Snapshot().LastReconcileTick.Should().BeNull();
        health.Snapshot().LastReconcileActivity.Should().NotBeNull();
    }
}
