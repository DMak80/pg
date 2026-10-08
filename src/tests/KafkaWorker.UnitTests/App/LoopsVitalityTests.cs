using KafkaWorker.App;

namespace KafkaWorker.UnitTests.App;

// Перечень циклов KafkaWorker для watchdog: все 3 цикла, единый порог сноса
// = Watchdog:Multiplier × Watchdog:CheckIntervalSec (30 c при дефолтах),
// null-отметки до старта циклов.
public sealed class LoopsVitalityTests
{
    private static readonly FixedOptionsMonitor Options = new(new KafkaWorkerOptions
    {
        Loops = new LoopsOptions { ScanIntervalSec = 5, KeepaliveSec = 5, SnapshotIntervalMin = 360 },
    });

    [Fact]
    public void Snapshot_AllThreeLoops_StaleAfterFromWatchdogOptions()
    {
        // Arrange: дефолты Loops (scan=5, keepalive=5, snapshot=360 мин) — прежде
        // давали fast=60 c / snapshot-часы; теперь порог единый 2×15=30 c
        var health = new HealthState(TimeProvider.System);
        var sut = new KafkaWorkerLoopsVitality(Options, health);

        // Act
        var beats = sut.Snapshot();

        // Assert: StaleAfter = Multiplier × CheckIntervalSec у ВСЕХ циклов (критерий 6)
        beats.Select(b => b.Name).Should().Equal("reconcile", "keepalive", "snapshot");
        beats.Should().OnlyContain(b => b.StaleAfter == TimeSpan.FromSeconds(30));
        beats.Should().OnlyContain(b => b.LastActivityAt == null); // циклы ещё не тикали
    }

    [Fact]
    public void Snapshot_ПорогНеЗависитОтИнтерваловЦиклов_E2E_30_а_не_36()
    {
        // Arrange: E2E-параметры scan=1/keepalive=1 — прежняя формула давала
        // 2×(3×1+15)=36 c (диагноз: healthz-порог 18 в формуле); новая — 30 c
        var options = new FixedOptionsMonitor(new KafkaWorkerOptions
        {
            Loops = new LoopsOptions { ScanIntervalSec = 1, KeepaliveSec = 1, SnapshotIntervalMin = 360 },
        });
        var sut = new KafkaWorkerLoopsVitality(options, new HealthState(TimeProvider.System));

        // Act
        var beats = sut.Snapshot();

        // Assert: порог от опций watchdog, не от интервалов циклов
        beats.Should().OnlyContain(b => b.StaleAfter == TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void Snapshot_КастомныйПорог_ОтОпцийWatchdog()
    {
        // Arrange: Multiplier=3, CheckIntervalSec=20 → порог 60 c (конфигурируемость
        // прежняя — окном и множителем, без healthz)
        var options = new FixedOptionsMonitor(new KafkaWorkerOptions
        {
            Loops = new LoopsOptions
            {
                ScanIntervalSec = 5, KeepaliveSec = 5,
                Watchdog = new Shared.Core.Hosting.WatchdogOptions { Multiplier = 3, CheckIntervalSec = 20 },
            },
        });
        var sut = new KafkaWorkerLoopsVitality(options, new HealthState(TimeProvider.System));

        // Act/Assert
        sut.Snapshot().Should().OnlyContain(b => b.StaleAfter == TimeSpan.FromSeconds(60));
    }

    [Fact]
    public void Snapshot_ПульсСнаБезТика_ЖивостьБезТика()
    {
        // Arrange: сон snapshot-лидера — только пульс активности, тика нет
        var health = new HealthState(TimeProvider.System);
        health.MarkSnapshotActivity();
        var sut = new KafkaWorkerLoopsVitality(Options, health);

        // Act
        var beats = sut.Snapshot();

        // Assert: активность snapshot-цикла свежая (watchdog не firing), тик
        // остался null — healthz loops-alive по тикам, семантика не меняется
        beats.First(b => b.Name == "snapshot").LastActivityAt.Should().NotBeNull();
        health.Snapshot().LastSnapshotTick.Should().BeNull();
        health.Snapshot().LastSnapshotActivity.Should().NotBeNull();
    }

    // Собственный FakeTimeProvider (как в PgWorker-файле: новый пакет НЕ тащим).
    private sealed class FakeTimeProvider : TimeProvider
    {
        public DateTimeOffset Now;
        public override DateTimeOffset GetUtcNow() => Now;
    }

    // Зеркало PgWorker-кейса «позднейший факт»: пульс сна, затем тик снимка
    // ПОЗЖЕ пульса — виталити отдаёт тик (FakeTimeProvider: метки «позже»
    // гарантированы явным продвижением часов, не реальным временем).
    [Fact]
    public void Snapshot_ТикПозднееПульса_ПозднейшийФакт()
    {
        // Arrange: пульс сна, затем тик снимка ПОЗЖЕ пульса — виталити отдаёт тик
        var clock = new FakeTimeProvider { Now = DateTimeOffset.UnixEpoch.AddHours(1) };
        var health = new HealthState(clock);
        health.MarkSnapshotActivity();
        clock.Now = clock.Now.AddMinutes(10); // тик позже пульса
        health.MarkSnapshotTick();
        var sut = new KafkaWorkerLoopsVitality(Options, health);

        // Act
        var beats = sut.Snapshot();

        // Assert: позднейший факт — тик (не пульс)
        beats.First(b => b.Name == "snapshot").LastActivityAt.Should().Be(health.Snapshot().LastSnapshotTick);
    }

    [Fact]
    public void Snapshot_PassesHealthStateTicks()
    {
        // Arrange
        var health = new HealthState(TimeProvider.System);
        health.MarkReconcileTick(ok: true, claimsHeld: 0);
        var sut = new KafkaWorkerLoopsVitality(Options, health);

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
        var sut = new KafkaWorkerLoopsVitality(Options, health);

        // Act
        var beats = sut.Snapshot();

        // Assert: активность свежая (watchdog не firing), тик остался null
        // (healthz loops-alive по тикам — Degraded, HTTP-семантика не меняется)
        beats.First(b => b.Name == "reconcile").LastActivityAt.Should().NotBeNull();
        health.Snapshot().LastReconcileTick.Should().BeNull();
        health.Snapshot().LastReconcileActivity.Should().NotBeNull();
    }
}
