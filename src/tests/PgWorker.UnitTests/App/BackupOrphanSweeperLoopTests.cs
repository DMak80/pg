using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.App;
using PgWorker.App.Loops;
using PgWorker.Backups.Supervisor;
using PgWorker.UnitTests.Provisioning;

namespace PgWorker.UnitTests.App;

// Тики живости BackupOrphanSweeperLoop: каждая итерация отмечается в HealthState
// (порог watchdog у sweeper'а — как у быстрых циклов: сон Supervisor:IntervalSec
// тикает чанками, спящий лидер не «stale»).
public sealed class BackupOrphanSweeperLoopTests
{
    private static (BackupOrphanSweeperLoop Loop, HealthState Health, CancellationTokenSource Cts) Create(
        int scanIntervalSec, int supervisorIntervalSec)
    {
        var etcd = new Fakes.FakeEtcd();
        var options = new FixedOptionsMonitor(new PgWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Loops = new LoopsOptions { ScanIntervalSec = scanIntervalSec },
            Backups = new BackupsOptions { Enabled = false }, // не-лидер/выключено — тихий цикл
        });
        options.CurrentValue.Backups.Supervisor.IntervalSec = supervisorIntervalSec;
        var health = new HealthState(TimeProvider.System);
        var claims = new ClaimStore(
            "/pgworker", options.CurrentValue.Etcd.Endpoints, etcd, TimeProvider.System);
        var sweeper = new BackupOrphanSweeper(
            etcd, options.CurrentValue.Etcd.Endpoints,
            new Fakes.DisabledBackupS3Stub(), claims,
            new WorkJournal("/pgworker", etcd, options.CurrentValue.Etcd.Endpoints),
            () => null, TimeProvider.System, NullLogger<BackupOrphanSweeper>.Instance);
        var loop = new BackupOrphanSweeperLoop(options, claims, sweeper, health,
            NullLogger<BackupOrphanSweeperLoop>.Instance);
        return (loop, health, new CancellationTokenSource());
    }

    [Fact]
    public async Task ExecuteAsync_MarksOrphanSweepTick()
    {
        // Arrange: цикл с нулевым интервалом скана (тик каждой итерацией)
        var (loop, health, cts) = Create(scanIntervalSec: 0, supervisorIntervalSec: 600);

        // Act: запускаем и ждём первой отметки (поллинг ≤5 с, шаг 50 мс)
        await loop.StartAsync(cts.Token);
        for (var i = 0; i < 100 && health.Snapshot().LastOrphanSweepTick is null; i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        cts.Cancel();

        // Assert: sweeper отмечает тики живости (невидим до задачи — AC5)
        health.Snapshot().LastOrphanSweepTick.Should().NotBeNull();
    }

    [Fact]
    public async Task DelayTickingAsync_TicksInChunks_DoesNotSleepWholeInterval()
    {
        // Arrange: лидерный сон 600 c чанками по 1 c — тики без 600-с паузы
        var (loop, health, cts) = Create(scanIntervalSec: 1, supervisorIntervalSec: 600);

        // Act: 2,5 c сна чанками → ≥2 отметки, затем отмена
        var task = loop.DelayTickingAsync(TimeSpan.FromSeconds(600), cts.Token);
        for (var i = 0; i < 50; i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        var ticksInWindow = health.Snapshot().LastOrphanSweepTick;
        cts.Cancel();
        await Task.WhenAny(task, Task.Delay(5000, TestContext.Current.CancellationToken));

        // Assert: за окно ≪ 600 c отметки уже есть — сон не глушит живость
        ticksInWindow.Should().NotBeNull();
    }
}
