using EtcdWorker.App;
using Microsoft.Extensions.Options;

namespace EtcdWorker.UnitTests.App;

/// <summary>Витальность циклов для watchdog (arch/etcdworker/06-operability.md §3): цикл в законном
/// длинном ожидании обязан считаться живым по пульсу/прогресс-отметке —
/// снапшот-цикл при отстающей выгрузке спит RetryIntervalSec пульсирующим
/// сном (PulsingDelay + MarkSnapshotActivity); reconcile в долгом
/// health-ожидании провижининга узла жив прогресс-отметками ILoopProgress
/// (EtcdNodeProvisioner); застрявшая после первого тика отметка игнорирует
/// пульс — self-restart на живом контуре.</summary>
public class EtcdWorkerLoopsVitalityTests
{
    // Компактный IOptionsMonitor с фиксированным значением (без внешних пакетов).
    private sealed class FixedOptionsMonitor<T>(T value) : IOptionsMonitor<T>
    {
        public T CurrentValue => value;

        public T Get(string? name) => value;

        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }

    private static EtcdWorkerLoopsVitality Vitality(HealthState health)
        => new(new FixedOptionsMonitor<EtcdWorkerOptions>(new EtcdWorkerOptions()), health);

    // AAA: тик был раньше пульса сна — heartbeat обязан видеть пульс, а не
    // застрявший тик (квант таймера не даёт «после тика» бесплатно — ждём
    // строго большую отметку до пульса).
    [Fact]
    public void Snapshot_Heartbeat_SeesPulseActivityAfterTick()
    {
        // Arrange: тик снапшот-цикла в T0; пульс Retry-сна — строго позже тика
        // (сон RetryIntervalSec при отстающем статусе выгрузки, arch/etcdworker/00-etcd-contract.md).
        var health = new HealthState(TimeProvider.System);
        var vitality = Vitality(health);
        health.MarkSnapshotTick();
        var tickMark = health.Snapshot().LastSnapshotTick!.Value;
        DateTimeOffset afterTick;
        do
        {
            afterTick = DateTimeOffset.UtcNow;
        }
        while (afterTick <= tickMark); // строго позже тика (кванты таймера)
        Thread.Sleep(50);

        // Act: пульс сна посреди длинного ожидания
        health.MarkSnapshotActivity();
        var heartbeat = vitality.Snapshot().First(h => h.Name == "snapshot");

        // Assert: активность heartbeat — не застрявший тик: цикл жив пульсом
        heartbeat.LastActivityAt.Should().BeOnOrAfter(afterTick,
            "a snapshot loop sleeping RetryIntervalSec with a pulsing delay is alive by its pulse mark");
    }

    // AAA: прогресс-отметка долгого health-ожидания провижининга
    // (ILoopProgress.Mark из EtcdNodeProvisioner) держит витальность
    // reconcile — тик старше, активность новее.
    [Fact]
    public void Reconcile_Heartbeat_SeesProgressMarkInsideHealthWait()
    {
        // Arrange: тик reconcile в T0 (запуск провижининга узла); узел грузится
        // дольше порога сноса — health-поллинг маркирует каждую итерацию.
        var health = new HealthState(TimeProvider.System);
        Shared.Core.Hosting.ILoopProgress progress = health;
        var vitality = Vitality(health);
        health.MarkReconcileTick(ok: true, claimsHeld: 1);
        var tickMark = health.Snapshot().LastReconcileTick!.Value;
        DateTimeOffset afterTick;
        do
        {
            afterTick = DateTimeOffset.UtcNow;
        }
        while (afterTick <= tickMark); // строго позже тика (кванты таймера)
        Thread.Sleep(50);

        // Act: итерация health-ожидания внутри долгой фазы провижининга
        progress.Mark();
        var heartbeat = vitality.Snapshot().First(h => h.Name == "reconcile");

        // Assert: активность — отметка итерации, а не застрявший тик
        heartbeat.LastActivityAt.Should().BeOnOrAfter(afterTick,
            "a reconcile loop polling node health inside NodeBootSec budget is alive by its progress mark");
    }

    // AAA: без пульса heartbeat равен тику (пульс не нужен).
    [Fact]
    public void Snapshot_Heartbeat_TickOnlyEqualsTick()
    {
        // Arrange: единственная отметка — тик
        var health = new HealthState(TimeProvider.System);
        var vitality = Vitality(health);
        health.MarkSnapshotTick();
        var tickMark = health.Snapshot().LastSnapshotTick!.Value;

        // Act
        var heartbeat = vitality.Snapshot().First(h => h.Name == "snapshot");

        // Assert
        heartbeat.LastActivityAt.Should().Be(tickMark);
    }

    // AAA: без пульса и тика heartbeat остаётся null (стартовый пробел).
    [Fact]
    public void Snapshot_Heartbeat_NullBeforeAnyMark()
    {
        // Arrange: свежий HealthState без отметок
        var health = new HealthState(TimeProvider.System);
        var vitality = Vitality(health);

        // Act
        var heartbeat = vitality.Snapshot().First(h => h.Name == "snapshot");

        // Assert
        heartbeat.LastActivityAt.Should().BeNull();
    }
}
