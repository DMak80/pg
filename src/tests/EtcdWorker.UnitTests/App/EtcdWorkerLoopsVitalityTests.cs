using EtcdWorker.App;
using Microsoft.Extensions.Options;

namespace EtcdWorker.UnitTests.App;

/// <summary>Витальность циклов для watchdog (arch/etcdworker/06-operability.md §3): цикл в законном
/// длинном ожидании обязан считаться живым по пульсу/прогресс-отметке —
/// reconcile в долгом health-ожидании провижининга узла жив прогресс-отметками
/// ILoopProgress (EtcdNodeProvisioner); застрявшая после первого тика отметка
/// игнорирует пульс — self-restart на живом контуре. DR-слепки — у агентов
/// EtcdNode, снапшот-цикла в воркере нет.</summary>
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
}
