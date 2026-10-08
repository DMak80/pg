namespace Shared.Core.Hosting;

/// <summary>Пульсирующий сон: законные ожидания длиннее порога сноса watchdog —
/// чанками короче окна проверки (CheckIntervalSec; по умолчанию половина окна),
/// каждый чанк — колбэк-отметка активности БЕЗ тика (healthz loops-alive
/// семантику не меняет). Перенос паттерна
/// BackupOrphanSweeperLoop.DelayTickingAsync в общий код.</summary>
public static class PulsingDelay
{
    public static async Task SleepAsync(TimeSpan total, TimeSpan checkWindow, Action pulse, CancellationToken ct)
    {
        var chunk = TimeSpan.FromTicks(Math.Max(TimeSpan.TicksPerSecond, checkWindow.Ticks / 2));
        var remaining = total;
        while (remaining > TimeSpan.Zero && !ct.IsCancellationRequested)
        {
            var step = chunk < remaining ? chunk : remaining;
            await Task.Delay(step, ct);
            remaining -= step;
            pulse();
        }
    }
}
