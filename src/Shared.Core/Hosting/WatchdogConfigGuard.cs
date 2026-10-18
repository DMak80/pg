namespace Shared.Core.Hosting;

/// <summary>Fail-fast инвариант живости старта: тики быстрых циклов
/// (reconcile/keepalive) обязаны приходить чаще ПОРОГА СНОСА watchdog
/// (Multiplier × CheckIntervalSec = 30 c при дефолтах) — порог фиксирован
/// опциями watchdog и, в отличие от прежней healthz-зависимой формулы, не
/// растёт вместе с интервалами циклов: кастомный scan ≥ 30 с дал бы
/// гарантированный ложный само-снос. Выравнивание — увеличить
/// CheckIntervalSec/Multiplier.</summary>
public static class WatchdogConfigGuard
{
    public static void EnsureFastLoopsBelowStaleThreshold(
        int scanIntervalSec, int keepaliveSec, WatchdogOptions watchdog)
    {
        var threshold = TimeSpan.FromSeconds(
            Math.Max(1, watchdog.Multiplier) * Math.Max(1, watchdog.CheckIntervalSec));
        var slowest = TimeSpan.FromSeconds(Math.Max(1, Math.Max(scanIntervalSec, keepaliveSec)));
        if (slowest >= threshold)
            throw new ApplicationException(
                $"Loops:ScanIntervalSec/KeepaliveSec ({slowest.TotalSeconds:F0} s) must be LESS than the watchdog teardown threshold " +
                $"({threshold.TotalSeconds:F0} s = Multiplier({watchdog.Multiplier}) × CheckIntervalSec({watchdog.CheckIntervalSec})): " +
                "fast loop ticks must come more often than the threshold — otherwise the watchdog tears down a live worker; increase CheckIntervalSec/Multiplier");
    }
}
