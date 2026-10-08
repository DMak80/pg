namespace Shared.Core.HealthChecks;

/// <summary>Пороги staleness циклов воркера — потребитель ТОЛЬКО healthz
/// loops-alive (Degraded-окно); watchdog порог сноса вычисляет от собственных
/// опций (Multiplier × CheckIntervalSec) и эти формулы не читает.</summary>
public static class LoopStaleness
{
    /// <summary>Быстрые циклы (reconcile/keepalive/orphan-sweep):
    /// 3 × max(scan, keepalive) + 15 секунд.</summary>
    public static TimeSpan FastLoops(int scanIntervalSec, int keepaliveSec)
        => TimeSpan.FromSeconds(3 * Math.Max(scanIntervalSec, keepaliveSec) + 15);

    /// <summary>Snapshot-цикл: между тиками лидер спит SnapshotIntervalMin —
    /// 3 × max(scan, 60 × interval) + 15 секунд.</summary>
    public static TimeSpan SnapshotLoop(int scanIntervalSec, int snapshotIntervalMin)
        => TimeSpan.FromSeconds(3 * Math.Max(scanIntervalSec, 60 * snapshotIntervalMin) + 15);
}
