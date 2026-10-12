using System.Collections.Concurrent;

namespace S3Worker.Provisioning.Processes;

/// <summary>
/// Бюджет boot-ожидания provisioning (arch/24 §5 A, фаза P2.5): время первого
/// наблюдения «health ноды молчит» по кластеру (диагностический бюджет
/// NodeBootSec, не клэйм). DI-синглтон, общий для процессов-соседей:
/// deprovisioning сбрасывает бюджет при завершении демонтажа, provisioning —
/// при выходе кластера из provisioning (done/aborted-state-changed), —
/// повторный provisioning того же имени в том же процессе воркера начинает
/// отсчёт заново, без мгновенного permanent-фейла по старому since.
/// </summary>
public sealed class BootWaitBudget(TimeProvider clock)
{
    private readonly ConcurrentDictionary<string, long> _since = new();

    // Бюджет истёк: не готов ни через сколько (budgetSec<=0) либо с первого
    // наблюдения прошло больше budgetSec.
    public bool Exceeded(string cluster, int budgetSec)
        => budgetSec <= 0 || NowUnix() - SinceOrStart(cluster) > budgetSec;

    // Сброс (успешный boot, abort provisioning, завершение демонтажа).
    public void Reset(string cluster) => _since.TryRemove(cluster, out _);

    // Первое наблюдение фиксируется, повторные отсчёт не сдвигают.
    private long SinceOrStart(string cluster) => _since.GetOrAdd(cluster, _ => NowUnix());

    private long NowUnix() => clock.GetUtcNow().ToUnixTimeSeconds();
}
