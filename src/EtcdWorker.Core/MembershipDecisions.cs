namespace EtcdWorker.Core;

/// <summary>Чистые решения доводки состава (arch/25 §3.1).</summary>
public static class MembershipDecisions
{
    /// <summary>Номер нового узла: максимальный существующий + 1 (имена не
    /// переиспользуются, сквозная нумерация); пусто → 1.</summary>
    public static int NextNumber(IReadOnlyList<int> existingNumbers)
        => existingNumbers.Count == 0 ? 1 : existingNumbers.Max() + 1;
}
