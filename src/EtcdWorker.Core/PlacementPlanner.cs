using Shared.Core;

namespace EtcdWorker.Core;

/// <summary>Размещение managed-узла (arch/25 §2; Swarm): анти-аффинити —
/// swarm-ноды без managed-узлов; исчерпаны → least-loaded (минимум занятых
/// слотов), tie-break — имя Ordinal (детерминизм).</summary>
public static class PlacementPlanner
{
    public static Result<string> PickHost(
        IReadOnlyList<string> swarmNodes, IReadOnlyList<string> placedOnExisting)
    {
        if (swarmNodes.Count == 0)
            return Result<string>.Failed(new ApplicationException(
                "placement: no swarm nodes available (arch/25 §2)"));

        var free = swarmNodes
            .Where(n => !placedOnExisting.Contains(n, StringComparer.Ordinal))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();
        if (free.Count > 0)
            return Result<string>.Success(free[0]);

        var leastLoaded = swarmNodes
            .GroupBy(n => placedOnExisting.Count(p => string.Equals(p, n, StringComparison.Ordinal)))
            .OrderBy(g => g.Key)
            .First()
            .OrderBy(n => n, StringComparer.Ordinal)
            .First();
        return Result<string>.Success(leastLoaded);
    }
}
