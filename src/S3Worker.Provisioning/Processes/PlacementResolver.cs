using S3Worker.Core.Model;
using S3Worker.Docker.Drivers;

namespace S3Worker.Provisioning.Processes;

/// <summary>
/// Резолвер размещения ноды (arch/23 §4): селектор placement-заявки — предикат
/// равенства по атрибутам хостов (конъюнкция всех указанных: docker_host →
/// Name plain-таблицы, swarm_node → Name (hostname), swarm_labels → подмножество
/// Labels). Кандидаты сортируются least-loaded (UsedSlots, потом имя Ordinal) —
/// детерминизм повторных проходов; ноль кандидатов → Failed (transient-фейл
/// фазы P1, перечень кандидатов — пустой).
/// </summary>
public static class PlacementResolver
{
    public static Result<string> Resolve(
        S3NodePlacement selector,
        IReadOnlyList<S3HostAttributes> hosts,
        out IReadOnlyList<string> candidates)
    {
        var matched = hosts.Where(h => Matches(selector, h)).ToList();
        candidates = matched
            .OrderBy(h => h.UsedSlots)
            .ThenBy(h => h.Name, StringComparer.Ordinal)
            .Select(h => h.Name)
            .ToList();

        if (candidates.Count == 0)
            return Result<string>.Failed(new ApplicationException(
                "placement: ноль кандидатов по селектору (см. журнал фазы P1)"));

        return Result<string>.Success(candidates[0]);
    }

    // Конъюнкция равенств: указанный атрибут обязан сойтись; не указанный —
    // не сужает выбор. swarm_labels — подмножество меток хоста.
    private static bool Matches(S3NodePlacement selector, S3HostAttributes host)
    {
        if (selector.DockerHost is { Length: > 0 } dockerHost
            && !string.Equals(host.Name, dockerHost, StringComparison.Ordinal))
            return false;

        if (selector.SwarmNode is { Length: > 0 } swarmNode
            && !string.Equals(host.Name, swarmNode, StringComparison.Ordinal))
            return false;

        if (selector.SwarmLabels is { Count: > 0 } labels
            && labels.Any(l => !host.Labels.TryGetValue(l.Key, out var v)
                || !string.Equals(v, l.Value, StringComparison.Ordinal)))
            return false;

        return true;
    }
}
