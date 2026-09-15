namespace Shared.Core.Planning;

/// <summary>Хост размещения: имя + занятые слоты (ноды всех кластеров).</summary>
public sealed record HostInfo(string Name, int UsedSlots);

/// <summary>Группа нод с анти-аффинитетом (Pg — шард; Kfw — кластер).</summary>
public sealed record NodeGroup(string Name, IReadOnlyList<string> Nodes);

/// <summary>Назначение ноды группы на хост.</summary>
public sealed record NodePlacement(string Group, string Node, string Host);

/// <summary>План размещения: по записи на каждую плановую ноду.</summary>
public sealed record PlacementPlan(IReadOnlyList<NodePlacement> Nodes);

/// <summary>
/// Планировщик размещения по docker-хостам (t09, обобщение Pg/Kfw): анти-аффинити —
/// ноды одной группы на разных хостах, если позволяет топология; иначе least-loaded.
/// Детерминизм: группы/ноды/кандидаты сортируются StringComparer.Ordinal.
/// </summary>
public static class PlacementPlanner
{
    public static PlacementPlan Plan(IReadOnlyList<NodeGroup> groups, IReadOnlyList<HostInfo> hosts)
    {
        if (hosts.Count == 0)
            throw new InvalidOperationException("PlacementPlanner: список docker-хостов пуст");

        // Текущая загрузка: исходные UsedSlots + уже размещённые этим планом ноды.
        var load = hosts.ToDictionary(h => h.Name, h => h.UsedSlots);
        var placements = new List<NodePlacement>();

        foreach (var group in groups.OrderBy(g => g.Name, StringComparer.Ordinal))
        {
            // Хосты, уже занятые этой группой текущим планом (анти-аффинити).
            var takenByGroup = new HashSet<string>();

            foreach (var node in group.Nodes.OrderBy(n => n, StringComparer.Ordinal))
            {
                // Кандидаты — хосты, ещё не занятые группой, least-loaded;
                // если топология не позволяет — наименее загруженный хост.
                var host = hosts
                   .Where(h => !takenByGroup.Contains(h.Name))
                   .OrderBy(h => load[h.Name])
                   .ThenBy(h => h.Name)
                   .FirstOrDefault()
                 ?? hosts
                       .OrderBy(h => load[h.Name])
                       .ThenBy(h => h.Name)
                       .First();

                placements.Add(new NodePlacement(group.Name, node, host.Name));
                load[host.Name]++;
                takenByGroup.Add(host.Name);
            }
        }

        return new PlacementPlan(placements);
    }
}
