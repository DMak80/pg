namespace KafkaWorker.Core.Planning;

/// <summary>Хост размещения: имя + занятые слоты (ноды всех кластеров).</summary>
public sealed record HostInfo(string Name, int UsedSlots);

/// <summary>Назначение брокера на хост.</summary>
public sealed record NodePlacement(string Node, string Host);

/// <summary>План размещения кластера: по записи на каждый брокер.</summary>
public sealed record PlacementPlan(IReadOnlyList<NodePlacement> Nodes);

/// <summary>
/// Планировщик размещения брокеров по docker-хостам (arch/16 §2.1): анти-аффинити —
/// ноды одного кластера на разных хостах, если hosts.Count ≥ nodes; иначе —
/// равномерно least-loaded. Детерминизм: ноды обрабатываются по имени,
/// кандидаты сортируются по (загрузка, имя). Порт PlacementPlanner PgWorker.
/// </summary>
public static class PlacementPlanner
{
    public static PlacementPlan Plan(IReadOnlyList<string> nodes, IReadOnlyList<HostInfo> hosts)
    {
        if (hosts.Count == 0)
            throw new InvalidOperationException("PlacementPlanner: список docker-хостов пуст");

        // Текущая загрузка: исходные UsedSlots + уже размещённые этим планом ноды.
        var load = hosts.ToDictionary(h => h.Name, h => h.UsedSlots);
        var placements = new List<NodePlacement>();

        // Хосты, уже занятые этим кластером текущим планом (анти-аффинити).
        var takenByCluster = new HashSet<string>();

        foreach (var node in nodes.OrderBy(n => n, StringComparer.Ordinal))
        {
            // Кандидаты — хосты, ещё не занятые кластером, least-loaded;
            // если топология не позволяет — наименее загруженный хост.
            var host = hosts
               .Where(h => !takenByCluster.Contains(h.Name))
               .OrderBy(h => load[h.Name])
               .ThenBy(h => h.Name)
               .FirstOrDefault()
             ?? hosts
                   .OrderBy(h => load[h.Name])
                   .ThenBy(h => h.Name)
                   .First();

            placements.Add(new NodePlacement(node, host.Name));
            load[host.Name]++;
            takenByCluster.Add(host.Name);
        }

        return new PlacementPlan(placements);
    }
}
