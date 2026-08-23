using PgWorker.Core.Model;

namespace PgWorker.Core.Planning;

/// <summary>Хост размещения: имя + занятые слоты (ноды всех кластеров).</summary>
public sealed record HostInfo(string Name, int UsedSlots);

/// <summary>Назначение ноды шарда на хост.</summary>
public sealed record NodePlacement(string Shard, string Node, string Host);

/// <summary>План размещения кластера: по записи на каждую плановую ноду.</summary>
public sealed record PlacementPlan(IReadOnlyList<NodePlacement> Nodes);

/// <summary>
/// Планировщик размещения нод по docker-хостам (spec §6.3, Д5): анти-аффинити —
/// ноды одного шарда на разных хостах, если hosts.Count ≥ replicas; иначе —
/// равномерно least-loaded. Детерминизм: шарды/ноды обрабатываются по имени,
/// кандидаты сортируются по (загрузка, имя).
/// </summary>
public static class PlacementPlanner
{
    public static PlacementPlan Plan(IReadOnlyList<ShardSpec> shards, IReadOnlyList<HostInfo> hosts)
    {
        if (hosts.Count == 0)
            throw new InvalidOperationException("PlacementPlanner: список docker-хостов пуст");

        // Текущая загрузка: исходные UsedSlots + уже размещённые этим планом ноды.
        var load = hosts.ToDictionary(h => h.Name, h => h.UsedSlots);
        var placements = new List<NodePlacement>();

        foreach (var shard in shards.OrderBy(s => s.Name))
        {
            // Хосты, уже занятые этим шардом текущим планом (анти-аффинити).
            var takenByShard = new HashSet<string>();

            foreach (var node in shard.Nodes.OrderBy(n => n.Name))
            {
                // Кандидаты — хосты, ещё не занятые этим шардом, least-loaded;
                // если топология не позволяет — наименее загруженный хост.
                var host = hosts
                   .Where(h => !takenByShard.Contains(h.Name))
                   .OrderBy(h => load[h.Name])
                   .ThenBy(h => h.Name)
                   .FirstOrDefault()
                 ?? hosts
                       .OrderBy(h => load[h.Name])
                       .ThenBy(h => h.Name)
                       .First();

                placements.Add(new NodePlacement(shard.Name, node.Name, host.Name));
                load[host.Name]++;
                takenByShard.Add(host.Name);
            }
        }

        return new PlacementPlan(placements);
    }
}
