using PgWorker.Core.Model;

namespace PgWorker.Core.Planning;

/// <summary>
/// Аллокатор портов нод (spec §6.3, Д5): каждой ноде — тройка
/// pg=base / doorman=base+1500 / patroni=base+3000 из диапазона конфига.
/// Закреплённые в /pgworker/portalloc адреса переиспользуются (переживают
/// rebuild); конфликт или отсутствие свободного base — сдвиг к следующему;
/// диапазон исчерпан — Result.Failed. Ключ результата — "shard/node"
/// (формат значения /pgworker/portalloc/&lt;C&gt;).
/// </summary>
public static class PortAllocator
{
    public static Result<IReadOnlyDictionary<string, NodeAddress>> Allocate(
        PlacementPlan plan,
        IReadOnlyDictionary<string, NodeAddress> existing,
        IReadOnlySet<(string Host, int Port)> busy,
        int rangeFrom,
        int rangeTo)
    {
        var result = new Dictionary<string, NodeAddress>();
        // Порты, выделенные этим вызовом: кандидаты не должны пересекаться
        // не только с busy, но и между собой.
        var taken = new HashSet<(string Host, int Port)>(busy);

        foreach (var placement in plan.Nodes)
        {
            var key = $"{placement.Shard}/{placement.Node}";

            // Закреплённый адрес переиспользуется, если нода на том же хосте
            // и порты никто не занял.
            if (existing.TryGetValue(key, out var pinned)
                && pinned.Host == placement.Host
                && IsFree(pinned, taken))
            {
                MarkTaken(pinned, taken);
                result[key] = pinned;
                continue;
            }

            // Новый base: первый свободный с шагом 1 (все 3 порта свободны).
            var allocated = false;
            for (var port = rangeFrom; port < rangeTo; port++)
            {
                var candidate = new NodeAddress(
                    placement.Host, new NodePorts(port, port + 3000, port + 1500));
                if (!IsFree(candidate, taken))
                    continue;

                MarkTaken(candidate, taken);
                result[key] = candidate;
                allocated = true;
                break;
            }

            if (!allocated)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(
                    new InvalidOperationException(
                        $"PortAllocator: нет свободной тройки портов на хосте {placement.Host} " +
                        $"в диапазоне [{rangeFrom},{rangeTo}) для ноды {key}"));
        }

        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(result);
    }

    // Все три порта адреса свободны (не заняты docker и этим вызовом).
    private static bool IsFree(NodeAddress addr, IReadOnlySet<(string Host, int Port)> taken) =>
        !taken.Contains((addr.Host, addr.Ports.Pg))
        && !taken.Contains((addr.Host, addr.Ports.Patroni))
        && !taken.Contains((addr.Host, addr.Ports.Doorman));

    private static void MarkTaken(NodeAddress addr, ISet<(string Host, int Port)> taken)
    {
        taken.Add((addr.Host, addr.Ports.Pg));
        taken.Add((addr.Host, addr.Ports.Patroni));
        taken.Add((addr.Host, addr.Ports.Doorman));
    }
}
