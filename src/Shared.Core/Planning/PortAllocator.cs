using Shared.Core;

namespace Shared.Core.Planning;

/// <summary>
/// Аллокатор портов (t09, обобщение Pg/Kfw): схема одна — закрепление
/// переиспользуется (нода на том же хосте и все порты адреса свободны),
/// новый адрес — первый свободный base с шагом 1; диапазон исчерпан —
/// Result.Failed. Доменная модель адреса — параметры-делегаты:
/// portsOf (тройка Pg / один порт Kfw), hostOf, makeAddress, keyOf
/// (Pg — "shard/node", Kfw — имя ноды; строки-ключи строит потребитель, spec §4.4).
/// </summary>
public static class PortAllocator
{
    public static Result<IReadOnlyDictionary<string, TAddress>> Allocate<TAddress>(
        PlacementPlan plan,
        IReadOnlyDictionary<string, TAddress> existing,
        IReadOnlySet<(string Host, int Port)> busy,
        int rangeFrom,
        int rangeTo,
        Func<TAddress, IReadOnlyList<int>> portsOf,
        Func<TAddress, string> hostOf,
        Func<string, int, TAddress> makeAddress,
        Func<NodePlacement, string> keyOf)
    {
        var result = new Dictionary<string, TAddress>();
        // Порты, выделенные этим вызовом: кандидаты не должны пересекаться
        // не только с busy, но и между собой.
        var taken = new HashSet<(string Host, int Port)>(busy);

        foreach (var placement in plan.Nodes)
        {
            var key = keyOf(placement);

            // Закреплённый адрес переиспользуется, если нода на том же хосте
            // и порты никто не занял.
            if (existing.TryGetValue(key, out var pinned)
                && hostOf(pinned) == placement.Host
                && IsFree(pinned))
            {
                MarkTaken(pinned);
                result[key] = pinned;
                continue;
            }

            // Новый base: первый свободный с шагом 1 (все порта кандидата свободны).
            var allocated = false;
            for (var port = rangeFrom; port < rangeTo; port++)
            {
                var candidate = makeAddress(placement.Host, port);
                if (!IsFree(candidate))
                    continue;

                MarkTaken(candidate);
                result[key] = candidate;
                allocated = true;
                break;
            }

            if (!allocated)
                return Result<IReadOnlyDictionary<string, TAddress>>.Failed(
                    new InvalidOperationException(
                        $"PortAllocator: нет свободного порта на хосте {placement.Host} " +
                        $"в диапазоне [{rangeFrom},{rangeTo}) для {key}"));
        }

        return Result<IReadOnlyDictionary<string, TAddress>>.Success(result);

        // Все порты адреса свободны (не заняты docker и этим вызовом).
        bool IsFree(TAddress addr) => portsOf(addr).All(p => !taken.Contains((hostOf(addr), p)));

        void MarkTaken(TAddress addr)
        {
            foreach (var p in portsOf(addr))
                taken.Add((hostOf(addr), p));
        }
    }
}
