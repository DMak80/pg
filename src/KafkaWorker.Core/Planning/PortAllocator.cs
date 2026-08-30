using KafkaWorker.Core.Model;

namespace KafkaWorker.Core.Planning;

/// <summary>
/// Аллокатор клиентских портов брокеров (arch/16 §2.1): каждой ноде — ОДИН порт
/// из диапазона конфига (16000–16999). Закреплённые в /kafkaworker/portalloc
/// адреса переиспользуются (переживают rebuild); конфликт или отсутствие
/// свободного порта — сдвиг к следующему; диапазон исчерпан — Result.Failed.
/// Упрощение порта PgWorker: тройка pg/patroni/doorman не нужна — у kafka-ноды
/// наружу публикуется только CLIENT-listener.
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
            // Закреплённый адрес переиспользуется, если нода на том же хосте
            // и порт никто не занял.
            if (existing.TryGetValue(placement.Node, out var pinned)
                && pinned.Host == placement.Host
                && !taken.Contains((pinned.Host, pinned.ClientPort)))
            {
                taken.Add((pinned.Host, pinned.ClientPort));
                result[placement.Node] = pinned;
                continue;
            }

            // Новый порт: первый свободный с шагом 1.
            var allocated = false;
            for (var port = rangeFrom; port < rangeTo; port++)
            {
                if (taken.Contains((placement.Host, port)))
                    continue;

                taken.Add((placement.Host, port));
                result[placement.Node] = new NodeAddress(placement.Host, port);
                allocated = true;
                break;
            }

            if (!allocated)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(
                    new InvalidOperationException(
                        $"PortAllocator: нет свободного порта на хосте {placement.Host} " +
                        $"в диапазоне [{rangeFrom},{rangeTo}) для ноды {placement.Node}"));
        }

        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(result);
    }
}
