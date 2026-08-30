using KafkaWorker.Core;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>Целевой assignment одной партиции (элемент reassignment.json).</summary>
public sealed record ReassignMove(string Topic, int Partition, IReadOnlyList<int> Replicas);

/// <summary>
/// Чистые функции планов reassignment (arch/16 §5 I; spec t02 §3.3/§3.4):
/// drain (замещение реплик drain-брокера с minISR-guard'ом и автоматическим
/// снижением RF при нехватке целей) и balance (converge к декларации:
/// RF = min(configRf, цели) для юзер-топиков, min(3, цели) для internal,
/// лидер-первая-реплика сохраняется, добор least-loaded). Без побочных
/// эффектов — юнит-тесты без Kafka; детерминизм сортировкой (topic,
/// partition, brokerId) — план стабилен между тиками, осцилляций нет.
/// </summary>
public static class ReassignPlanner
{
    /// <summary>
    /// Drain: для каждой партиции с репликой drainBroker — переезд.
    /// newReplicas = старые без drain (порядок сохранён; неживые брокеры вне
    /// targetBrokerIds не считаются целью) + добор least-loaded из targets до
    /// min(len(old), targets.Count). Инвариант: newReplicas.Count &gt;=
    /// minIsr(topic) — иначе Result.Failed с причиной (spec §5.2 D3).
    /// </summary>
    public static Result<IReadOnlyList<ReassignMove>> PlanDrain(
        IReadOnlyList<KafkaTopicView> topics,
        int drainBrokerId,
        IReadOnlyList<int> targetBrokerIds,
        IReadOnlyDictionary<string, int> minIsrByTopic)
    {
        var load = new Dictionary<int, int>();
        var moves = new List<ReassignMove>();

        foreach (var topic in topics.OrderBy(t => t.Topic, StringComparer.Ordinal))
        {
            for (var partition = 0; partition < topic.ReplicasPerPartition.Count; partition++)
            {
                var old = topic.ReplicasPerPartition[partition];
                if (!old.Contains(drainBrokerId))
                    continue; // партиции без drain-реплики не двигаются

                // База: живые реплики без drain, порядок факта сохранён
                // (лидер — первая реплика — не трогаем).
                var chosen = new HashSet<int>();
                var replicas = new List<int>();
                foreach (var broker in old.Where(b => b != drainBrokerId && targetBrokerIds.Contains(b)))
                {
                    if (chosen.Add(broker))
                    {
                        replicas.Add(broker);
                        load[broker] = load.GetValueOrDefault(broker) + 1;
                    }
                }

                // Добор least-loaded до min(len(old), цели) — RF снижается
                // автоматически, когда живых целей меньше прежнего RF.
                var targetCount = Math.Min(old.Count, targetBrokerIds.Count);
                while (replicas.Count < targetCount)
                {
                    var pick = PickLeastLoaded(chosen, load, targetBrokerIds);
                    chosen.Add(pick);
                    replicas.Add(pick);
                    load[pick] = load.GetValueOrDefault(pick) + 1;
                }

                // Guard min.insync.replicas: ниже — только с отказом
                // (перманентное ожидание, оператор снижает minISR или
                // добавляет брокеров; spec §5.2 D3).
                if (minIsrByTopic.TryGetValue(topic.Topic, out var minIsr) && replicas.Count < minIsr)
                    return Result<IReadOnlyList<ReassignMove>>.Failed(new ApplicationException(
                        $"min.insync.replicas недостижим для {topic.Topic} p{partition}: "
                        + $"план даёт {replicas.Count} реплик, minISR={minIsr} — снизьте minISR или добавьте брокеров"));

                moves.Add(new ReassignMove(topic.Topic, partition, replicas));
            }
        }

        return Result<IReadOnlyList<ReassignMove>>.Success(moves);
    }

    /// <summary>
    /// Balance: converge к декларации (spec §3.4): RF юзер-топиков
    /// min(configRf, targets.Count), internal — min(3, targets.Count)
    /// (формулы от числа живых); лидер (первая реплика) сохраняется; добор
    /// least-loaded; детерминизм сортировкой (topic, partition, brokerId).
    /// </summary>
    public static IReadOnlyList<ReassignMove> PlanBalance(
        IReadOnlyList<KafkaTopicView> topics,
        IReadOnlyList<int> targetBrokerIds,
        int configRf)
    {
        var load = new Dictionary<int, int>();
        var moves = new List<ReassignMove>();

        foreach (var topic in topics.OrderBy(t => t.Topic, StringComparer.Ordinal))
        {
            var rfTarget = IsInternal(topic.Topic)
                ? Math.Min(3, targetBrokerIds.Count)
                : Math.Min(configRf, targetBrokerIds.Count);

            for (var partition = 0; partition < topic.ReplicasPerPartition.Count; partition++)
            {
                var old = topic.ReplicasPerPartition[partition];

                // База: живые реплики (порядок факта — лидер первым);
                // лишние (сверх RF-цели, конфиг понизили) — с хвоста.
                var chosen = new HashSet<int>();
                var replicas = new List<int>();
                foreach (var broker in old.Where(b => targetBrokerIds.Contains(b)))
                {
                    if (replicas.Count >= rfTarget)
                        break;
                    if (chosen.Add(broker))
                        replicas.Add(broker);
                }

                foreach (var broker in replicas)
                    load[broker] = load.GetValueOrDefault(broker) + 1;

                while (replicas.Count < rfTarget)
                {
                    var pick = PickLeastLoaded(chosen, load, targetBrokerIds);
                    chosen.Add(pick);
                    replicas.Add(pick);
                    load[pick] = load.GetValueOrDefault(pick) + 1;
                }

                moves.Add(new ReassignMove(topic.Topic, partition, replicas));
            }
        }

        return moves;
    }

    /// <summary>
    /// Партиции, чей факт != план (по множеству реплик, порядок не важен) —
    /// кандидаты батча; сортировка (Topic, Partition).
    /// </summary>
    public static IReadOnlyList<ReassignMove> Pending(
        IReadOnlyList<KafkaTopicView> topics, IReadOnlyList<ReassignMove> plan)
    {
        var facts = topics
            .SelectMany(t => t.ReplicasPerPartition.Select((_, p) => (t.Topic, Partition: p)))
            .ToHashSet();
        var planByPartition = plan
            .GroupBy(m => (m.Topic, m.Partition))
            .ToDictionary(g => g.Key, g => g.Single());

        return plan
            .Where(m =>
                facts.Contains((m.Topic, m.Partition))
                && FactDiffers(topics, m))
            .OrderBy(m => m.Topic, StringComparer.Ordinal)
            .ThenBy(m => m.Partition)
            .ToList();
    }

    /// <summary>
    /// Drain завершён: drainBrokerId отсутствует в Replicas всех партиций
    /// (ISR не учитывается — см. процесс: USR-критерий отдельной проверкой).
    /// </summary>
    public static bool DrainComplete(IReadOnlyList<KafkaTopicView> topics, int drainBrokerId)
        => topics.All(t => t.ReplicasPerPartition.All(p => !p.Contains(drainBrokerId)));

    /// <summary>
    /// Under-replicated есть: любая партиция Isr.Count &lt; Replicas.Count.
    /// IsrPerPartition == null (ISR не задан — фейк без ISR) = данных о USR
    /// нет → false (не блокирует завершение; реальный адаптер заполняет ISR).
    /// </summary>
    public static bool HasUnderReplicated(IReadOnlyList<KafkaTopicView> topics)
        => topics.Any(t => t.IsrPerPartition is { } isr
            && Enumerable.Range(0, Math.Min(isr.Count, t.ReplicasPerPartition.Count))
                .Any(p => isr[p].Count < t.ReplicasPerPartition[p].Count));

    // Факт партиции отличается от плана (по множеству реплик).
    private static bool FactDiffers(IReadOnlyList<KafkaTopicView> topics, ReassignMove move)
    {
        var topic = topics.FirstOrDefault(t => t.Topic == move.Topic);
        if (topic is null || move.Partition >= topic.ReplicasPerPartition.Count)
            return true; // партиция исчезла из факта — план к ней не применим

        var fact = topic.ReplicasPerPartition[move.Partition];
        return fact.Count != move.Replicas.Count
            || !fact.OrderBy(b => b).SequenceEqual(move.Replicas.OrderBy(b => b));
    }

    private static bool IsInternal(string topic)
        => topic.StartsWith("__", StringComparison.Ordinal);

    // greedy: минимальный load, tie-break — меньший brokerId; load считает
    // сам план (база + добор), инкремент в месте добавления реплики.
    private static int PickLeastLoaded(HashSet<int> chosen, Dictionary<int, int> load, IReadOnlyList<int> targets)
        => targets
            .Where(b => !chosen.Contains(b))
            .OrderBy(b => load.GetValueOrDefault(b))
            .ThenBy(b => b)
            .First();
}
