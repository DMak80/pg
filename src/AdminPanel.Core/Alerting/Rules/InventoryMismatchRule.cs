using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// inventory-mismatch (warning): фактические схемы bucket_% ≠ routing — «тихие»
// расхождения P21/P23 (arch/03 §4). Сверка только по ACTIVE-бакетам: схемы
// переездных бакетов на приёмнике — норма (spec §3.11).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class InventoryMismatchRule : IAlertRule
{
    public const string KindName = "inventory-mismatch";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Clusters)
        foreach (var shard in cluster.Shards)
        {
            // Runtime нет (пробы выключены/не было тика) — сверки не будет: гвард
            // обязан отсекать и null, и Error (spec §5.1 «Runtime без ошибки»).
            var runtime = shard.Runtime;
            if (runtime is null || runtime.Error is not null)
                continue;

            var expected = cluster.Buckets
               .Where(b => b.Owner == shard.Name && b.State == BucketState.Active)
               .Select(b => $"bucket_{b.Id}")
               .ToHashSet();
            var actual = runtime.BucketSchemas.ToHashSet();
            var missing = expected.Except(actual).Order().ToList(); // Order() без компаратора — Ordinal для строк
            var extra = actual.Except(expected).Order().ToList();
            if (missing.Count == 0 && extra.Count == 0)
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Name}/{shard.Name}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/{shard.Name}",
                $"инвентарь схем шарда {cluster.Name}/{shard.Name} не совпадает с routing: отсутствуют [{string.Join(", ", missing)}], лишние [{string.Join(", ", extra)}]",
                new Dictionary<string, string>
                {
                    ["missing"] = string.Join(", ", missing),
                    ["extra"] = string.Join(", ", extra),
                },
                null);
        }
    }
}
