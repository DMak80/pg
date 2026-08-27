using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// bucket-lost (critical): routing указывает на несуществующий шард (P23-а, arch/03 §4).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BucketLostRule : IAlertRule
{
    public const string KindName = "bucket-lost";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Clusters)
        foreach (var bucket in cluster.Buckets)
        {
            if (bucket.Owner is not { } owner || cluster.Shards.Any(s => s.Name == owner))
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                AlertSeverity.Critical,
                KindName,
                $"{cluster.Name}/bucket_{bucket.Id}",
                $"routing бакета bucket_{bucket.Id} кластера {cluster.Name} указывает на несуществующий шард {owner}",
                new Dictionary<string, string> { ["owner"] = owner },
                null);
        }
    }
}
