using AdminPanel.Core.Alerting;
using Shared.Core.DI;

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
                $"routing of bucket bucket_{bucket.Id} of cluster {cluster.Name} points to a non-existent shard {owner}",
                new Dictionary<string, string> { ["owner"] = owner },
                null,
                "routing points to a shard missing from the declaration; routing is the single authority for bucket placement; dangling references break moves and SQL reconciliation; every routing must point to a shard with the shards/<X>/replicas key",
                AlertRemedy.OperatorApi,
                "POST /api/clusters/{c}/moves — move buckets to a live shard or restore the shard declaration");
        }
    }
}
