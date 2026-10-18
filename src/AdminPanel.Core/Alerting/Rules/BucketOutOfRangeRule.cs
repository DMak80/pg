using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// bucket-out-of-range (warning): routing-ключ с id >= N (P18, arch/03 §4);
// incomplete (N=0) — мимо: без config нет и диапазона (spec §3.13).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BucketOutOfRangeRule : IAlertRule
{
    public const string KindName = "bucket-out-of-range";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Clusters.Where(c => c.BucketsCount > 0))
        foreach (var bucket in cluster.Buckets)
        {
            if (bucket.Owner is null || bucket.Id < cluster.BucketsCount)
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/bucket_{bucket.Id}",
                $"routing key bucket_{bucket.Id} of cluster {cluster.Name} out of range 0..{cluster.BucketsCount - 1}",
                new Dictionary<string, string>
                {
                    ["bucketId"] = bucket.Id.ToString(),
                    ["bucketsCount"] = cluster.BucketsCount.ToString(),
                },
                null,
                "routing key with id out of range 0..N-1 (config.buckets): the parser reserves the declared range, a dangling id pollutes the map; every routing key must fall within the config.buckets range",
                AlertRemedy.OperatorRunbook,
                "delete the dangling routing key (etcdctl) — the worker does not extend the range; buckets can only be expanded by recreating the cluster (config is written by PgWorker via POST /api/clusters)");
        }
    }
}
