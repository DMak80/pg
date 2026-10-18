using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// bucket-no-routing (warning): бакет из 0..N-1 без routing-ключа — дыра карты (arch/03 §4).
// incomplete-кластер (N=0) не проверяется — он уже алертится cluster-incomplete (spec §3.13).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BucketNoRoutingRule : IAlertRule
{
    public const string KindName = "bucket-no-routing";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Clusters)
        foreach (var bucket in cluster.Buckets)
        {
            if (bucket.Owner is not null || bucket.Id < 0 || bucket.Id >= cluster.BucketsCount)
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/bucket_{bucket.Id}",
                $"bucket {bucket.Id} of cluster {cluster.Name} from range 0..{cluster.BucketsCount - 1} without a routing key (map hole)",
                new Dictionary<string, string>
                {
                    ["bucketId"] = bucket.Id.ToString(),
                    ["bucketsCount"] = cluster.BucketsCount.ToString(),
                },
                null,
                "a bucket in the 0..N-1 range has no routing key: routing is the only authority for \"where the bucket is\"; a hole breaks SQL cross-checks and moves; every bucket must have this key",
                AlertRemedy.OperatorRunbook,
                "routing keys are written only by PgWorker (initialization/moves): a hole is a consequence of external interference or a failure; check the worker journal (/pgworker/work) and the moves journal, recreate the cluster if necessary");
        }
    }
}
