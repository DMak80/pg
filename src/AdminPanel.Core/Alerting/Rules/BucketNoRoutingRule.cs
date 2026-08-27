using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

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
                $"бакет {bucket.Id} кластера {cluster.Name} из диапазона 0..{cluster.BucketsCount - 1} без routing-ключа (дыра карты)",
                new Dictionary<string, string>
                {
                    ["bucketId"] = bucket.Id.ToString(),
                    ["bucketsCount"] = cluster.BucketsCount.ToString(),
                },
                null);
        }
    }
}
