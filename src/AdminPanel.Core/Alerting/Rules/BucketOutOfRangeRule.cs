using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

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
                $"routing-ключ bucket_{bucket.Id} кластера {cluster.Name} вне диапазона 0..{cluster.BucketsCount - 1}",
                new Dictionary<string, string>
                {
                    ["bucketId"] = bucket.Id.ToString(),
                    ["bucketsCount"] = cluster.BucketsCount.ToString(),
                },
                null,
                "routing-ключ с id за пределами 0..N-1 (config.buckets): парсер резервирует диапазон декларации, висячий id мусорит карту; каждый routing обязан попадать в диапазон config.buckets",
                AlertRemedy.OperatorRunbook,
                "удалите висячий routing-ключ (etcdctl) — воркер диапазон не расширяет; расширение бакетов возможно только пересозданием кластера (config пишет PgWorker через POST /api/clusters)");
        }
    }
}
