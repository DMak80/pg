using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// move-aborting (warning): ABORTING — незавершённая уборка, безусловно (P7, arch/03 §4).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class MoveAbortingRule : IAlertRule
{
    public const string KindName = "move-aborting";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        foreach (var cluster in snapshot.Clusters)
        foreach (var bucket in cluster.Buckets.Where(b => b.State == BucketState.Aborting))
        {
            var details = new Dictionary<string, string>();
            if (bucket.Move?.Phase is { } phase)
                details["phase"] = phase;
            if (bucket.Move?.LastError is { } lastError)
                details["lastError"] = lastError;
            var stamp = MoveAge.Stamp(bucket);
            if (stamp is not null)
            {
                details["ageSeconds"] = (nowUnix - stamp.Value).ToString();
                details["updatedUnix"] = stamp.Value.ToString();
            }

            yield return new Alert(
                $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/bucket_{bucket.Id}",
                $"бакет bucket_{bucket.Id} кластера {cluster.Name} в ABORTING — незавершённая уборка",
                details,
                null);
        }
    }
}
