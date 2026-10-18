using AdminPanel.Core.Alerting;
using Shared.Core.DI;

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
                $"bucket bucket_{bucket.Id} of cluster {cluster.Name} is in ABORTING — incomplete cleanup",
                details,
                null,
                "ABORTING status is stuck: an aborted move must finish with rollback/status cleanup by the carrier; a stuck status keeps the bucket in move limbo",
                AlertRemedy.WorkerAuto,
                "PgWorker move reparator (feat-pgworker-adopt-repair) will close it; stuck — worker defect");
        }
    }
}
