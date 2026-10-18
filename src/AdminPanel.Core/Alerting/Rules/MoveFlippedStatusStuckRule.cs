using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// move-flipped-status-stuck (warning): status есть, routing уже = target (P7, arch/03 §4).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class MoveFlippedStatusStuckRule : IAlertRule
{
    public const string KindName = "move-flipped-status-stuck";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Clusters)
        foreach (var bucket in cluster.Buckets)
        {
            if (bucket.State == BucketState.Active
                || bucket.Move?.Target is not { } target
                || bucket.Owner != target)
                continue;

            // Строка канона статус-ключей — в message и details одинаково (spec §4.3).
            var state = bucket.State.ToString().ToUpperInvariant();
            yield return new Alert(
                $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/bucket_{bucket.Id}",
                $"routing of bucket bucket_{bucket.Id} of cluster {cluster.Name} already = target {target}, but status {state} is not removed",
                new Dictionary<string, string>
                {
                    ["owner"] = bucket.Owner!,
                    ["target"] = target,
                    ["state"] = state,
                },
                null,
                "routing already points to the target, but the move status is not removed: flip routing is the last step, after it the status key must disappear in the same tick; a dangling status keeps the shard in teardown locks",
                AlertRemedy.WorkerAuto,
                "PgWorker move repairer (feat-pgworker-adopt-repair) will remove the stuck status; still stuck — worker defect");
        }
    }
}
