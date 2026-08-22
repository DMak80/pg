using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

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
                $"routing бакета bucket_{bucket.Id} кластера {cluster.Name} уже = target {target}, но статус {state} не снят",
                new Dictionary<string, string>
                {
                    ["owner"] = bucket.Owner!,
                    ["target"] = target,
                    ["state"] = state,
                },
                null);
        }
    }
}
