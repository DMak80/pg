using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// cluster-incomplete (warning): префикс /clusters/<C> без config (arch/03 §4; Incomplete — §3.6).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ClusterIncompleteRule : IAlertRule
{
    public const string KindName = "cluster-incomplete";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Clusters.Where(c => c.Incomplete))
            yield return new Alert(
                $"{KindName}:{cluster.Name}",
                AlertSeverity.Warning,
                KindName,
                cluster.Name,
                $"cluster {cluster.Name} without config key (incomplete)",
                new Dictionary<string, string> { ["dbname"] = cluster.DbName ?? "missing" },
                null,
                "prefix /clusters/<C> exists, but the config key is missing: config is the entry point of the declaration (buckets/dbname), without it the cluster is invisible to the panel and worker processes; the prefix must start with config",
                AlertRemedy.WorkerAuto,
                "worker waits for the keys to become consistent (journal /pgworker/work); a permanent hang indicates a worker defect");
    }
}
