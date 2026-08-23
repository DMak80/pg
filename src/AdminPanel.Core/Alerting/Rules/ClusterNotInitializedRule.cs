using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// cluster-not-initialized (info): кластер заявлен, но ноды не подняты (arch/03 §4;
// arch/02 §9) — заметка вместо critical-шумa не поднятого кластера (spec t12 §8.11).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ClusterNotInitializedRule : IAlertRule
{
    public const string KindName = "cluster-not-initialized";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Clusters.Where(c => c.State == ClusterState.NotInitialized))
            yield return new Alert(
                $"{KindName}:{cluster.Name}",
                AlertSeverity.Info,
                KindName,
                cluster.Name,
                $"кластер {cluster.Name} заявлен (NOT_INITIALIZED): ноды не подняты, схемы не созданы",
                new Dictionary<string, string> { ["dbname"] = cluster.DbName ?? "missing" },
                null);
    }
}
