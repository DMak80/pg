using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// shard-no-leader (critical): matched HA-scope без leader-ключа (arch/03 §4; spec §3.10 —
// unmatched-скопы чужого service не алертятся, arch/02 §7).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ShardNoLeaderRule : IAlertRule
{
    public const string KindName = "shard-no-leader";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        // Не поднятые кластеры: лидера нет потому, что нод нет (spec t12 §3.7)
        var notInitialized = snapshot.Clusters
            .Where(c => c.State == ClusterState.NotInitialized)
            .Select(c => c.Name)
            .ToHashSet();

        foreach (var scope in snapshot.HaScopes)
        {
            if (scope.Cluster is not null && notInitialized.Contains(scope.Cluster))
                continue;

            if (!scope.Matched || scope.LeaderName is not null)
                continue;

            yield return new Alert(
                $"{KindName}:{scope.Scope}",
                AlertSeverity.Critical,
                KindName,
                scope.Scope,
                $"HA-scope {scope.Scope} без leader-ключа (шард {scope.Cluster}/{scope.Shard} без лидера)",
                new Dictionary<string, string>
                {
                    ["scope"] = scope.Scope,
                    ["cluster"] = scope.Cluster!,
                    ["shard"] = scope.Shard!,
                },
                null);
        }
    }
}
