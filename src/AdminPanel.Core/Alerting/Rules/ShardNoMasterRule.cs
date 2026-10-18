using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// shard-no-master (critical): dsn есть, master-ключа нет — lease протух или писателя нет (P11, arch/03 §4).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ShardNoMasterRule : IAlertRule
{
    public const string KindName = "shard-no-master";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Clusters)
        foreach (var shard in cluster.Shards)
        {
            if (shard.MasterAddress is not null || string.IsNullOrEmpty(shard.Dsn))
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Name}/{shard.Name}",
                AlertSeverity.Critical,
                KindName,
                $"{cluster.Name}/{shard.Name}",
                $"shard {cluster.Name}/{shard.Name} has no master key (lease expired or no writer)",
                new Dictionary<string, string>
                {
                    ["cluster"] = cluster.Name,
                    ["shard"] = shard.Name,
                    ["dsn"] = shard.Dsn,
                },
                null,
                "shard has a dsn but no master key: master (leases) is the current node executing SQL writes; every shard must have a live master key, in sync with /service/<scope>/leader",
                AlertRemedy.WorkerAuto,
                "PgWorker master reconciliation (feat-pgworker-adopt-repair) will restore the key; if it stalls, it is a worker defect");
        }
    }
}
