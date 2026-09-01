using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

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
                $"шард {cluster.Name}/{shard.Name} без master-ключа (lease протух или писателя нет)",
                new Dictionary<string, string>
                {
                    ["cluster"] = cluster.Name,
                    ["shard"] = shard.Name,
                    ["dsn"] = shard.Dsn,
                },
                null,
                "у шарда есть dsn, но нет master-ключа: master (leases) — текущая нода записи SQL-операций; каждый шард обязан иметь живой master-ключ, синхронный с /service/<scope>/leader",
                AlertRemedy.WorkerAuto,
                "сверка мастера PgWorker (feat-pgworker-adopt-repair) восстановит ключ; висит — дефект воркера");
        }
    }
}
