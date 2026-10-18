using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// sync-standby-missing (strict ? critical : warning, t06): у мастера нет standby
// с sync_state IN ('sync','quorum') — предусловие переездов не выполнено (P8,
// arch/03 §4); у strict-кластера запись блокирована — инцидент (critical).
// Проверяется только на мастере без ошибки пробы.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class SyncStandbyMissingRule : IAlertRule
{
    public const string KindName = "sync-standby-missing";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Clusters)
        foreach (var shard in cluster.Shards)
        {
            var runtime = shard.Runtime;
            if (runtime?.Error is not null || runtime?.IsInRecovery != false)
                continue;

            if (runtime.Standbies.Any(s => s.SyncState is "sync" or "quorum"))
                continue;

            // t06: strict-кластер без sync-standby БЛОКИРУЕТ запись — critical
            // (инцидент); не-strict — прежний warning (доступность переездов).
            var strict = cluster.SynchronousModeStrict;
            yield return new Alert(
                $"{KindName}:{cluster.Name}/{shard.Name}",
                strict ? AlertSeverity.Critical : AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/{shard.Name}",
                $"master of shard {cluster.Name}/{shard.Name} has no sync-standby (sync_state sync/quorum) — moves precondition not met (P8)",
                new Dictionary<string, string> { ["standbiesTotal"] = runtime.Standbies.Count.ToString() },
                null,
                "master has no synchronous standby (sync/quorum): synchronous replication is a precondition for seamless moves (cutover requires sync confirmation); the master must keep a sync-standby",
                AlertRemedy.WorkerAuto,
                strict
                    ? "writes are blocked (strict) — restore the replica (worker rebuild) or disable strict (PUT config)"
                    : "worker supervision will restore the replica (rebuild); if it lingers — check /service/<scope>/members and recreate the lagging node");
        }
    }
}
