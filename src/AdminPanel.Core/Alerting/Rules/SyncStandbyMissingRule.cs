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
                $"у мастера шарда {cluster.Name}/{shard.Name} нет sync-standby (sync_state sync/quorum) — предусловие переездов не выполнено (P8)",
                new Dictionary<string, string> { ["standbiesTotal"] = runtime.Standbies.Count.ToString() },
                null,
                "у мастера нет синхронного standby (sync/quorum): синхронная репликация — предусловие бесшовных переездов (cutover требует sync-подтверждения); мастер обязан держать sync-standby",
                AlertRemedy.WorkerAuto,
                strict
                    ? "запись блокирована (strict) — восстановите реплику (rebuild воркера) или выключите strict (PUT config)"
                    : "надзор воркера восстановит реплику (rebuild); висит — проверьте /service/<scope>/members и recreate отстающей ноды");
        }
    }
}
