using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// sync-standby-missing (warning): у мастера нет standby с sync_state IN ('sync','quorum')
// — предусловие переездов не выполнено (P8, arch/03 §4; по букве каталога, без
// carve-outs — spec §3.12). Проверяется только на мастере без ошибки пробы.
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

            yield return new Alert(
                $"{KindName}:{cluster.Name}/{shard.Name}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/{shard.Name}",
                $"у мастера шарда {cluster.Name}/{shard.Name} нет sync-standby (sync_state sync/quorum) — предусловие переездов не выполнено (P8)",
                new Dictionary<string, string> { ["standbiesTotal"] = runtime.Standbies.Count.ToString() },
                null);
        }
    }
}
