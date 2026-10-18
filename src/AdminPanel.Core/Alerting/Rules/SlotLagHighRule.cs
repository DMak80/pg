using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// slot-lag-high (warning): лаг слота > ReplicaLagBytes — один порог лага на
// replica/slot (каталог 03 §4, spec §3.8); источник — SQL-проба (P4).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class SlotLagHighRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "slot-lag-high";

    public const long DefaultBytes = ReplicaLagHighRule.DefaultBytes;

    public string Kind => KindName;

    private long ThresholdBytes
        => options.Value.ReplicaLagBytes > 0 ? options.Value.ReplicaLagBytes : DefaultBytes;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var (cluster, shard, slot) in Slots(snapshot))
        {
            var lag = slot.LagBytes;
            if (lag is null || lag <= ThresholdBytes)
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Name}/{shard.Name}/{slot.SlotName}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/{shard.Name}/{slot.SlotName}",
                $"slot {slot.SlotName} lag on shard {cluster.Name}/{shard.Name} — {lag} bytes, threshold {ThresholdBytes} bytes",
                new Dictionary<string, string>
                {
                    ["lagBytes"] = lag.Value.ToString(),
                    ["thresholdBytes"] = ThresholdBytes.ToString(),
                },
                null,
                "slot lag above threshold: the slot is accumulating WAL on the master (risk of WAL removal and slot loss); every shard slot must catch up",
                AlertRemedy.WorkerAuto,
                "worker supervision monitors slots (rebuild of a stuck replica); if stuck — check the load or run a node recreate");
        }
    }

    // Общий обход слотов безошибочных runtime — общий хелпер правил slot-* (spec §5.1).
    internal static IEnumerable<(ClusterInfo Cluster, ShardInfo Shard, ReplicationSlotInfo Slot)> Slots(
        EtcdSnapshot snapshot)
    {
        foreach (var cluster in snapshot.Clusters)
        foreach (var shard in cluster.Shards)
        {
            if (shard.Runtime?.Error is not null)
                continue;
            foreach (var slot in shard.Runtime?.Slots ?? [])
                yield return (cluster, shard, slot);
        }
    }
}
