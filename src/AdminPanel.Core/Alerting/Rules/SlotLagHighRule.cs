using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;
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
                $"лаг слота {slot.SlotName} шарда {cluster.Name}/{shard.Name} — {lag} байт, порог {ThresholdBytes} байт",
                new Dictionary<string, string>
                {
                    ["lagBytes"] = lag.Value.ToString(),
                    ["thresholdBytes"] = ThresholdBytes.ToString(),
                },
                null,
                "лаг слота выше порога: слот копит WAL на мастере (риск среза и потери слота); каждый слот шарда обязан догонять",
                AlertRemedy.WorkerAuto,
                "надзор воркера следит за слотами (rebuild зависшей реплики); висит — проверьте нагрузку или запустите recreate ноды");
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
