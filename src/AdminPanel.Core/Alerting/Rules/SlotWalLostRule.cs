using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// slot-wal-lost (critical): wal_status='lost' — WAL срезан, слот догонит только
// пересозданием (P4, arch/03 §4); источник — SQL-проба. Страховка: lost-слот
// воркер пересоздаёт сам (auto-recreate) — алерт гаснет после лечения.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class SlotWalLostRule : IAlertRule
{
    public const string KindName = "slot-wal-lost";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var (cluster, shard, slot) in SlotLagHighRule.Slots(snapshot))
        {
            if (slot.WalStatus != "lost")
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Name}/{shard.Name}/{slot.SlotName}",
                AlertSeverity.Critical,
                KindName,
                $"{cluster.Name}/{shard.Name}/{slot.SlotName}",
                $"slot {slot.SlotName} of shard {cluster.Name}/{shard.Name}: wal_status=lost — WAL removed, the source can catch up only by slot recreation (P4)",
                new Dictionary<string, string> { ["walStatus"] = "lost" },
                null,
                "wal_status=lost: WAL removed — the worker recreates the slot itself (recreate + agent resume from the S3 tail); the alert clears after remediation",
                AlertRemedy.WorkerAuto,
                "the worker recreates the lost slot automatically; the alert persists — the worker is not remediating: backup-wal journal / healthz");
        }
    }
}
