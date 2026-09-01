using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// slot-wal-lost (critical): wal_status='lost' — WAL срезан, слот догонит только
// пересозданием (P4, arch/03 §4); источник — SQL-проба.
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
                $"слот {slot.SlotName} шарда {cluster.Name}/{shard.Name}: wal_status=lost — WAL срезан, источник догонит только пересозданием (P4)",
                new Dictionary<string, string> { ["walStatus"] = "lost" },
                null,
                "wal_status=lost: WAL срезан — слот физически не догонит, реплика потеряла данные; слот обязан догонять до горизонта retention",
                AlertRemedy.OperatorRunbook,
                "запустите recreate ноды (API панели) — воркер пересоздаст реплику с basebackup; потерянный слот почистите по runbook");
        }
    }
}
