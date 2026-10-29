using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backup-wal-tli-gap (critical, arch/19 §3/§4): живое поле gap wal-ключа шарда
// живого Active-кластера — RPO-зазор tli-прыжка приёмника (58P01 пережит,
// сегменты между from и to невосстановимы из бэкапов); воркер сам переснимает
// полный (планировщик arch/19 §2 реагирует на gap), метка снимается
// заживлением. DEGRADED без gap — домен wal-chain-broken/лагов (не трогается).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupWalTliGapRule : IAlertRule
{
    public const string KindName = "backup-wal-tli-gap";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var backups in snapshot.Backups)
        foreach (var (shard, wal) in backups.Shards ?? new Dictionary<string, WalStreamInfo?>())
        {
            if (wal is not { Gap: { } gap })
                continue;

            // только живой Active-кластер (образец BackupChainBrokenRule)
            var cluster = snapshot.Clusters.FirstOrDefault(
                c => c.Name == backups.Cluster && c.State == ClusterState.Active
                     && c.Shards.Any(s => s.Name == shard));
            if (cluster is null)
                continue;

            yield return new Alert(
                $"{KindName}:{backups.Cluster}/{shard}",
                AlertSeverity.Critical,
                KindName,
                $"{backups.Cluster}/{shard}",
                $"WAL tli-gap for shard {shard} of cluster {backups.Cluster}: segments " +
                $"({gap.From} .. {gap.To}] are unrestorable from backups (RPO event, tli ->{gap.Tli}) " +
                $"— the worker re-runs the full backup",
                new Dictionary<string, string>
                {
                    ["state"] = wal.State.ToString().ToUpperInvariant(),
                    ["slot"] = wal.Slot,
                    ["error"] = wal.Error ?? "",
                    ["gap_from"] = gap.From,
                    ["gap_to"] = gap.To,
                    ["gap_tli"] = $"{gap.Tli}",
                },
                null,
                "data between the gap boundaries cannot be restored from backups (RPO event): " +
                "check the data-loss window against your recovery requirements",
                AlertRemedy.WorkerAuto,
                "the worker re-runs the full backup; the alert clears when the covering full " +
                "backup completes and the gap field is lifted by the worker");
        }
    }
}
