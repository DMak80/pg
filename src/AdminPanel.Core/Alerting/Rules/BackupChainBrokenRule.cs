using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backup-chain-broken (critical, t07, arch/19 §3/§4): state=WAL_CHAIN_ERROR wal-ключа
// шарда живого Active-кластера — разрыв цепочки; воркер сам лечит пересъёмом
// полного (планировщик arch/19 §2 реагирует на WAL_CHAIN_ERROR). Текст показывает и
// сбой, и действие воркера. DEGRADED — transient, остаётся правилу
// wal-chain-broken (t03).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupChainBrokenRule : IAlertRule
{
    public const string KindName = "backup-chain-broken";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var backups in snapshot.Backups)
        foreach (var (shard, wal) in backups.Shards ?? new Dictionary<string, WalStreamInfo?>())
        {
            if (wal is not { State: WalStreamInfoState.WalChainError })
                continue;

            // только живой Active-кластер: мёртвый кластер — не алерт, а сирота
            // (backup-orphan); демонтаж сам останавливает поток.
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
                $"WAL chain broken for shard {shard} of cluster {backups.Cluster}: {wal.Error ?? "no reason"} — worker re-runs the full backup",
                new Dictionary<string, string>
                {
                    ["state"] = "WAL_CHAIN_ERROR",
                    ["slot"] = wal.Slot,
                    ["error"] = wal.Error ?? "",
                },
                null,
                "the full+WAL chain cannot be restored incrementally: a new point requires a new full backup",
                AlertRemedy.WorkerAuto,
                "worker re-runs the full backup and brings the agent up (t07); prolonged remediation is visible via backup-full-stale/FAILED series — investigate per the arch/19 runbook");
        }
    }
}
