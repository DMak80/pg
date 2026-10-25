using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// wal-chain-broken (critical, t03, arch/19 §3/§8): DEGRADED WAL-поток шарда живого
// Active-кластера — дыра цепочки/инвалидация слота; текст — error статуса.
// transient-деградации (lag/тишина) тоже DEGRADED у воркера — различает текст
// error статуса (дыра/слот vs отставание/тишина); правило едино для DEGRADED,
// критичность оправдана: восстановление только новым полным (t02/t07) либо
// самооздоровлением потока (transient) — оператор смотрит error.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class WalChainBrokenRule : IAlertRule
{
    public const string KindName = "wal-chain-broken";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var backups in snapshot.Backups)
        foreach (var (shard, wal) in backups.Shards ?? new Dictionary<string, WalStreamInfo?>())
        {
            if (wal is not { State: WalStreamInfoState.Degraded })
                continue;

            // только живой Active-кластер: демонтаж сам останавливает поток
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
                $"WAL chain of shard {shard} in cluster {backups.Cluster} is DEGRADED: {wal.Error ?? "no reason"}",
                new Dictionary<string, string>
                {
                    ["state"] = "DEGRADED",
                    ["slot"] = wal.Slot,
                    ["error"] = wal.Error ?? "",
                },
                null,
                "investigate the status error: chain gap/slot — take a new full backup (PgWorker, runbook arch/19 §3: a full backup with wal_start_segment above the gap is required); lag/silence — check the receivers' status keys /pgworker/backups/<C>/<X>/agent/<N> in etcd and the walreceiver log inside the node containers (transient, the receiver retries)",
                AlertRemedy.WorkerAuto,
                "gap/slot: receivers pause themselves by the STOPPED/BROKEN gate (etcd), recovery is a new full backup (t02)/reconcile (t07); lag/silence: self-healing by the receiver retry loop");
        }
    }
}
