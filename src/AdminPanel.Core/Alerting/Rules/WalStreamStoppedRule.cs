using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// wal-stream-stopped (warning, t03, arch/19 §3/§8): STOPPED при живом шарде
// Active-кластера — операционная остановка (Enabled=false/QUARANTINED) пишется
// этим же статусом; алерт информирует, что поток бэкапов не идёт.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class WalStreamStoppedRule : IAlertRule
{
    public const string KindName = "wal-stream-stopped";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var backups in snapshot.Backups)
        foreach (var (shard, wal) in backups.Shards ?? new Dictionary<string, WalStreamInfo?>())
        {
            if (wal is not { State: WalStreamInfoState.Stopped })
                continue;

            // только живой Active-кластер: демонтаж удаляет ключи бэкапов сам (AC6)
            var cluster = snapshot.Clusters.FirstOrDefault(
                c => c.Name == backups.Cluster && c.State == ClusterState.Active
                     && c.Shards.Any(s => s.Name == shard));
            if (cluster is null)
                continue;

            yield return new Alert(
                $"{KindName}:{backups.Cluster}/{shard}",
                AlertSeverity.Warning,
                KindName,
                $"{backups.Cluster}/{shard}",
                $"WAL-поток шарда {shard} кластера {backups.Cluster} остановлен (STOPPED)",
                new Dictionary<string, string>
                {
                    ["state"] = "STOPPED",
                    ["slot"] = wal.Slot,
                },
                null,
                "если остановка не операционная (Enabled=false/QUARANTINED) — перезапусти подсистему/разбери журналы /pgworker/work/<C>",
                AlertRemedy.WorkerAuto,
                "последнее касание ключа — стоп-семантика воркера; демонтаж кластера/шарда удаляет ключ (arch/19 §4)");
        }
    }
}
