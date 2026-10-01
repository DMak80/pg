using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backup-drill-failed (critical, reliability t02, arch/19 §3.6): последний
// дрилл FAILED живого шарда Active-кластера — восстановимость из бэкапа не
// доказана; разбор по runbook (docs/backup-restore.md §Дрилл) и повторный
// дрилл после лечения. RUNNING/фазы/SUCCEEDED без алерта — прогресс виден
// в статусе ключа.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupDrillFailedRule : IAlertRule
{
    public const string KindName = "backup-drill-failed";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var backups in snapshot.Backups)
        foreach (var (shard, drill) in backups.ShardsDrills
                     ?? new Dictionary<string, DrillInfo>())
        {
            if (drill.State != "FAILED")
                continue;

            // только живой Active-кластер: демонтаж удаляет ключи бэкапов сам.
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
                $"дрилл восстановления шарда {shard} кластера {backups.Cluster} провалился: {drill.Error ?? "без причины"}",
                new Dictionary<string, string>
                {
                    ["drillId"] = drill.Id,
                    ["backupId"] = drill.BackupId,
                    ["startedUnix"] = drill.StartedUnix.ToString(),
                },
                null,
                "дрилл восстановления провалился — восстановимость из бэкапа не доказана",
                AlertRemedy.OperatorRunbook,
                "разбор по docs/backup-restore.md §Дрилл; восстановимость доказывается повторным дриллом после лечения");
        }
    }
}
