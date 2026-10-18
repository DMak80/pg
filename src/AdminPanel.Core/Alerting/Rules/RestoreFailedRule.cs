using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// restore-failed (critical, t05, arch/19 §3.5/§8): FAILED restore-заявки живого
// шарда Active-кластера — восстановление не удалось, разбор по runbook
// (docs/backup-restore.md) и повтор заявки. Активные фазы
// (PLANNED/RUNNING/REJOINING) без алерта — прогресс виден в статусе ключа.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class RestoreFailedRule : IAlertRule
{
    public const string KindName = "restore-failed";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var backups in snapshot.Backups)
        foreach (var (shard, restores) in backups.ShardsRestores
                     ?? new Dictionary<string, IReadOnlyList<RestoreOperationInfo>>())
        foreach (var restore in restores.Where(r => r.State == "FAILED"))
        {
            // только живой Active-кластер: демонтаж удаляет ключи бэкапов сам (AC6)
            var cluster = snapshot.Clusters.FirstOrDefault(
                c => c.Name == backups.Cluster && c.State == ClusterState.Active
                     && c.Shards.Any(s => s.Name == shard));
            if (cluster is null)
                continue;

            yield return new Alert(
                $"{KindName}:{backups.Cluster}/{shard}/{restore.Id}",
                AlertSeverity.Critical,
                KindName,
                $"{backups.Cluster}/{shard}",
                $"restore of shard {shard} in cluster {backups.Cluster} failed: {restore.Error ?? "no reason"}",
                new Dictionary<string, string>
                {
                    ["restoreId"] = restore.Id,
                    ["error"] = restore.Error ?? string.Empty,
                },
                null,
                "troubleshoot per docs/backup-restore.md (FAILED diagnostics) and re-submit the restore job",
                AlertRemedy.OperatorRunbook,
                "active restore (PLANNED/RUNNING/REJOINING) raises no alert — phases are visible in the status");
        }
    }
}
