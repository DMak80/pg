using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

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
                $"восстановление шарда {shard} кластера {backups.Cluster} не удалось: {restore.Error ?? "без причины"}",
                new Dictionary<string, string>
                {
                    ["restoreId"] = restore.Id,
                    ["error"] = restore.Error ?? string.Empty,
                },
                null,
                "разбор по docs/backup-restore.md (диагностика FAILED) и повтор заявки restore",
                AlertRemedy.OperatorRunbook,
                "активный restore (PLANNED/RUNNING/REJOINING) без алерта — фазы видны в статусе");
        }
    }
}
