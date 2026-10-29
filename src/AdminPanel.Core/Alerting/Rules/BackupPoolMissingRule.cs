using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backup-pool-missing (critical, каталог 03 §4): Active-кластер с
// включённой подсистемой бэкапов, policy без секции s3 — пул не выбран,
// бэкапы кластера не пишутся (arch/19 §4, «только явный выбор»).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupPoolMissingRule : IAlertRule
{
    public const string KindName = "backup-pool-missing";
    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Backups)
        {
            // Политика отсутствует вовсе = кластер на глобальном дефолте с
            // выключенным пулом — «policy без s3», алерт честный (архив §4).
            if (cluster.Policy?.S3 is not null)
                continue;

            // Только живой Active-кластер (03 §4): в переходном окне демонтажа
            // (NotInitialized/ToRemove) алерт «пул не выбран» — ложный critical,
            // демонтаж ключи бэкапов чистит сам.
            if (!snapshot.Clusters.Any(
                    c => c.Name == cluster.Cluster && c.State == ClusterState.Active))
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Cluster}",
                AlertSeverity.Critical,
                KindName,
                cluster.Cluster,
                $"cluster {cluster.Cluster}: S3 pool not selected (policy without s3 section) — cluster backups are neither written nor read",
                new Dictionary<string, string>(),
                null,
                "select a pool on the Backup Storage page — without selecting a pool, cluster backups are not written (arch/19 §4)",
                AlertRemedy.OperatorApi,
                "open Backup Storage → Backup Pool, select the pool and the master/replica lists");
        }
    }
}
