using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backup-pool-missing (critical, t36, каталог 03 §4): Active-кластер с
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
                $"кластер {cluster.Cluster}: пул S3 не выбран (policy без секции s3) — бэкапы кластера не пишутся и не читаются",
                new Dictionary<string, string>(),
                null,
                "выбери пул на странице «Хранилище бэкапов» — без выбора бэкапы кластера не пишутся (t36, arch/19 §4)",
                AlertRemedy.OperatorApi,
                "открой «Хранилище бэкапов» → «Пул бэкапов», выбери пул и списки мастера/реплики");
        }
    }
}
