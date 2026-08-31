using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// cluster-incomplete (warning): префикс /clusters/<C> без config (arch/03 §4; Incomplete — t03 §3.6).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ClusterIncompleteRule : IAlertRule
{
    public const string KindName = "cluster-incomplete";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Clusters.Where(c => c.Incomplete))
            yield return new Alert(
                $"{KindName}:{cluster.Name}",
                AlertSeverity.Warning,
                KindName,
                cluster.Name,
                $"кластер {cluster.Name} без config-ключа (incomplete)",
                new Dictionary<string, string> { ["dbname"] = cluster.DbName ?? "missing" },
                null,
                "префикс /clusters/<C> есть, но config-ключа нет: config — входная точка декларации (buckets/dbname), без неё кластер невидим для процессов панели и воркера; префикс обязан начинаться с config",
                AlertRemedy.WorkerAuto,
                "воркер ждёт доустойчивости ключей (journal /pgworker/work); при вечном висе — дефект воркера");
    }
}
