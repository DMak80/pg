using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// provision-stuck (warning): /pgworker/work/<C> несёт живой last_error и серию
// фейлов provision старше ProvisionStuckSec — воркер сообщил причину, но кластер
// не инициализируется (arch/adminpanel/03 §4; серия живёт с первого фейла до
// успеха — возраст по fail_first_unix, не по updated_unix: InProgress-фазы
// обновляют журнал каждый тик, алерт не мигает).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ProvisionStuckRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "provision-stuck";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var threshold = options.Value.ProvisionStuckSec;
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        foreach (var w in snapshot.PgWorkerWork.Where(w =>
                     w.Op == "provision" && w.LastError is not null
                     && w.FailFirstUnix is { } first && nowUnix - first > threshold))
        {
            yield return new Alert(
                $"{KindName}:{w.Cluster}",
                AlertSeverity.Warning,
                KindName,
                w.Cluster,
                $"provision of cluster {w.Cluster} is failing: {w.LastError}",
                new Dictionary<string, string>
                {
                    ["op"] = w.Op,
                    ["phase"] = w.Phase,
                    ["fail_count"] = w.FailCount?.ToString() ?? "?",
                    ["updated_unix"] = w.UpdatedUnix.ToString(),
                    ["retry_not_before_unix"] = w.RetryNotBeforeUnix?.ToString() ?? "",
                },
                null,
                "worker reports the provision failure reason in /pgworker/work/<C>: the series lives from the first failure (fail_first_unix) until success; an endless series = worker or environment defect (ports/image/etcd)",
                AlertRemedy.WorkerAuto,
                "see the /pgworker/work/<C> journal and worker logs; the worker retries on its own with backoff — if the cause is external (busy ports, broken image), follow runbook arch/09");
        }
    }
}
