using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// cluster-not-initialized (info → warning по возрасту, arch/adminpanel/03 §4):
// кластер заявлен, но ноды не подняты — заметка, пока висит недолго; зависание
// дольше NotInitializedWarnSec (900 c > PatroniBootSec=600) — эскалация.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ClusterNotInitializedRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "cluster-not-initialized";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var threshold = options.Value.NotInitializedWarnSec;
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        foreach (var cluster in snapshot.Clusters.Where(c => c.State == ClusterState.NotInitialized))
        {
            var id = $"{KindName}:{cluster.Name}";
            // Возраст: created_unix (не зависит от рестартов панели), fallback —
            // возраст алерта по previous-снапшоту, иначе «только что увидели».
            var since = cluster.CreatedUnix
                        ?? context.Previous?.Alerts.FirstOrDefault(a => a.Id == id)?.SinceUnix
                        ?? nowUnix;
            var stuckFor = nowUnix - since > threshold;
            yield return new Alert(
                id,
                stuckFor ? AlertSeverity.Warning : AlertSeverity.Info,
                KindName,
                cluster.Name,
                stuckFor
                    ? $"cluster {cluster.Name} has been in NOT_INITIALIZED for more than {threshold} s — provisioning does not complete (see provision-stuck/worker journal for the cause)"
                    : $"cluster {cluster.Name} declared (NOT_INITIALIZED): nodes not up, schemas not created",
                new Dictionary<string, string> { ["dbname"] = cluster.DbName ?? "missing" },
                null,
                "cluster declared (config.state=NOT_INITIALIZED), nodes not up: this is a normal lifecycle — worker provisioning brings nodes up and switches state to ACTIVE; being stuck longer than the Patroni budget (600 s) is no longer a normal cycle",
                AlertRemedy.WorkerAuto,
                stuckFor
                    ? "see /pgworker/work/<C> (last_error/fail_count) and worker logs: provisioning stuck forever = worker or environment defect"
                    : "wait for provisioning (worker writes nodes state and clears NOT_INITIALIZED); if stuck longer than usual — see the worker journal");
        }
    }
}
