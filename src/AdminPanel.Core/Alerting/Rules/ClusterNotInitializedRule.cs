using AdminPanel.Infrastructure.DI;
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
                    ? $"кластер {cluster.Name} висит в NOT_INITIALIZED дольше {threshold} c — provisioning не завершается (причину см. provision-stuck/journal воркера)"
                    : $"кластер {cluster.Name} заявлен (NOT_INITIALIZED): ноды не подняты, схемы не созданы",
                new Dictionary<string, string> { ["dbname"] = cluster.DbName ?? "missing" },
                null,
                "кластер заявлен (config.state=NOT_INITIALIZED), ноды не подняты: это нормальный жизненный цикл — provisioning воркера поднимает ноды и переведёт state в ACTIVE; зависание дольше бюджета Patroni (600 c) — уже не нормальный цикл",
                AlertRemedy.WorkerAuto,
                stuckFor
                    ? "смотрите /pgworker/work/<C> (last_error/fail_count) и логи воркера: вечный provisioning = дефект воркера или окружения"
                    : "дождитесь provisioning (воркер пишет nodes state и снимет NOT_INITIALIZED); висит дольше обычного — смотрите journal воркера");
        }
    }
}
