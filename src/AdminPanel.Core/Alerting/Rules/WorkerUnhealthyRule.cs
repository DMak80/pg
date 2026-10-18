using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// worker-unhealthy (warning): живой lease-ключ /pgworker/api/<id>, но /healthz ≠ 200 —
// процесс нездоров ДО истечения lease (docker-healthcheck гасит контейнер, ключи
// вот-вот исчезнут → эстафета worker-api-unreachable critical). arch/adminpanel/03 §4.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class WorkerUnhealthyRule : IAlertRule
{
    public const string KindName = "worker-unhealthy";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var w in snapshot.WorkerHealth.Where(w => w.Status != WorkerHealthStatus.Healthy))
        {
            var what = w.Status == WorkerHealthStatus.Degraded
                ? $"/healthz returned non-200 ({w.Detail ?? "degraded"})"
                : $"unreachable at the lease-key URL ({w.Detail ?? "network error"})";
            yield return new Alert(
                $"{KindName}:pgworker/{w.InstanceId}",
                AlertSeverity.Warning,
                KindName,
                $"pgworker/{w.InstanceId}",
                $"PgWorker instance {w.InstanceId} is unhealthy: {what}",
                new Dictionary<string, string>
                {
                    ["url"] = w.Url,
                    ["checked_unix"] = w.CheckedAtUtc.ToUnixTimeSeconds().ToString(),
                },
                null,
                "lease key is alive but the process health probe is failing: /healthz sections (etcd/docker hosts/loops/snapshot) are degraded; docker-healthcheck will stop the container — the lease will disappear next, followed by critical worker-api-unreachable",
                AlertRemedy.OperatorRunbook,
                "see docker logs pgworker and /healthz directly (sections etcd-reachable/docker-hosts/loops-alive/snapshot); restore the dependency (etcd/docker) or restart the worker container (deploy/docker-compose.yml)");
        }
    }
}
