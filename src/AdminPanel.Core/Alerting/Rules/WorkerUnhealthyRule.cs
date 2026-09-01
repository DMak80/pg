using AdminPanel.Infrastructure.DI;

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
                ? $"/healthz отвечает не-200 ({w.Detail ?? "degraded"})"
                : $"недостижим по URL lease-ключа ({w.Detail ?? "network error"})";
            yield return new Alert(
                $"{KindName}:pgworker/{w.InstanceId}",
                AlertSeverity.Warning,
                KindName,
                $"pgworker/{w.InstanceId}",
                $"инстанс PgWorker {w.InstanceId} нездоров: {what}",
                new Dictionary<string, string>
                {
                    ["url"] = w.Url,
                    ["checked_unix"] = w.CheckedAtUtc.ToUnixTimeSeconds().ToString(),
                },
                null,
                "lease-ключ жив, но health-проба процесса плохая: секции /healthz (etcd/docker-хосты/циклы/снапшот) деградированы; docker-healthcheck гасит контейнер — за этим последует исчезновение lease и critical worker-api-unreachable",
                AlertRemedy.OperatorRunbook,
                "смотрите docker logs pgworker и /healthz напрямую (секции etcd-reachable/docker-hosts/loops-alive/snapshot); поднимите зависимость (etcd/docker) или перезапустите контейнер воркера (deploy/docker-compose.yml)");
        }
    }
}
