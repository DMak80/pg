using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// worker-api-unreachable (critical, task etcd-via-worker-api): нет живых ключей
// /pgworker/api/ (arch/02 §2.3.1) — мутации панели (прокси в API PgWorker)
// отвечают 503; чтение данных не страдает. Kafka-грань — в KafkaAlertEngine.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class WorkerApiUnreachableRule : IAlertRule
{
    public const string KindName = "worker-api-unreachable";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        if (snapshot.PgWorkerEndpoints.Count > 0)
            yield break;

        yield return new Alert(
            $"{KindName}:pgworker",
            AlertSeverity.Critical,
            KindName,
            "pgworker",
            "PgWorker API unreachable: no live /pgworker/api/ keys — panel mutations return 503; data reads are unaffected",
            null,
            null,
            Hint: "worker writes the lease key at startup; no key = worker did not start or died ≤15 s ago",
            Remedy: AlertRemedy.OperatorRunbook,
            RemedyText: "start the worker container (deploy/docker-compose.yml), check /healthz and PgWorker:Api:AdvertiseUrl");
    }
}
