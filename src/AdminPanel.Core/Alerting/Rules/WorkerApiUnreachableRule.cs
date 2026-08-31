using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

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
            "API PgWorker недоступен: живых ключей /pgworker/api/ нет — мутации из панели 503; чтение данных не страдает",
            null,
            null,
            Hint: "воркер ставит lease-ключ при старте; ключа нет = воркер не поднялся или умер ≤15 c назад",
            Remedy: AlertRemedy.OperatorRunbook,
            RemedyText: "запустите контейнер воркера (deploy/docker-compose.yml), проверьте /healthz и PgWorker:Api:AdvertiseUrl");
    }
}
