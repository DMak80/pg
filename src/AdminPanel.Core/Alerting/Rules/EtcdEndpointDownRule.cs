using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// etcd-endpoint-down (warning): endpoint из настроек недоступен — по одному на endpoint (arch/03 §4).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class EtcdEndpointDownRule : IAlertRule
{
    public const string KindName = "etcd-endpoint-down";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var endpoint in snapshot.Etcd.Endpoints.Where(e => !e.Reachable))
            yield return new Alert(
                $"{KindName}:{endpoint.Url}",
                AlertSeverity.Warning,
                KindName,
                endpoint.Url,
                $"endpoint etcd недоступен: {endpoint.Url}",
                new Dictionary<string, string> { ["errors"] = string.Join("; ", endpoint.Errors) },
                null,
                "endpoint etcd не отвечает: панель читает с failover по живым, но кворум и производительность зависят от всех участников; каждый endpoint из AdminPanel:Etcd:Endpoints обязан быть жив",
                AlertRemedy.OperatorRunbook,
                "проверьте контейнеры etcd стенда и сеть (arch/09); endpoint, выведенный из кластера, уберите из AdminPanel:Etcd:Endpoints");
    }
}
