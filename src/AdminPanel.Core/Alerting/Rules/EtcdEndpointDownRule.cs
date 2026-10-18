using AdminPanel.Core.Alerting;
using Shared.Core.DI;

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
                $"etcd endpoint unavailable: {endpoint.Url}",
                new Dictionary<string, string> { ["errors"] = string.Join("; ", endpoint.Errors) },
                null,
                "etcd endpoint not responding: the panel reads with failover over live endpoints, but quorum and performance depend on all members; every endpoint from AdminPanel:Etcd:Endpoints must be alive",
                AlertRemedy.OperatorRunbook,
                "check the stand etcd containers and the network (arch/09); an endpoint taken out of the cluster must be removed from AdminPanel:Etcd:Endpoints");
    }
}
