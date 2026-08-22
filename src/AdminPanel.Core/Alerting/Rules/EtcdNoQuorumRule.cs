using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// etcd-no-quorum (critical): raft-признаки отсутствия лидера — QuorumSuspected t03 §3.11 (spec §4.2).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class EtcdNoQuorumRule : IAlertRule
{
    public const string KindName = "etcd-no-quorum";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        if (!snapshot.Etcd.QuorumSuspected)
            yield break;

        yield return new Alert(
            $"{KindName}:etcd",
            AlertSeverity.Critical,
            KindName,
            "etcd",
            "подозрение на отсутствие кворума etcd (raft без лидера)",
            new Dictionary<string, string>
            {
                ["errors"] = string.Join("; ", snapshot.Etcd.Endpoints.SelectMany(e => e.Errors)),
            },
            null);
    }
}
