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
            null,
            "raft-признаки отсутствия лидера: запись невозможна у всех участников — декларации воркеров и тики панели встанут; кластер etcd обязан держать большинство",
            AlertRemedy.OperatorRunbook,
            "восстановите кворум по arch/09 (перезапуск упавших членов, при потере большинства — восстановление из снапшота)");
    }
}
