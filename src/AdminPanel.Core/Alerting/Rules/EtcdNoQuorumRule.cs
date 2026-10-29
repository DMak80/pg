using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// etcd-no-quorum (critical): raft-признаки отсутствия лидера — QuorumSuspected §3.11 (spec §4.2).
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
            "suspected loss of etcd quorum (raft without a leader)",
            new Dictionary<string, string>
            {
                ["errors"] = string.Join("; ", snapshot.Etcd.Endpoints.SelectMany(e => e.Errors)),
            },
            null,
            "raft signs of a missing leader: writes fail on all members — worker declarations and panel heartbeats will stall; the etcd cluster must keep a majority",
            AlertRemedy.OperatorRunbook,
            "restore quorum per arch/09 (restart failed members; if the majority is lost — restore from a snapshot)");
    }
}
