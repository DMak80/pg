using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using Microsoft.Extensions.Options;

namespace AdminPanel.UnitTests;

// Все правила t04+t05+t06 одним списком: харнессы refresher'а и тест уникальности kind'ов (spec §10.1, §3.16).
internal static class AlertTestRules
{
    public static IReadOnlyList<IAlertRule> All()
        =>
        [
            new EtcdUnreachableRule(),
            new EtcdNoQuorumRule(),
            new EtcdEndpointDownRule(),
            new EtcdAlarmRule(),
            new SnapshotStaleRule(),
            new ClusterIncompleteRule(),
            new ClusterNotInitializedRule(Options.Create(new AlertsOptions())),
            new ProvisionStuckRule(Options.Create(new AlertsOptions())),
            new KeyMalformedRule(),
            new ShardNoMasterRule(),
            new MoveStaleRule(Options.Create(new AlertsOptions())),
            new MoveFrozenLongRule(Options.Create(new AlertsOptions())),
            new MoveAbortingRule(),
            new MoveFlippedStatusStuckRule(),
            new BucketLostRule(),
            new BucketNoRoutingRule(),
            new BucketOutOfRangeRule(),
            // t06: 9 HA-правил (spec §5)
            new ShardNoLeaderRule(),
            new HaMemberNotStreamingRule(),
            new ReplicaLagHighRule(Options.Create(new AlertsOptions())),
            new ProbeFailedRule(),
            new SlotLagHighRule(Options.Create(new AlertsOptions())),
            new SlotWalLostRule(),
            new SlotInvalidationRiskRule(Options.Create(new AlertsOptions())),
            new SyncStandbyMissingRule(),
            new InventoryMismatchRule(),
            // task etcd-via-worker-api: доступность API воркера (arch/03 §4.1)
            new WorkerApiUnreachableRule(),
        ];
}
