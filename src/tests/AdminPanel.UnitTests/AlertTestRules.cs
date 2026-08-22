using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using Microsoft.Extensions.Options;

namespace AdminPanel.UnitTests;

// Все правила t04+t05 одним списком: харнессы refresher'а и тест уникальности kind'ов (spec §10.1, §3.16).
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
            new KeyMalformedRule(),
            new ShardNoMasterRule(),
            new MoveStaleRule(Options.Create(new AlertsOptions())),
            new MoveFrozenLongRule(Options.Create(new AlertsOptions())),
            new MoveAbortingRule(),
            new MoveFlippedStatusStuckRule(),
            new BucketLostRule(),
            new BucketNoRoutingRule(),
            new BucketOutOfRangeRule(),
        ];
}
