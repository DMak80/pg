using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;

namespace AdminPanel.UnitTests;

// Все правила t04 одним списком: харнессы refresher'а и тест уникальности kind'ов (spec §10.1).
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
        ];
}
