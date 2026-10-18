using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// etcd-unreachable (critical): consecutiveFailures >= 2 тиков (arch/03 §4, spec §4.2).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class EtcdUnreachableRule : IAlertRule
{
    public const string KindName = "etcd-unreachable";

    // Порог каталога «>= 2 тиков» — константа, не настройка (spec §3.6).
    public const int Threshold = 2;

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var failures = snapshot.Etcd.ConsecutiveFailures;
        if (failures < Threshold)
            yield break;

        yield return new Alert(
            $"{KindName}:etcd",
            AlertSeverity.Critical,
            KindName,
            "etcd",
            $"etcd unreachable: {failures} consecutive failed ticks",
            new Dictionary<string, string> { ["consecutiveFailures"] = failures.ToString() },
            SinceUnix: null, // проставляет AlertEngine (spec §3.4)
            Hint: "all etcd endpoints are unreachable: the panel keeps serving stale data (BuiltAtUtc does not advance) and cannot see contract changes; etcd is the single source of truth and must be reachable",
            Remedy: AlertRemedy.OperatorRunbook,
            RemedyText: "check the network/containers of the etcd stand contour (as-etcd) per arch/09; after recovery the tick will pick up the data on its own");
    }
}
