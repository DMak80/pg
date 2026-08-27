using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

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
            $"etcd недоступен: {failures} подряд неудачных тика",
            new Dictionary<string, string> { ["consecutiveFailures"] = failures.ToString() },
            SinceUnix: null); // проставляет AlertEngine (spec §3.4)
    }
}
