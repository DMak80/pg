using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// etcd-alarm (critical): активные тревоги /v3/maintenance/alarm — по одной на alarm (arch/03 §4).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class EtcdAlarmRule : IAlertRule
{
    public const string KindName = "etcd-alarm";

    public string Kind => KindName;

    // Строчное имя типа тревоги; толерантность к будущим типам etcd — "unknown" (spec §3.7).
    // Public: тот же маппинг использует EtcdStatusMapper (Task 4) — единый источник.
    public static string AlarmTypeName(EtcdAlarmType type)
        => type switch
        {
            EtcdAlarmType.NoSpace => "nospace",
            EtcdAlarmType.Corrupt => "corrupt",
            _ => "unknown",
        };

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var alarm in snapshot.Etcd.Alarms)
        {
            var type = AlarmTypeName(alarm.Type);
            yield return new Alert(
                $"{KindName}:{alarm.MemberId}:{type}",
                AlertSeverity.Critical,
                KindName,
                $"{alarm.MemberId}:{type}",
                $"тревога etcd {type.ToUpperInvariant()} на member {alarm.MemberId}",
                new Dictionary<string, string>
                {
                    ["memberId"] = alarm.MemberId.ToString(),
                    ["alarmType"] = type,
                },
                null);
        }
    }
}
