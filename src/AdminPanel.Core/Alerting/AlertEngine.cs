using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting;

// Каркас: прогон правил → sinceUnix из прошлого снапшота → детерминированная сортировка (spec §4.1).
// Id ("kind:target") формируют правила; движок не меняет id, только SinceUnix.
[InjectAsSingleton(typeof(IAlertEngine))]
public sealed class AlertEngine(IEnumerable<IAlertRule> rules) : IAlertEngine
{
    // Severity по убыванию: Critical → Warning → Info (spec §3.10).
    private static readonly IComparer<AlertSeverity> SeverityDescending =
        Comparer<AlertSeverity>.Create((x, y) => y.CompareTo(x));

    public IReadOnlyList<Alert> Evaluate(
        EtcdSnapshot snapshot,
        EtcdSnapshot? previous,
        DateTimeOffset nowUtc,
        double refreshIntervalSeconds)
    {
        var context = new AlertContext(previous, nowUtc, refreshIntervalSeconds);
        var nowUnix = nowUtc.ToUnixTimeSeconds();
        return
        [
            .. rules
               .SelectMany(r => r.Evaluate(snapshot, context))
               .Select(a => a with { SinceUnix = ResolveSince(a, previous, nowUnix) })
               .OrderBy(a => a.Severity, SeverityDescending)
               .ThenBy(a => a.Kind, StringComparer.Ordinal)
               .ThenBy(a => a.Target, StringComparer.Ordinal),
        ];
    }

    // sinceUnix: previous нет → null; id был в previous → перенос (в т.ч. null);
    // новый id → unix текущей оценки (spec §3.4).
    private static long? ResolveSince(Alert alert, EtcdSnapshot? previous, long nowUnix)
    {
        if (previous is null)
            return null;
        var before = previous.Alerts.FirstOrDefault(a => a.Id == alert.Id);
        return before is null ? nowUnix : before.SinceUnix;
    }
}
