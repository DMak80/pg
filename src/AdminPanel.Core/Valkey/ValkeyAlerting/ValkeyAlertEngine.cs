using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Valkey.ValkeyAlerting;

// Чистая функция (ValkeySnapshot next, prev) → Alert[] — каталог arch/03 §8.4.
// sinceUnix — по стабильному id из prev.Alerts (механика KafkaAlertEngine);
// сортировка severity → kind → target.
public interface IValkeyAlertEngine
{
    IReadOnlyList<Alert> Evaluate(ValkeySnapshot next, ValkeySnapshot? previous);
}

[InjectAsSingleton(typeof(IValkeyAlertEngine))]
public sealed class ValkeyAlertEngine(IOptions<ValkeyAlertsOptions> options) : IValkeyAlertEngine
{
    private static readonly IComparer<AlertSeverity> SeverityDescending =
        Comparer<AlertSeverity>.Create((x, y) => y.CompareTo(x));

    private readonly ValkeyAlertsOptions _options = options.Value;

    public IReadOnlyList<Alert> Evaluate(ValkeySnapshot next, ValkeySnapshot? previous)
    {
        var nowUnix = next.BuiltAtUtc.ToUnixTimeSeconds();
        return
        [
            .. Enumerate(next, previous)
               .Select(a => a with { SinceUnix = ResolveSince(a, previous, nowUnix) })
               .OrderBy(a => a.Severity, SeverityDescending)
               .ThenBy(a => a.Kind, StringComparer.Ordinal)
               .ThenBy(a => a.Target, StringComparer.Ordinal),
        ];
    }

    // Каталог 8 kinds — наполняется задачей алертов (TDD); до неё движок пуст.
    private IEnumerable<Alert> Enumerate(ValkeySnapshot next, ValkeySnapshot? previous)
    {
        yield break;
    }

    // sinceUnix: prev нет → null; id был в prev → перенос; новый → now (pg-механика).
    private static long? ResolveSince(Alert alert, ValkeySnapshot? previous, long nowUnix)
    {
        if (previous is null)
            return null;
        var before = previous.Alerts.FirstOrDefault(a => a.Id == alert.Id);
        return before is null ? nowUnix : before.SinceUnix;
    }
}
