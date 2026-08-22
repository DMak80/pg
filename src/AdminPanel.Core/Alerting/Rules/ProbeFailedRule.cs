using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// probe-failed (info): каждая неудавшаяся проба тика — один алерт на цель
// (arch/03 §4; severity по каталогу — конфликт с обзором arch/01 §8 разрешён
// каталогом, spec §3.9; target "{kind}:{target}" — уникальный id, §3.14).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ProbeFailedRule : IAlertRule
{
    public const string KindName = "probe-failed";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var probe in snapshot.Probes.Where(p => !p.Ok))
        {
            var target = $"{probe.Kind}:{probe.Target}";
            yield return new Alert(
                $"{KindName}:{target}",
                AlertSeverity.Info,
                KindName,
                target,
                $"проба {probe.Kind} по {probe.Target} не удалась: {probe.Error}",
                new Dictionary<string, string>
                {
                    ["kind"] = probe.Kind,
                    ["target"] = probe.Target,
                    ["error"] = probe.Error ?? string.Empty,
                },
                null);
        }
    }
}
