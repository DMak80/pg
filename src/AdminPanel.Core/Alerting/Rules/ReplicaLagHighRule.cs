using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// replica-lag-high (warning): лаг члена по Patroni-пробе > ReplicaLagBytes (arch/03 §4;
// источник — Patroni REST, arch/01 §6; spec §5.1).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ReplicaLagHighRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "replica-lag-high";

    // Каталожный дефолт 16 МБ — фолбэк при опечатке конфига (spec §3.8).
    public const long DefaultBytes = 16 * 1024 * 1024;

    public string Kind => KindName;

    private long ThresholdBytes
        => options.Value.ReplicaLagBytes > 0 ? options.Value.ReplicaLagBytes : DefaultBytes;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var scope in snapshot.HaScopes.Where(s => s.Matched))
        foreach (var member in scope.Members)
        {
            var lag = member.LagBytes;
            if (member.ProbeAtUtc is null || member.ProbeError is not null || lag is null || lag <= ThresholdBytes)
                continue;

            yield return new Alert(
                $"{KindName}:{scope.Scope}/{member.Name}",
                AlertSeverity.Warning,
                KindName,
                $"{scope.Scope}/{member.Name}",
                $"лаг члена {member.Name} scope {scope.Scope} — {lag} байт, порог {ThresholdBytes} байт",
                new Dictionary<string, string>
                {
                    ["lagBytes"] = lag.Value.ToString(),
                    ["thresholdBytes"] = ThresholdBytes.ToString(),
                },
                null);
        }
    }
}
