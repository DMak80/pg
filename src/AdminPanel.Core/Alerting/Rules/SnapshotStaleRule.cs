using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// snapshot-stale (warning): BuiltAtUtc старше 3×RefreshInterval (arch/03 §4).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class SnapshotStaleRule : IAlertRule
{
    public const string KindName = "snapshot-stale";

    // «старше 3×RefreshInterval» — константа каталога, не настройка (spec §3.6).
    // Public: порог OverviewDto.stale использует ту же константу (Task 4).
    public const double Multiplier = 3;

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var threshold = TimeSpan.FromSeconds(Multiplier * context.RefreshIntervalSeconds);
        var age = context.NowUtc - snapshot.BuiltAtUtc;
        if (age <= threshold)
            yield break;

        yield return new Alert(
            $"{KindName}:snapshot",
            AlertSeverity.Warning,
            KindName,
            "snapshot",
            $"снапшот устарел: возраст {(long)age.TotalSeconds} c при пороге {(long)threshold.TotalSeconds} c",
            new Dictionary<string, string>
            {
                ["ageSeconds"] = ((long)age.TotalSeconds).ToString(),
                ["thresholdSeconds"] = ((long)threshold.TotalSeconds).ToString(),
                ["builtAtUnix"] = snapshot.BuiltAtUtc.ToUnixTimeSeconds().ToString(),
            },
            null);
    }
}
