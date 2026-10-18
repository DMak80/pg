using AdminPanel.Core.Alerting;
using Shared.Core.DI;

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
            $"snapshot is stale: age {(long)age.TotalSeconds} s exceeds threshold {(long)threshold.TotalSeconds} s",
            new Dictionary<string, string>
            {
                ["ageSeconds"] = ((long)age.TotalSeconds).ToString(),
                ["thresholdSeconds"] = ((long)threshold.TotalSeconds).ToString(),
                ["builtAtUnix"] = snapshot.BuiltAtUtc.ToUnixTimeSeconds().ToString(),
            },
            null,
            "panel snapshot has not been refreshed for more than 3 ticks: the panel is running on stale data (BuiltAtUtc is not advancing), UI and alerts may be inaccurate; a tick must complete every RefreshIntervalSeconds",
            AlertRemedy.OperatorRunbook,
            "check the etcd environment per arch/09 (endpoints/quorum/alarms) — the refresher will recover on its own after the etcd cluster is restored");
    }
}
