using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-drill-stale (warning, reliability t02, arch/19 §3.6): у шарда
// Active-кластера есть COMPLETED-полные и интервал включён, а успешного
// дрилла нет/он старше 2×интервала — восстановимость не доказывается.
public class BackupDrillStaleRuleTests
{
    // 2026-10-05 12:00 UTC = 1790001600 (пороги считаются от него).
    private static readonly DateTimeOffset Now = new(2026, 10, 5, 12, 0, 0, TimeSpan.Zero);

    private readonly BackupDrillStaleRule _rule = new();

    private static IReadOnlyList<Alert> Evaluate(IAlertRule rule, EtcdSnapshot snapshot)
        => [.. rule.Evaluate(snapshot, new AlertContext(null, Now, 3))];

    private static EtcdSnapshot Snapshot(params ClusterBackupsInfo[] backups)
        => TestSnapshots.Healthy(Now) with { Backups = [.. backups] };

    private static ClusterBackupsInfo ClusterInfo(
        int? drillIntervalDays,
        (string Shard, long? LastCompleted, string? DrillState, long? DrillFinished)[] shards)
        => new("demo", null,
            shards.ToDictionary(s => s.Shard, s => s.LastCompleted),
            ShardsDrills: shards.Where(s => s.DrillState is not null)
                .ToDictionary(s => s.Shard, s => new DrillInfo(
                    "demo", s.Shard, "20261001120000Z", s.DrillState!, "b1",
                    100, s.DrillFinished, null, null, null)),
            Policy: new BackupsPolicyInfo(null, null, null, null, null, drillIntervalDays));

    // AAA: COMPLETED-полные есть, интервал включён (policy 1), успешного
    // дрилла нет → warning.
    [Fact]
    public void Evaluate_NoSuccess_Warning()
    {
        // Arrange — полный свежий, дриллов не было вовсе.
        var snapshot = Snapshot(ClusterInfo(1,
            [("s1", Now.ToUnixTimeSeconds() - 60, null, null)]));

        // Act
        var alerts = Evaluate(_rule, snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("backup-drill-stale");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Target.Should().Be("demo/s1");
    }

    // AAA: успешный старше 2×interval → warning; свежий (≤ interval) → молчит.
    [Fact]
    public void Evaluate_StaleSuccess_Warning_FreshSilent()
    {
        // Arrange — интервал 2 суток (172800 c); порог горения 2×interval.
        var stale = ClusterInfo(2,
            [("s1", Now.ToUnixTimeSeconds(), "SUCCEEDED", Now.ToUnixTimeSeconds() - 2L * 172800 - 1)]);
        var fresh = ClusterInfo(2,
            [("s1", Now.ToUnixTimeSeconds(), "SUCCEEDED", Now.ToUnixTimeSeconds() - 172800)]);

        // Act
        var staleAlerts = Evaluate(_rule, Snapshot(stale));
        var freshAlerts = Evaluate(_rule, Snapshot(fresh));

        // Assert
        staleAlerts.Should().ContainSingle(a => a.Kind == "backup-drill-stale");
        freshAlerts.Should().BeEmpty("свежий успех — восстановимость доказывается");
    }

    // AAA: последний FAILED не «освежает» — при включённом интервале без
    // свежего успеха правило горит (молчание только при свежем SUCCEEDED).
    [Fact]
    public void Evaluate_LastFailed_NoFreshSuccess_Warning()
    {
        // Arrange — последний дрилл FAILED свежий (дрилл «выполнялся»!),
        // но успешного нет: восстановимость НЕ доказана.
        var snapshot = Snapshot(ClusterInfo(1,
            [("s1", Now.ToUnixTimeSeconds(), "FAILED", Now.ToUnixTimeSeconds() - 60)]));

        // Act
        var alerts = Evaluate(_rule, snapshot);

        // Assert
        alerts.Should().ContainSingle(a => a.Kind == "backup-drill-stale");
    }

    // AAA: interval_days=0 (policy) → выключено, молчит; пустой префикс
    // бэкапов кластера (нет COMPLETED) → молчит.
    [Fact]
    public void Evaluate_DisabledOrEmptyPrefix_Silent()
    {
        // Arrange — policy интервал 0; второй кластер без COMPLETED-полных.
        var disabled = ClusterInfo(0,
            [("s1", Now.ToUnixTimeSeconds(), null, null)]);
        var empty = new ClusterBackupsInfo("demo2", null,
            new Dictionary<string, long?> { ["s1"] = null });

        // Act
        var alerts = Evaluate(_rule, Snapshot(disabled, empty));

        // Assert
        alerts.Should().BeEmpty();
    }
}
