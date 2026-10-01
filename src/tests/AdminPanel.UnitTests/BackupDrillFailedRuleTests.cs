using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-drill-failed (critical, reliability t02, arch/19 §3.6): последний
// дрилл FAILED живого шарда Active-кластера → алерт; прочие состояния и
// не-Active/удалённые — молчание.
public class BackupDrillFailedRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly BackupDrillFailedRule _rule = new();

    private static IReadOnlyList<Alert> Evaluate(IAlertRule rule, EtcdSnapshot snapshot)
        => [.. rule.Evaluate(snapshot, new AlertContext(null, Now, 3))];

    private static EtcdSnapshot Snapshot(params ClusterBackupsInfo[] backups)
        => TestSnapshots.Healthy(Now) with { Backups = [.. backups] };

    private static ClusterBackupsInfo ClusterInfo(
        string cluster, (string Shard, string State, string? Error)[] drills)
        => new(cluster, null,
            new Dictionary<string, long?> { ["s1"] = 1757380800 },
            ShardsDrills: new Dictionary<string, DrillInfo>(
                drills.Select(d => (d.Shard, new DrillInfo(
                    cluster, d.Shard, "20261001120000Z", d.State, "20261001090000Z",
                    100, 400, null, null, d.Error)))
                    .ToDictionary(x => x.Item1, x => x.Item2)));

    // AAA: последний дрилл FAILED живого Active-кластера → critical, labels
    // drillId/backupId, текст error.
    [Fact]
    public void Evaluate_FailedDrill_CriticalAlert()
    {
        // Arrange — FAILED с причиной на живом шарде demo/s1.
        var snapshot = Snapshot(ClusterInfo("demo",
            [("s1", "FAILED", "recovery-бюджет исчерпан")]));

        // Act
        var alerts = Evaluate(_rule, snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("backup-drill-failed");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Target.Should().Be("demo/s1");
        alert.Message.Should().Contain("recovery-бюджет исчерпан");
        alert.Details!["drillId"].Should().Be("20261001120000Z");
        alert.Details["backupId"].Should().Be("20261001090000Z");
        alert.Remedy.Should().Be(AlertRemedy.OperatorRunbook);
    }

    // AAA: SUCCEEDED/RUNNING/фазы — без алерта.
    [Fact]
    public void Evaluate_NonFailed_NoAlert()
    {
        // Arrange — все не-FAILED состояния (в т.ч. фазы сноса).
        var snapshot = Snapshot(
            ClusterInfo("demo", [("s1", "SUCCEEDED", null)]),
            new ClusterBackupsInfo("demo2", null,
                new Dictionary<string, long?> { ["s1"] = 1757380800 },
                ShardsDrills: new Dictionary<string, DrillInfo>
                {
                    ["s1"] = new("demo2", "s1", "d2", "RUNNING", "b2", 100, null, "recovering", null, null),
                }));

        // Act
        var alerts = Evaluate(_rule, snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: не-Active кластер / шард удалён из декларации — молчим.
    [Fact]
    public void Evaluate_NonActiveOrGoneShard_Silent()
    {
        // Arrange — FAILED на шарде, которого нет в декларации Active-кластера.
        var snapshot = Snapshot(ClusterInfo("demo",
            [("gone", "FAILED", "boom")]));

        // Act
        var alerts = Evaluate(_rule, snapshot);

        // Assert
        alerts.Should().BeEmpty("шард удалён из декларации — правило молчит");
    }

    // AAA: drill-ключей нет (подсистема не включена) — молчим.
    [Fact]
    public void Evaluate_NoDrills_Silent()
    {
        // Arrange — кластер с полными, но без drill-ключей.
        var snapshot = Snapshot(new ClusterBackupsInfo("demo", null,
            new Dictionary<string, long?> { ["s1"] = 1757380800 }));

        // Act
        var alerts = Evaluate(_rule, snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }
}
