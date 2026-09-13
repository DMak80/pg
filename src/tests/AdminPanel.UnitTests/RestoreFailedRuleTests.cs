using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// restore-failed (t05, spec §3.7/AC8): FAILED restore-заявки живого шарда
// Active-кластера → один critical-алерт; активные фазы и чужие/удалённые
// кластеры — молчание.
public class RestoreFailedRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 10, 12, 0, 0, TimeSpan.Zero);

    private static IReadOnlyList<Alert> Evaluate(IAlertRule rule, EtcdSnapshot snapshot)
        => [.. rule.Evaluate(snapshot, new AlertContext(null, Now, 3))];

    private static EtcdSnapshot Snapshot(params ClusterBackupsInfo[] backups)
        => TestSnapshots.Healthy(Now) with { Backups = [.. backups] };

    private static ClusterBackupsInfo ClusterInfo(
        (string Id, string State, string? Error)[] restores)
        => new("demo", null,
            new Dictionary<string, long?> { ["s1"] = 1757380800 },
            ShardsRestores: new Dictionary<string, IReadOnlyList<RestoreOperationInfo>>
            {
                ["s1"] = [.. restores.Select(r => new RestoreOperationInfo(
                    "demo", "s1", r.Id, r.State, r.Error, 1760000000, null, null, null))],
            });

    private readonly RestoreFailedRule _rule = new();

    // AAA: FAILED живого шарда Active-кластера — один critical-алерт, error в
    // тексте и в details, remedy — runbook
    [Fact]
    public void FailedRestore_CriticalAlert()
    {
        // Arrange — FAILED с причиной в живом кластере demo/s1
        var snapshot = Snapshot(ClusterInfo(
            [("20260911120000Z", "FAILED", "дыра WAL-цепочки: ожидался 000000010000000000000003")]));

        // Act
        var alerts = Evaluate(_rule, snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("restore-failed");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Target.Should().Be("demo/s1");
        alert.Id.Should().Be("restore-failed:demo/s1/20260911120000Z");
        alert.Message.Should().Contain("дыра WAL-цепочки");
        alert.Details!["restoreId"].Should().Be("20260911120000Z");
        alert.Remedy.Should().Be(AlertRemedy.OperatorRunbook);
        alert.Hint.Should().Contain("backup-restore.md");
    }

    // AAA: RUNNING/COMPLETED — алерта нет (фазы видны в статусе ключа)
    [Fact]
    public void ActiveOrCompletedPhases_NoAlert()
    {
        // Arrange
        var snapshot = Snapshot(ClusterInfo(
        [
            ("id1", "RUNNING", null),
            ("id2", "PLANNED", null),
            ("id3", "REJOINING", null),
            ("id4", "COMPLETED", null),
        ]));

        // Act / Assert
        Evaluate(_rule, snapshot).Should().BeEmpty();
    }

    // AAA: FAILED, но кластера нет в снапшоте (демонтирован) — алерта нет
    [Fact]
    public void FailedRestore_ClusterGone_NoAlert()
    {
        // Arrange — кластер с чужим именем (в Healthy-снапшоте только demo)
        var backups = new ClusterBackupsInfo("gone", null,
            new Dictionary<string, long?> { ["s1"] = null },
            ShardsRestores: new Dictionary<string, IReadOnlyList<RestoreOperationInfo>>
            {
                ["s1"] = [new RestoreOperationInfo(
                    "gone", "s1", "id1", "FAILED", "boom", 1760000000, null, null, null)],
            });
        var snapshot = Snapshot(backups);

        // Act / Assert
        Evaluate(_rule, snapshot).Should().BeEmpty();
    }
}
