using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-verify-failed (t04, spec §3.6/AC7): critical per-shard на невалидный
// полный, текст — verify.error; без FAILED и на пустом префиксе — молчит.
public class BackupVerifyFailedRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 11, 12, 0, 0, TimeSpan.Zero);

    private static IReadOnlyList<Alert> Evaluate(IAlertRule rule, EtcdSnapshot snapshot)
        => [.. rule.Evaluate(snapshot, new AlertContext(null, Now, 3))];

    private static EtcdSnapshot Snapshot(params ClusterBackupsInfo[] backups)
        => TestSnapshots.Healthy(Now) with { Backups = [.. backups] };

    // AAA: FAILED → critical с текстом verify.error и target C/X (AC7)
    [Fact]
    public void FailedVerify_CriticalAlertСErrorТекстом()
    {
        // Arrange — шарда s1: невалидный полный с причиной цепочки
        var rule = new BackupVerifyFailedRule();
        var snapshot = Snapshot(new ClusterBackupsInfo("demo", null,
            new Dictionary<string, long?> { ["s1"] = null },
            null,
            ShardVerifyFailures: new Dictionary<string, ShardVerifyFailure>
            {
                ["s1"] = new("s1", "20260911120000Z", "дыра WAL-цепочки: ожидался A, найден B", 1757500600),
            }));

        // Act
        var alerts = Evaluate(rule, snapshot);

        // Assert — канон: kind/severity/target/id + текст ошибки в message
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("backup-verify-failed");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Target.Should().Be("demo/s1");
        alert.Id.Should().Be("backup-verify-failed:demo/s1");
        alert.Message.Should().Contain("20260911120000Z").And.Contain("дыра WAL-цепочки");
        alert.Details!["backupId"].Should().Be("20260911120000Z");
    }

    // AAA: без FAILED — молчит (AC7)
    [Fact]
    public void БезFailed_Молчит()
    {
        // Arrange — свежие валидные полные, failures пуст
        var rule = new BackupVerifyFailedRule();
        var snapshot = Snapshot(new ClusterBackupsInfo("demo", null,
            new Dictionary<string, long?> { ["s1"] = Now.ToUnixTimeSeconds() - 3600 },
            null,
            ShardVerifyFailures: new Dictionary<string, ShardVerifyFailure>()));

        // Act
        var alerts = Evaluate(rule, snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: пустой префикс — молчит (AC7)
    [Fact]
    public void ПустойПрефикс_Молчит()
    {
        // Arrange — подсистема не включена (нет Backups вовсе)
        var rule = new BackupVerifyFailedRule();

        // Act
        var alerts = Evaluate(rule, TestSnapshots.Healthy(Now));

        // Assert
        alerts.Should().BeEmpty();
    }
}
