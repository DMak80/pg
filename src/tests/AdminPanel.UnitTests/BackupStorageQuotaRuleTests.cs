using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-storage-quota (t06, spec §3.6/AC6): нет ключа/OK — молчим;
// WARN → warning, CRIT → critical; Message несёт used/percent.
public class BackupStorageQuotaRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 10, 12, 0, 0, TimeSpan.Zero);

    private static IReadOnlyList<Alert> Evaluate(EtcdSnapshot snapshot)
        => [.. new BackupStorageQuotaRule().Evaluate(snapshot, new AlertContext(null, Now, 3))];

    // Снапшот с заданным storage-ключом поверх здорового базиса.
    private static EtcdSnapshot Snapshot(BackupStorageInfo? storage)
        => TestSnapshots.Healthy(Now) with { BackupStorage = storage };

    // AAA: ключа нет — подсистема не включена, алерта нет
    [Fact]
    public void NoKey_NoAlert()
    {
        // Arrange — снапшот без BackupStorage
        var snapshot = Snapshot(null);

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: state=OK — молчим
    [Fact]
    public void Ok_NoAlert()
    {
        // Arrange
        var snapshot = Snapshot(new BackupStorageInfo(10, 1000, 1, BackupStorageState.Ok, Now.ToUnixTimeSeconds()));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: state=WARN → warning с used/percent в Message
    [Fact]
    public void Warn_WarningAlert()
    {
        // Arrange — 82% занятости
        var snapshot = Snapshot(new BackupStorageInfo(820, 1000, 82, BackupStorageState.Warn, Now.ToUnixTimeSeconds()));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().ContainSingle();
        var alert = alerts[0];
        alert.Kind.Should().Be("backup-storage-quota");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Message.Should().Contain("820").And.Contain("82%");
        alert.Hint.Should().Contain("ретенционный проход");
        alert.RemedyText.Should().Contain("квоту");
        alert.Remedy.Should().Be(AlertRemedy.OperatorRunbook);
    }

    // AAA: state=CRIT → critical
    [Fact]
    public void Crit_CriticalAlert()
    {
        // Arrange — 95% занятости
        var snapshot = Snapshot(new BackupStorageInfo(950, 1000, 95, BackupStorageState.Crit, Now.ToUnixTimeSeconds()));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().ContainSingle();
        alerts[0].Severity.Should().Be(AlertSeverity.Critical);
    }
}
