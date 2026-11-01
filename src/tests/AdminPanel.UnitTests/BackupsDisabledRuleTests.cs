using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Правило backups-disabled (u10): ключ жив → warning-алерт с Remedy
// OperatorRunbook; ключа нет → алерта нет (правило молчит). AAA.
public class BackupsDisabledRuleTests
{
    private static readonly AlertContext Context = new(null, DateTimeOffset.FromUnixTimeSeconds(1761900600), 3);

    [Fact]
    public void Evaluate_KeyAlive_WarningAlertWithRunbookRemedy()
    {
        // Arrange — глобальный стоп бэкапов
        var snapshot = TestSnapshots.Healthy(DateTimeOffset.FromUnixTimeSeconds(1761900000)) with
        {
            BackupsDisabled = new BackupsDisabledInfo(1761900000, "ops"),
        };

        // Act
        var alerts = new BackupsDisabledRule().Evaluate(snapshot, Context).ToList();

        // Assert
        alerts.Should().ContainSingle();
        var alert = alerts.Single();
        alert.Kind.Should().Be("backups-disabled");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Target.Should().Be("global");
        alert.Remedy.Should().Be(AlertRemedy.OperatorRunbook);
        alert.RemedyText.Should().Contain("/pgworker/backups-disabled").And.Contain("Backups:Enabled");
        alert.Hint.Should().Contain("стоп бэкапов");
        alert.Id.Should().Be("backups-disabled:global");
    }

    [Fact]
    public void Evaluate_NoKey_Silent()
    {
        // Arrange — ключа нет (бэкапы работают)
        var snapshot = TestSnapshots.Healthy(DateTimeOffset.FromUnixTimeSeconds(1761900000)) with { BackupsDisabled = null };

        // Act
        var alerts = new BackupsDisabledRule().Evaluate(snapshot, Context).ToList();

        // Assert
        alerts.Should().BeEmpty();
    }
}
