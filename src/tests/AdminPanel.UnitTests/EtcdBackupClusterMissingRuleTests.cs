using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// EtcdBackupClusterMissingRule (t32, adminpanel/03 §4): S3-домен прочитан и
// живого etcd-backup кластера нет → critical; цель жива/домен не читался — молчание.
public class EtcdBackupClusterMissingRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 1, 12, 0, 0, TimeSpan.Zero);

    private static IReadOnlyList<Alert> Evaluate(EtcdSnapshot snapshot)
        => [.. new EtcdBackupClusterMissingRule().Evaluate(snapshot, new AlertContext(null, Now, 3))];

    // AAA: домен прочитан, кластера с etcd_backup=true нет — critical-алерт
    // с target "etcd" и ремеди создания etcd-backup S3-кластера.
    [Fact]
    public void NoTarget_CriticalAlert()
    {
        // Arrange — домен прочитан, цели нет.
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            EtcdBackupTarget = new EtcdBackupTargetInfo(null),
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("etcd-backup-cluster-missing");
        alert.Target.Should().Be("etcd");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.RemedyText.Should().Contain("etcd-backup S3 cluster");
    }

    // AAA: живой etcd-backup кластер есть — молчание.
    [Fact]
    public void HasTarget_Silent()
    {
        // Arrange
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            EtcdBackupTarget = new EtcdBackupTargetInfo("s3-etcd-main"),
        };

        // Act + Assert
        Evaluate(snapshot).Should().BeEmpty();
    }

    // AAA: S3-домен ещё не читался (факт не внесён) — молчание.
    [Fact]
    public void DomainNotRead_Silent()
    {
        // Arrange — EtcdBackupTarget отсутствует (null).
        var snapshot = TestSnapshots.Healthy(Now);

        // Act + Assert
        Evaluate(snapshot).Should().BeEmpty();
    }
}
