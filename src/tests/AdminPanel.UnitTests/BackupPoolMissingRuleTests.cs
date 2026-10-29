using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-pool-missing (каталог 03 §4): Active-кластер, policy без s3 →
// critical; с s3 → нет; policy отсутствует → алерт (дефолт = пул не выбран).
public class BackupPoolMissingRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);

    private static ClusterBackupsInfo Cluster(BackupsPolicyInfo? policy) => new(
        "demo", 86400,
        new Dictionary<string, long?>(), Policy: policy);

    private static EtcdSnapshot SnapshotWithBackups(ClusterBackupsInfo[] backups)
        => TestSnapshots.Healthy(Now) with { Backups = [.. backups] };

    private static IReadOnlyList<Alert> Evaluate(EtcdSnapshot snapshot)
        => [.. new BackupPoolMissingRule().Evaluate(snapshot, new AlertContext(null, Now, 3))];

    // AAA: policy без s3 → critical-алерт по кластеру.
    [Fact]
    public void PolicyWithoutS3_CriticalAlert()
    {
        // Arrange — кластер с policy без секции s3.
        var snapshot = SnapshotWithBackups([Cluster(
            new BackupsPolicyInfo(1, 1, 1, 86400, true, 1))]);

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().ContainSingle();
        alerts[0].Severity.Should().Be(AlertSeverity.Critical);
        alerts[0].Kind.Should().Be("backup-pool-missing");
        alerts[0].Target.Should().Be("demo");
    }

    // AAA: policy с s3 → алерта нет.
    [Fact]
    public void PolicyWithS3_NoAlert()
    {
        // Arrange
        var snapshot = SnapshotWithBackups([Cluster(
            new BackupsPolicyInfo(1, 1, 1, 86400, true, 1,
                S3: new BackupsPolicyS3Info("backups", ["n1"], ["n1"])))]);

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: policy-ключа нет вовсе → алерт (молчание = «не выбран», arch/19 §4).
    [Fact]
    public void NoPolicy_Alert()
    {
        // Arrange
        var snapshot = SnapshotWithBackups([Cluster(null)]);

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().ContainSingle();
    }

    // AAA: кластер не-Active (переходное окно демонтажа) — алерта нет (03 §4:
    // только Active-кластер; ложный critical на демонтаже недопустим — демонтаж
    // ключи бэкапов чистит сам).
    [Theory]
    [InlineData(ClusterState.NotInitialized)]
    [InlineData(ClusterState.ToRemove)]
    public void NotActiveCluster_NoAlert(ClusterState state)
    {
        // Arrange — policy без s3, но кластер вне Active.
        var snapshot = SnapshotWithBackups([Cluster(null)]) with
        {
            Clusters = [TestSnapshots.FullCluster() with { State = state }],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }
}
