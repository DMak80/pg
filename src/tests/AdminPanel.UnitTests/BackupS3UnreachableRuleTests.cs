using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-s3-unreachable (spec §4.5): configured && consecutiveFailures >= 2
// → warning с target endpoint/bucket; выключенная грань/порог не достигнут/
// успех — молчание (снимается первым успешным тиком).
public class BackupS3UnreachableRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 13, 12, 0, 0, TimeSpan.Zero);

    private static IReadOnlyList<Alert> Evaluate(EtcdSnapshot snapshot) =>
        [.. new BackupS3UnreachableRule().Evaluate(snapshot, new AlertContext(null, Now, 3))];

    private static EtcdSnapshot SnapshotWith(S3StorageInfo? s3)
        => TestSnapshots.Healthy(Now) with { S3Storage = s3 };

    private static S3StorageInfo Storage(
        bool configured = true, int failures = 0, string? error = null) => new(
        Configured: configured, Endpoints: ["http://s3:9000"], Bucket: "pgworker-backups",
        Health: null, Buckets: [], UsedBytes: 0, ObjectCount: 0,
        Clusters: [], ForeignPrefixes: [], UpdatedAtUnix: 1000,
        ConsecutiveFailures: failures, LastError: error);

    // AAA: S3Storage = null (грань выключена) → алерта нет
    [Fact]
    public void Evaluate_NoS3Storage_NoAlert()
    {
        // Arrange — снапшот без S3Storage
        var snapshot = SnapshotWith(null);

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: configured, но 1 неудачный тик — порог 2 не достигнут → молчание
    [Fact]
    public void Evaluate_ConfiguredUnderThreshold_NoAlert()
    {
        // Arrange
        var snapshot = SnapshotWith(Storage(failures: 1, error: "down"));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: 2 неудачных тика подряд → warning с kind/target/полем/Hint/Remedy
    [Fact]
    public void Evaluate_TwoFailures_Warning()
    {
        // Arrange
        var snapshot = SnapshotWith(Storage(failures: 2, error: "s3 down"));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Kind.Should().Be("backup-s3-unreachable");
        alert.Target.Should().Be("http://s3:9000/pgworker-backups");
        alert.Details!["consecutiveFailures"].Should().Be("2");
        alert.Details["bucket"].Should().Be("pgworker-backups");
        alert.Hint.Should().Contain("bucket").And.Contain("PgWorker");
        alert.Remedy.Should().Be(AlertRemedy.OperatorRunbook);
    }

    // AAA: failures = 0 (первый успешный тик) → алерт снят
    [Fact]
    public void Evaluate_SuccessResets_NoAlert()
    {
        // Arrange
        var snapshot = SnapshotWith(Storage(failures: 0));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }
}
