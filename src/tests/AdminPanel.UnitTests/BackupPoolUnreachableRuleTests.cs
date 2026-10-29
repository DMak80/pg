using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-pool-unreachable (каталог 03 §4): маркер s3-pool-exhausted в
// wal DEGRADED / full FAILED → critical; без маркера → нет; чистый статус → нет.
public class BackupPoolUnreachableRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);
    private const string Marker = "s3-pool-exhausted";

    private static EtcdSnapshot Snapshot(ClusterBackupsInfo[] backups)
        => TestSnapshots.Healthy(Now) with { Backups = [.. backups] };

    private static IReadOnlyList<Alert> Evaluate(EtcdSnapshot snapshot)
        => [.. new BackupPoolUnreachableRule().Evaluate(snapshot, new AlertContext(null, Now, 3))];

    private static ClusterBackupsInfo Cluster(
        WalStreamInfo? wal = null,
        (string Id, string Error)? failedFull = null)
        => new("demo", 86400,
            new Dictionary<string, long?>(),
            Shards: wal is null
                ? new Dictionary<string, WalStreamInfo?>()
                : new Dictionary<string, WalStreamInfo?> { ["s1"] = wal },
            ShardsFulls: failedFull is null
                ? new Dictionary<string, IReadOnlyList<BackupFullInfo>>()
                : new Dictionary<string, IReadOnlyList<BackupFullInfo>>
                {
                    ["s1"] = [new BackupFullInfo(
                        failedFull.Value.Id, "FAILED", failedFull.Value.Error,
                        1760000000, 1760000100, 100, null, null, null)],
                });

    // AAA: wal DEGRADED с маркером → critical с текстом ошибки.
    [Fact]
    public void WalDegradedWithMarker_Critical()
    {
        // Arrange
        var snapshot = Snapshot([Cluster(wal: new WalStreamInfo(
            "demo", "s1", WalStreamInfoState.Degraded, "slot", "n1", 1760000000,
            null, $"{Marker}: put wal/000000010000000000000001 — all endpoints in the list are unreachable"))]);

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().ContainSingle();
        alerts[0].Severity.Should().Be(AlertSeverity.Critical);
        alerts[0].Message.Should().Contain(Marker);
    }

    // AAA: full FAILED с маркером → алерт.
    [Fact]
    public void FullFailedWithMarker_Alert()
    {
        // Arrange
        var snapshot = Snapshot([Cluster(failedFull: ("20261015120000Z", $"{Marker}: upload full failed on all pool endpoints"))]);

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().ContainSingle();
    }

    // AAA: DEGRADED без маркера (обычный lag) → алерта нет.
    [Fact]
    public void WalDegradedWithoutMarker_NoAlert()
    {
        // Arrange
        var snapshot = Snapshot([Cluster(wal: new WalStreamInfo(
            "demo", "s1", WalStreamInfoState.Degraded, "slot", "n1", 1760000000,
            5, "WAL stream lagging by 5 segments"))]);

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: чистые статусы → алерта нет (гашение).
    [Fact]
    public void CleanStatuses_NoAlert()
    {
        // Arrange
        var snapshot = Snapshot([Cluster(
            wal: new WalStreamInfo("demo", "s1", WalStreamInfoState.Active, "slot", "n1", 1760000000, null, null))]);

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }
}
