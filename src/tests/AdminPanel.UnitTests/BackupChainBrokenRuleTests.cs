using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-chain-broken (t07, spec §3.5): wal=BROKEN живого Active-кластера —
// critical с текстом сбоя И действия воркера; DEGRADED/STOPPED/BROKEN мёртвого
// кластера — молчание (DEGRADED — домен wal-chain-broken t03).
public class BackupChainBrokenRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 13, 12, 0, 0, TimeSpan.Zero);

    private static IReadOnlyList<Alert> Evaluate(EtcdSnapshot snapshot)
        => [.. new BackupChainBrokenRule().Evaluate(snapshot, new AlertContext(null, Now, 3))];

    private static WalStreamInfo Wal(WalStreamInfoState state, string? error = null)
        => new("demo", "s1", state, "pgw_bkp_demo_s1", "s1a",
            Now.ToUnixTimeSeconds(), null, error);

    private static ClusterBackupsInfo ClusterOf(WalStreamInfo wal)
        => new("demo", null,
            new Dictionary<string, long?>(),
            new Dictionary<string, WalStreamInfo?> { ["s1"] = wal });

    private static EtcdSnapshot SnapshotWith(ClusterBackupsInfo backups)
        => TestSnapshots.Healthy(Now) with { Backups = [backups] };

    // AAA (AC8→алерт): wal=BROKEN живого Active-кластера → critical backup-chain-broken
    // с текстом сбоя И действия воркера
    [Fact]
    public void Broken_ЖивойКластер_Critical()
    {
        // Arrange — кластер demo Active из TestSnapshots.Healthy; шарда s1 нет в
        // кластере → нужно имя шарда из кластера: берём демо-шард FullCluster()
        var cluster = TestSnapshots.FullCluster();
        var shardName = cluster.Shards[0].Name;
        var wal = Wal(WalStreamInfoState.Broken, "дыра WAL-цепочки: ожидался X") with { Shard = shardName };
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Backups = [new ClusterBackupsInfo("demo", null,
                new Dictionary<string, long?>(),
                new Dictionary<string, WalStreamInfo?> { [shardName] = wal })],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert — kind/critical/remedy; текст содержит сбой и действие
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("backup-chain-broken");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Remedy.Should().Be(AlertRemedy.WorkerAuto);
        alert.Message.Should().Contain(shardName).And.Contain("дыра").And.Contain("переснимает");
    }

    // AAA: DEGRADED — не правило t07 (домен wal-chain-broken t03); молчание
    [Fact]
    public void Degraded_Молчание()
    {
        // Arrange
        var snapshot = SnapshotWith(ClusterOf(Wal(WalStreamInfoState.Degraded, "отставание WAL-потока")));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: BROKEN мёртвого (не-Active) кластера — молчание
    [Fact]
    public void Broken_МёртвыйКластер_Молчание()
    {
        // Arrange — кластер ghost в снапшоте отсутствует → живого Active нет
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Backups = [new ClusterBackupsInfo("ghost", null,
                new Dictionary<string, long?>(),
                new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal(WalStreamInfoState.Broken) })],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }
}
