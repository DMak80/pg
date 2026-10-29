using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-chain-broken (spec §3.5): wal=WAL_CHAIN_ERROR живого Active-кластера —
// critical с текстом сбоя И действия воркера; DEGRADED/STOPPED/WAL_CHAIN_ERROR мёртвого
// кластера — молчание (DEGRADED — домен wal-chain-broken).
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

    // AAA (AC8→алерт): wal=WAL_CHAIN_ERROR живого Active-кластера → critical backup-chain-broken
    // с текстом сбоя И действия воркера
    [Fact]
    public void Broken_ActiveCluster_Critical()
    {
        // Arrange — кластер demo Active из TestSnapshots.Healthy; шарда s1 нет в
        // кластере → нужно имя шарда из кластера: берём демо-шард FullCluster()
        var cluster = TestSnapshots.FullCluster();
        var shardName = cluster.Shards[0].Name;
        var wal = Wal(WalStreamInfoState.WalChainError, "WAL chain hole: expected X") with { Shard = shardName };
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
        alert.Message.Should().Contain(shardName).And.Contain("hole").And.Contain("re-runs the full backup");
    }

    // AAA: DEGRADED — не правило (домен wal-chain-broken); молчание
    [Fact]
    public void Degraded_Silent()
    {
        // Arrange
        var snapshot = SnapshotWith(ClusterOf(Wal(WalStreamInfoState.Degraded, "WAL stream lagging")));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: WAL_CHAIN_ERROR мёртвого (не-Active) кластера — молчание
    [Fact]
    public void Broken_DeadCluster_Silent()
    {
        // Arrange — кластер ghost в снапшоте отсутствует → живого Active нет
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Backups = [new ClusterBackupsInfo("ghost", null,
                new Dictionary<string, long?>(),
                new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal(WalStreamInfoState.WalChainError) })],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }
}
