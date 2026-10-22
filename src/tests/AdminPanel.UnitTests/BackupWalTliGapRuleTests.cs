using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-wal-tli-gap (t24, spec §3.5): живое поле gap wal-ключа живого
// Active-кластера — critical (Remedy WorkerAuto, границы зазора в тексте и
// атрибутах); DEGRADED без gap — молчание (домен существующих правил);
// мёртвый кластер / шард вне кластера — молчание.
public class BackupWalTliGapRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 13, 12, 0, 0, TimeSpan.Zero);

    private static IReadOnlyList<Alert> Evaluate(EtcdSnapshot snapshot)
        => [.. new BackupWalTliGapRule().Evaluate(snapshot, new AlertContext(null, Now, 3))];

    private static WalStreamInfo Wal(WalStreamInfoState state, string? error = null,
        WalGapInfo? gap = null)
        => new("demo", "s1", state, "pgw_bkp_demo_s1", "s1a",
            Now.ToUnixTimeSeconds(), null, error, Gap: gap);

    private static ClusterBackupsInfo ClusterOf(WalStreamInfo wal)
        => new("demo", null,
            new Dictionary<string, long?>(),
            new Dictionary<string, WalStreamInfo?> { ["s1"] = wal });

    // AAA (AC5): Gap живой живого Active-кластера → один alert, critical,
    // Remedy WorkerAuto, Message содержит границы и действие воркера
    [Fact]
    public void Gap_ActiveCluster_Critical()
    {
        // Arrange — шард demo-кластера из TestSnapshots.FullCluster
        var cluster = TestSnapshots.FullCluster();
        var shardName = cluster.Shards[0].Name;
        var gap = new WalGapInfo("000000010000000000000003", "000000020000000000000007", 2);
        var wal = Wal(WalStreamInfoState.Degraded, "tli-gap: ...", gap) with { Shard = shardName };
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Backups = [new ClusterBackupsInfo("demo", null,
                new Dictionary<string, long?>(),
                new Dictionary<string, WalStreamInfo?> { [shardName] = wal })],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert — kind/critical/remedy; границы и действие в тексте
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("backup-wal-tli-gap");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Remedy.Should().Be(AlertRemedy.WorkerAuto);
        alert.Message.Should().Contain(shardName)
            .And.Contain("000000010000000000000003")
            .And.Contain("000000020000000000000007")
            .And.Contain("re-runs the full backup");
        alert.Details.Should().NotBeNull();
        alert.Details!["gap_from"].Should().Be("000000010000000000000003");
        alert.Details["gap_to"].Should().Be("000000020000000000000007");
        alert.Details["gap_tli"].Should().Be("2");
    }

    // AAA: DEGRADED без Gap — молчание (домен wal-chain-broken/лагов, t03/t07)
    [Fact]
    public void NoGap_Silent()
    {
        // Arrange
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Backups = [ClusterOf(Wal(WalStreamInfoState.Degraded, "WAL stream lagging"))],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: Gap мёртвого (не-Active) кластера — молчание
    [Fact]
    public void Gap_DeadCluster_Silent()
    {
        // Arrange — кластер ghost в снапшоте отсутствует → живого Active нет
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Backups = [new ClusterBackupsInfo("ghost", null,
                new Dictionary<string, long?>(),
                new Dictionary<string, WalStreamInfo?>
                {
                    ["s1"] = Wal(WalStreamInfoState.Degraded,
                        gap: new WalGapInfo("000000010000000000000003", "000000020000000000000007", 2)),
                })],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: шард не в кластере — молчание (снапшот бэкапов шире живого кластера)
    [Fact]
    public void Gap_ClusterWithoutShard_Silent()
    {
        // Arrange — demo Active есть, но шарда ghost-shard в нём нет
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Backups = [new ClusterBackupsInfo("demo", null,
                new Dictionary<string, long?>(),
                new Dictionary<string, WalStreamInfo?>
                {
                    ["ghost-shard"] = Wal(WalStreamInfoState.Degraded,
                        gap: new WalGapInfo("000000010000000000000003", "000000020000000000000007", 2))
                            with { Shard = "ghost-shard" },
                })],
        };
        snapshot.Clusters.Should().Contain(c => c.Name == "demo");
        snapshot.Clusters.Single(c => c.Name == "demo").Shards
            .Should().NotContain(s => s.Name == "ghost-shard");

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }
}
