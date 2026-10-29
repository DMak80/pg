using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;
using Shared.Etcd.Client;

namespace AdminPanel.UnitTests;

// Юниты : панельный парсер /pgworker/backups/ + правила алертов
// wal-chain-broken / wal-stream-lag / wal-stream-stopped (arch/19 §3/§8).
public class WalAlertRulesTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 10, 12, 0, 0, TimeSpan.Zero);

    private static AlertContext Context() => new(null, Now, 5);

    // Пороги по умолчанию (1024/300) — синхронизированы с воркерными дефолтами.
    private static IOptions<AlertsOptions> Opts()
        => Microsoft.Extensions.Options.Options.Create(new AlertsOptions());

    // Снапшот с живым Active-кластером demo (шард s1) и заданным Backups-префиксом.
    private static EtcdSnapshot SnapshotWith(ClusterBackupsInfo backups)
        => TestSnapshots.Healthy(Now) with { Backups = [backups] };

    // модель: (Cluster, FullMaxAgeSec, ShardLastCompletedUnix, Wal-словарь).
    private static ClusterBackupsInfo Backups(
        string cluster, Dictionary<string, WalStreamInfo?> wal)
        => new(cluster, null, new Dictionary<string, long?>(), wal);

    private static WalStreamInfo Wal(
        WalStreamInfoState state = WalStreamInfoState.Active,
        long? lag = 0,
        long uploadedUnix = 1757486400,
        string? error = null)
        => new("demo", "s1", state, "pgw_bkp_demo_s1", "s1a", uploadedUnix, lag, error);

    private static Kv Kv(string key, string value) => new(key, value, 1);

    // ── Парсер ──

    [Fact]
    public void Parser_ValidWalKey_Model()
    {
        // Arrange
        var kvs = new[]
        {
            Kv("/pgworker/backups/demo/s1/wal",
                """{"state":"ACTIVE","slot":"pgw_bkp_demo_s1","master_node":"s1a","chain_start_segment":"000000010000000000000001","last_received_segment":"000000010000000000000003","last_uploaded_segment":"000000010000000000000003","last_uploaded_unix":1757486400,"lag_segments":2}"""),
        };

        // Act
        var parsed = BackupsParser.Parse(kvs);

        // Assert
        parsed.Errors.Should().BeEmpty();
        var cluster = parsed.Clusters.Should().ContainSingle().Subject;
        cluster.Cluster.Should().Be("demo");
        var wal = cluster.Shards!["s1"]!;
        wal.State.Should().Be(WalStreamInfoState.Active);
        wal.LagSegments.Should().Be(2);
    }

    [Fact]
    public void Parser_BrokenJson_ErrorsWithoutException()
    {
        // Arrange — не-JSON и не-объект значения
        var kvs = new[]
        {
            Kv("/pgworker/backups/demo/s1/wal", "{broken"),
            Kv("/pgworker/backups/demo/s2/wal", "[]"),
        };

        // Act
        var parsed = BackupsParser.Parse(kvs);

        // Assert — толерантность: тик не роняется, ошибки видны
        parsed.Errors.Should().HaveCount(2);
        parsed.Clusters.Should().BeEmpty();
    }

    [Fact]
    public void Parser_FullAndPolicyWithoutWal_DoNotBreakWalDictionary()
    {
        // Arrange — full/policy разбираются семантикой (шард в словаре с null);
        // wal-словарь шарда при отсутствии wal-ключа пуст → wal-правила молчат
        var kvs = new[]
        {
            Kv("/pgworker/backups/demo/s1/full/20260910120000Z", """{"state":"COMPLETED"}"""),
            Kv("/pgworker/backups/demo/policy", """{"retention_days":7}"""),
        };

        // Act
        var parsed = BackupsParser.Parse(kvs);

        // Assert — семантика: шард зарегистрирован; wal-инфы нет (null) —
        // наши wal-правила по null не срабатывают
        parsed.Errors.Should().BeEmpty();
        var cluster = parsed.Clusters.Should().ContainSingle().Subject;
        cluster.Shards.Should().NotContainKey("s1");
        cluster.ShardLastCompletedUnix.Should().ContainKey("s1");
    }

    // ── wal-chain-broken ──

    [Fact]
    public void ChainBroken_Degraded_ActiveCluster_Critical()
    {
        // Arrange — DEGRADED-поток у шарда живого Active-кластера
        var snapshot = SnapshotWith(Backups("demo",
            new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal(WalStreamInfoState.Degraded, error: "WAL chain hole: expected ..02, found ..03") }));

        // Act
        var alerts = new WalChainBrokenRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Id.Should().Be("wal-chain-broken:demo/s1");
        alert.Message.Should().Contain("WAL chain hole");
    }

    [Fact]
    public void ChainBroken_Active_NoAlert()
    {
        // Arrange
        var snapshot = SnapshotWith(Backups("demo",
            new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal() }));

        // Act / Assert
        new WalChainBrokenRule().Evaluate(snapshot, Context()).Should().BeEmpty();
    }

    // ── wal-stream-lag ──

    [Fact]
    public void StreamLag_LagAboveThreshold_Warning()
    {
        // Arrange — ACTIVE с лагом 5000 (порог по умолчанию 1024)
        var snapshot = SnapshotWith(Backups("demo",
            new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal(lag: 5000) }));

        // Act
        var alerts = new WalStreamLagRule(Opts()).Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Kind.Should().Be("wal-stream-lag");
        alert.Message.Should().Contain("lags by");
    }

    [Fact]
    public void StreamLag_FreshAndSmallLag_NoAlert()
    {
        // Arrange — свежая загрузка, лаг 0
        var snapshot = SnapshotWith(Backups("demo",
            new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal(lag: 0, uploadedUnix: Now.ToUnixTimeSeconds()) }));

        // Act / Assert
        new WalStreamLagRule(Opts()).Evaluate(snapshot, Context()).Should().BeEmpty();
    }

    [Fact]
    public void StreamLag_SilenceLongerThanThreshold_Warning()
    {
        // Arrange — last_uploaded = now − 3600 (порог 300)
        var snapshot = SnapshotWith(Backups("demo",
            new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal(lag: 0, uploadedUnix: Now.ToUnixTimeSeconds() - 3600) }));

        // Act
        var alerts = new WalStreamLagRule(Opts()).Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Message.Should().Contain("silent");
    }

    // ── wal-stream-stopped ──

    [Fact]
    public void StreamStopped_Stopped_LiveShard_Warning()
    {
        // Arrange
        var snapshot = SnapshotWith(Backups("demo",
            new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal(WalStreamInfoState.Stopped, lag: null) }));

        // Act
        var alerts = new WalStreamStoppedRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Kind.Should().Be("wal-stream-stopped");
    }

    [Fact]
    public void StreamStopped_NonexistentShard_NoAlert()
    {
        // Arrange — ключ wal от шарда, которого в кластере нет
        var snapshot = SnapshotWith(Backups("demo",
            new Dictionary<string, WalStreamInfo?> { ["gone"] = Wal(WalStreamInfoState.Stopped, lag: null) }));

        // Act / Assert
        new WalStreamStoppedRule().Evaluate(snapshot, Context()).Should().BeEmpty();
    }
}
