using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;
using AdminPanel.Etcd.Client;

namespace AdminPanel.UnitTests;

// Юниты t03: панельный парсер /pgworker/backups/ + правила алертов
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

    private static WalStreamInfo Wal(
        WalStreamInfoState state = WalStreamInfoState.Active,
        long? lag = 0,
        long uploadedUnix = 1757486400,
        string? error = null)
        => new("demo", "s1", state, "pgw_bkp_demo_s1", "s1a", uploadedUnix, lag, error);

    private static Kv Kv(string key, string value) => new(key, value, 1);

    // ── Парсер ──

    [Fact]
    public void Парсер_валидный_wal_ключ_модель()
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
        cluster.Shards["s1"]!.State.Should().Be(WalStreamInfoState.Active);
        cluster.Shards["s1"]!.LagSegments.Should().Be(2);
    }

    [Fact]
    public void Парсер_битый_json_errors_без_исключения()
    {
        // Arrange — не-JSON и не-объект значения
        var kvs = new[]
        {
            Kv("/pgworker/backups/demo/s1/wal", "{битый"),
            Kv("/pgworker/backups/demo/s2/wal", "[]"),
        };

        // Act
        var parsed = BackupsParser.Parse(kvs);

        // Assert — толерантность: тик не роняется, ошибки видны
        parsed.Errors.Should().HaveCount(2);
        parsed.Clusters.Should().BeEmpty();
    }

    [Fact]
    public void Парсер_full_ключи_игнор()
    {
        // Arrange — ключи не-wal сегментов не разбираются вовсе
        var kvs = new[]
        {
            Kv("/pgworker/backups/demo/s1/full/20260910120000Z", """{"state":"COMPLETED"}"""),
            Kv("/pgworker/backups/demo/policy", """{"retention_days":7}"""),
        };

        // Act / Assert
        var parsed = BackupsParser.Parse(kvs);
        parsed.Clusters.Should().BeEmpty();
        parsed.Errors.Should().BeEmpty();
    }

    // ── wal-chain-broken ──

    [Fact]
    public void ChainBroken_DEGRADED_живой_кластер_critical()
    {
        // Arrange — DEGRADED-поток у шарда живого Active-кластера
        var snapshot = SnapshotWith(new ClusterBackupsInfo("demo",
            new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal(WalStreamInfoState.Degraded, error: "дыра WAL-цепочки: ожидался ..02, найден ..03") }));

        // Act
        var alerts = new WalChainBrokenRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Id.Should().Be("wal-chain-broken:demo/s1");
        alert.Message.Should().Contain("дыра WAL-цепочки");
    }

    [Fact]
    public void ChainBroken_ACTIVE_нет_алерта()
    {
        // Arrange
        var snapshot = SnapshotWith(new ClusterBackupsInfo("demo",
            new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal() }));

        // Act / Assert
        new WalChainBrokenRule().Evaluate(snapshot, Context()).Should().BeEmpty();
    }

    // ── wal-stream-lag ──

    [Fact]
    public void StreamLag_лаг_выше_порога_warning()
    {
        // Arrange — ACTIVE с лагом 5000 (порог по умолчанию 1024)
        var snapshot = SnapshotWith(new ClusterBackupsInfo("demo",
            new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal(lag: 5000) }));

        // Act
        var alerts = new WalStreamLagRule(Opts()).Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Kind.Should().Be("wal-stream-lag");
        alert.Message.Should().Contain("отстаёт");
    }

    [Fact]
    public void StreamLag_свежий_и_малый_лаг_нет_алерта()
    {
        // Arrange — свежая загрузка, лаг 0
        var snapshot = SnapshotWith(new ClusterBackupsInfo("demo",
            new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal(lag: 0, uploadedUnix: Now.ToUnixTimeSeconds()) }));

        // Act / Assert
        new WalStreamLagRule(Opts()).Evaluate(snapshot, Context()).Should().BeEmpty();
    }

    [Fact]
    public void StreamLag_тишина_дольше_порога_warning()
    {
        // Arrange — last_uploaded = now − 3600 (порог 300)
        var snapshot = SnapshotWith(new ClusterBackupsInfo("demo",
            new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal(lag: 0, uploadedUnix: Now.ToUnixTimeSeconds() - 3600) }));

        // Act
        var alerts = new WalStreamLagRule(Opts()).Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Message.Should().Contain("молчит");
    }

    // ── wal-stream-stopped ──

    [Fact]
    public void StreamStopped_STOPPED_живой_шард_warning()
    {
        // Arrange
        var snapshot = SnapshotWith(new ClusterBackupsInfo("demo",
            new Dictionary<string, WalStreamInfo?> { ["s1"] = Wal(WalStreamInfoState.Stopped, lag: null) }));

        // Act
        var alerts = new WalStreamStoppedRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Kind.Should().Be("wal-stream-stopped");
    }

    [Fact]
    public void StreamStopped_несуществующий_шард_нет_алерта()
    {
        // Arrange — ключ wal от шарда, которого в кластере нет
        var snapshot = SnapshotWith(new ClusterBackupsInfo("demo",
            new Dictionary<string, WalStreamInfo?> { ["gone"] = Wal(WalStreamInfoState.Stopped, lag: null) }));

        // Act / Assert
        new WalStreamStoppedRule().Evaluate(snapshot, Context()).Should().BeEmpty();
    }
}
