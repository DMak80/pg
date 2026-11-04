using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Shared.Etcd.Client;
using Xunit;

namespace AdminPanel.UnitTests;

// t15: счётчик жизни цепочки wal-life — панельный парсер + правило
// backup-wal-life-changed (свежесть bumped_unix, severity по причине,
// гашение возрастом, молчание без ключа/для не-Active-кластера).
public class BackupWalLifeChangedRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 10, 12, 0, 0, TimeSpan.Zero);

    private static AlertContext Context() => new(null, Now, 5);

    private static IOptions<AlertsOptions> Opts()
        => Microsoft.Extensions.Options.Options.Create(new AlertsOptions());

    private static EtcdSnapshot SnapshotWith(ClusterBackupsInfo backups)
        => TestSnapshots.Healthy(Now) with { Backups = [backups] };

    private static ClusterBackupsInfo Backups(WalLifeInfo life)
        => new("demo", null, new Dictionary<string, long?>(),
            WalLife: new Dictionary<string, WalLifeInfo> { ["s1"] = life });

    private static long NowUnix => Now.ToUnixTimeSeconds();

    private static Kv Kv(string key, string value) => new(key, value, 1);

    // ── парсер ──

    // AAA: валидный ключ wal-life — модель с номером/временем/причиной.
    [Fact]
    public void Parser_WalLifeKey_Model()
    {
        // Arrange
        var kvs = new[]
        {
            Kv("/pgworker/backups/demo/s1/wal-life",
                """{"chain_life":3,"bumped_unix":1757486400,"reason":"restore"}"""),
        };

        // Act
        var parsed = BackupsParser.Parse(kvs);

        // Assert
        parsed.Errors.Should().BeEmpty();
        var life = parsed.Clusters.Should().ContainSingle().Subject.WalLife!["s1"];
        life.ChainLife.Should().Be(3);
        life.BumpedUnix.Should().Be(1757486400);
        life.Reason.Should().Be("restore");
    }

    // AAA: битый JSON — KeyParseError, кластер не падает (толерантный читатель).
    [Fact]
    public void Parser_BrokenWalLifeKey_ErrorWithoutException()
    {
        // Arrange
        var kvs = new[] { Kv("/pgworker/backups/demo/s1/wal-life", "{broken") };

        // Act
        var parsed = BackupsParser.Parse(kvs);

        // Assert
        parsed.Errors.Should().ContainSingle();
        parsed.Clusters.Should().BeEmpty();
    }

    // ── правило ──

    // AAA: свежая смена жизни по restore — info (штатная смена).
    [Fact]
    public void Rule_RestoreFresh_Info()
    {
        // Arrange
        var life = new WalLifeInfo("demo", "s1", 2, NowUnix - 60, "restore");

        // Act
        var alerts = new BackupWalLifeChangedRule(Opts())
            .Evaluate(SnapshotWith(Backups(life)), Context()).ToList();

        // Assert — один info-алерт с номером жизни в тексте
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Info);
        alert.Kind.Should().Be(BackupWalLifeChangedRule.KindName);
        alert.Id.Should().Be("backup-wal-life-changed:demo/s1");
        alert.Message.Should().Contain("life 2").And.Contain("restore");
    }

    // AAA: foreign-chain — warning (чужая цепочка, приёмник заморожен).
    [Fact]
    public void Rule_ForeignChain_Warning()
    {
        // Arrange
        var life = new WalLifeInfo("demo", "s1", 5, NowUnix - 30, "foreign-chain");

        // Act
        var alerts = new BackupWalLifeChangedRule(Opts())
            .Evaluate(SnapshotWith(Backups(life)), Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Message.Should().Contain("foreign WAL chain");
    }

    // AAA: событие гаснет возрастом (bumped_unix старше окна).
    [Fact]
    public void Rule_OldBump_NoAlert()
    {
        // Arrange — инкремент был 2 часа назад, окно 3600 с
        var life = new WalLifeInfo("demo", "s1", 3, NowUnix - 7200, "restore");

        // Act
        var alerts = new BackupWalLifeChangedRule(Opts())
            .Evaluate(SnapshotWith(Backups(life)), Context()).ToList();

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: без bumped_unix (старый/битый ключ) — события нет.
    [Fact]
    public void Rule_NoBumpedUnix_NoAlert()
    {
        // Arrange
        var life = new WalLifeInfo("demo", "s1", 3, null, null);

        // Act
        var alerts = new BackupWalLifeChangedRule(Opts())
            .Evaluate(SnapshotWith(Backups(life)), Context()).ToList();

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: без ключа wal-life — правило молчит.
    [Fact]
    public void Rule_NoKey_NoAlert()
    {
        // Arrange — WalLife отсутствует
        var backups = new ClusterBackupsInfo("demo", null, new Dictionary<string, long?>());

        // Act
        var alerts = new BackupWalLifeChangedRule(Opts())
            .Evaluate(SnapshotWith(backups), Context()).ToList();

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: не-Active кластер — правило молчит (образец BackupWalTliGapRule).
    [Fact]
    public void Rule_NotInitializedCluster_NoAlert()
    {
        // Arrange — снапшот без живого Active-кластера demo
        var life = new WalLifeInfo("demo", "s1", 3, NowUnix - 30, "restore");
        var snapshot = TestSnapshots.Healthy(Now) with { Clusters = [], Backups = [Backups(life)] };

        // Act
        var alerts = new BackupWalLifeChangedRule(Opts())
            .Evaluate(snapshot, Context()).ToList();

        // Assert
        alerts.Should().BeEmpty();
    }
}
