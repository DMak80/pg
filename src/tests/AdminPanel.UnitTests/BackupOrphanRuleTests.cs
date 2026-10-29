using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-orphan (spec §3.5): запись реестра → warning с prefix/size и
// остатком TTL; OrphanTtlSec=0 → «удаление вручную» (OperatorRunbook);
// DELETING → «идёт удаление»; ключа нет — молчание.
public class BackupOrphanRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 13, 12, 0, 0, TimeSpan.Zero);

    private static IReadOnlyList<Alert> Evaluate(
        EtcdSnapshot snapshot, IOptions<AlertsOptions>? options = null)
    {
        var opts = options ?? Options.Create(new AlertsOptions());
        return [.. new BackupOrphanRule(opts).Evaluate(snapshot, new AlertContext(null, Now, 3))];
    }

    private static BackupOrphanInfo Entry(
        string prefix = "ghost1/s1", string kind = "cluster",
        long size = 1024 * 1024 * 1024, long firstSeen = 1757100000,
        string state = "OBSERVED", bool hasValidFull = false)
        => new(prefix, kind, size, firstSeen, state, hasValidFull);

    private static EtcdSnapshot SnapshotWith(BackupOrphansInfo? orphans)
        => TestSnapshots.Healthy(Now) with { BackupOrphans = orphans };

    // AAA (AC7→алерт): запись OBSERVED в реестре → warning с prefix, size и
    // остатком TTL («удаление по TTL через …»)
    [Fact]
    public void Orphan_Observed_Warning_WithTtl()
    {
        // Arrange — first_seen 2 суток назад (Now − 172800), ttl 7 сут → остаток ~5 сут
        var snapshot = SnapshotWith(new BackupOrphansInfo(
            [Entry(firstSeen: Now.ToUnixTimeSeconds() - 172800)], Now.ToUnixTimeSeconds()));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("backup-orphan");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Remedy.Should().Be(AlertRemedy.WorkerAuto);
        alert.Message.Should().Contain("ghost1/s1").And.Contain("GiB").And.Contain("TTL").And.Contain(" d");
    }

    // AAA (AC7): OrphanTtlSec=0 → «удаление вручную», Remedy OperatorRunbook
    [Fact]
    public void Orphan_TtlZero_Manual()
    {
        // Arrange — панельный порог 0 (воркер авто-удаление выключил)
        var options = Options.Create(new AlertsOptions
        {
            Backups = new AlertsOptions.BackupsAlertsOptions { OrphanTtlSec = 0 },
        });
        var snapshot = SnapshotWith(new BackupOrphansInfo([Entry()], 1760000000));

        // Act
        var alerts = Evaluate(snapshot, options);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Remedy.Should().Be(AlertRemedy.OperatorRunbook);
        alert.Message.Should().Contain("manual deletion");
    }

    // AAA: DELETING-запись → «идёт удаление» (даже при ttl=0 — доводка важнее)
    [Fact]
    public void Orphan_Deleting_DeletionInProgress()
    {
        // Arrange
        var snapshot = SnapshotWith(new BackupOrphansInfo(
            [Entry(state: "DELETING")], 1760000000));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Message.Should().Contain("deletion in progress");
        alert.Remedy.Should().Be(AlertRemedy.WorkerAuto);
    }

    // AAA: ключа реестра нет — правило молчит (подсистема сирот не включалась)
    [Fact]
    public void NoKey_Silent()
    {
        // Arrange / Act / Assert
        Evaluate(SnapshotWith(null)).Should().BeEmpty();
    }

    // ── DR-hold (reliability): fate-приоритет DELETING → hold → полный →
    // заявка → TTL — AAA ——

    // AAA (AC7): OBSERVED+hold → «защищён hold-флагом (<by>) — удаление только
    // явной командой», Remedy OperatorRunbook
    [Fact]
    public void Orphan_Hold_ProtectedPhrase_AndRunbook()
    {
        // Arrange — сирота под hold панелью (вне TTL-отбора воркера)
        var snapshot = SnapshotWith(new BackupOrphansInfo(
            [Entry(firstSeen: Now.ToUnixTimeSeconds() - 172800)],
            Now.ToUnixTimeSeconds(),
            Holds: new Dictionary<string, OrphanHoldInfo>
            {
                ["ghost1/s1"] = new("ghost1/s1", Now.ToUnixTimeSeconds() - 60, "panel"),
            }));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Remedy.Should().Be(AlertRemedy.OperatorRunbook);
        alert.Message.Should().Contain("protected by hold flag (panel)")
            .And.Contain("only via explicit command");
        alert.Details!["heldBy"].Should().Be("panel");
    }

    // AAA (AC7): OBSERVED+has_valid_full → «защищён автоправилом (есть валидный
    // полный) — удаление только явной командой», Remedy OperatorRunbook
    [Fact]
    public void Orphan_FullBackup_AutoRulePhrase()
    {
        // Arrange — сирота с валидным полным (автоправило последнего полного)
        var snapshot = SnapshotWith(new BackupOrphansInfo(
            [Entry(firstSeen: Now.ToUnixTimeSeconds() - 172800, hasValidFull: true)],
            Now.ToUnixTimeSeconds()));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Remedy.Should().Be(AlertRemedy.OperatorRunbook);
        alert.Message.Should().Contain("protected by auto-rule (valid full backup exists)")
            .And.Contain("only via explicit command");
        alert.Details!["hasValidFull"].Should().Be("true");
    }

    // AAA (AC7): OBSERVED+заявка delete → «к удалению заявкой оператора —
    // ближайший проход воркера», Remedy WorkerAuto
    [Fact]
    public void Orphan_DeleteRequest_QueuedForDeletion()
    {
        // Arrange — заявка явного удаления на незащищённую сироту
        var snapshot = SnapshotWith(new BackupOrphansInfo(
            [Entry(firstSeen: Now.ToUnixTimeSeconds() - 172800)],
            Now.ToUnixTimeSeconds(),
            DeleteRequests: new Dictionary<string, OrphanDeleteRequestInfo>
            {
                ["ghost1/s1"] = new("ghost1/s1", Now.ToUnixTimeSeconds() - 30, "operator"),
            }));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Remedy.Should().Be(AlertRemedy.WorkerAuto);
        alert.Message.Should().Contain("queued for deletion by operator request")
            .And.Contain("next worker pass");
        alert.Details!["deleteRequested"].Should().Be("true");
    }
}
