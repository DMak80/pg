using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-orphan (t07, spec §3.5): запись реестра → warning с prefix/size и
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
        string state = "OBSERVED")
        => new(prefix, kind, size, firstSeen, state);

    private static EtcdSnapshot SnapshotWith(BackupOrphansInfo? orphans)
        => TestSnapshots.Healthy(Now) with { BackupOrphans = orphans };

    // AAA (AC7→алерт): запись OBSERVED в реестре → warning с prefix, size и
    // остатком TTL («удаление по TTL через …»)
    [Fact]
    public void Orphan_Observed_Warning_СTtl()
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
        alert.Message.Should().Contain("ghost1/s1").And.Contain("ГиБ").And.Contain("TTL").And.Contain("сут");
    }

    // AAA (AC7): OrphanTtlSec=0 → «удаление вручную», Remedy OperatorRunbook
    [Fact]
    public void Orphan_TtlZero_Ручное()
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
        alert.Message.Should().Contain("вручную");
    }

    // AAA: DELETING-запись → «идёт удаление» (даже при ttl=0 — доводка важнее)
    [Fact]
    public void Orphan_Deleting_ИдётУдаление()
    {
        // Arrange
        var snapshot = SnapshotWith(new BackupOrphansInfo(
            [Entry(state: "DELETING")], 1760000000));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Message.Should().Contain("идёт удаление");
        alert.Remedy.Should().Be(AlertRemedy.WorkerAuto);
    }

    // AAA: ключа реестра нет — правило молчит (подсистема сирот не включалась)
    [Fact]
    public void Нет_Ключа_Молчание()
    {
        // Arrange / Act / Assert
        Evaluate(SnapshotWith(null)).Should().BeEmpty();
    }
}
