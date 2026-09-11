using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-deleting-stuck (t06, spec §3.6): свежий DELETING — молчим;
// застарелый (по finished_unix либо по started_unix — fallback) — warning;
// из нескольких — старейший.
public class BackupDeletingStuckRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 10, 12, 0, 0, TimeSpan.Zero);

    private static readonly IOptions<AlertsOptions> DefaultOptions = Options.Create(new AlertsOptions());

    private static IReadOnlyList<Alert> Evaluate(
        EtcdSnapshot snapshot, IOptions<AlertsOptions>? options = null)
        => [.. new BackupDeletingStuckRule(options ?? DefaultOptions)
            .Evaluate(snapshot, new AlertContext(null, Now, 3))];

    private static ClusterBackupsInfo Cluster(string name, string shard, params DeletingFullInfo[] fulls)
        => new(name, null,
            new Dictionary<string, long?>(),
            DeletingFulls: new Dictionary<string, IReadOnlyList<DeletingFullInfo>>
            {
                [shard] = fulls,
            });

    // AAA: DELETING свежий (finished час назад, порог 6 ч) — пусто
    [Fact]
    public void FreshDeleting_NoAlert()
    {
        // Arrange — finished_unix = now − 3600
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Backups = [Cluster("demo", "s1",
                new DeletingFullInfo("20260910000000Z", Now.ToUnixTimeSeconds() - 7200, Now.ToUnixTimeSeconds() - 3600))],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: застарелый по finished_unix (7 ч > 6 ч) — warning с id
    [Fact]
    public void StaleByFinished_WarningAlert()
    {
        // Arrange — finished_unix = now − 7 ч
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Backups = [Cluster("demo", "s1",
                new DeletingFullInfo("20260909000000Z", Now.ToUnixTimeSeconds() - 25200 - 60, Now.ToUnixTimeSeconds() - 25200))],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().ContainSingle();
        var alert = alerts[0];
        alert.Kind.Should().Be("backup-deleting-stuck");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Target.Should().Be("demo/s1");
        alert.Message.Should().Contain("20260909000000Z");
        alert.Hint.Should().NotBeEmpty();
        alert.RemedyText.Should().NotBeEmpty();
        alert.Remedy.Should().Be(AlertRemedy.WorkerAuto);
    }

    // AAA: finished_unix нет — возраст по started_unix (fallback)
    [Fact]
    public void StaleByStartedFallback_WarningAlert()
    {
        // Arrange — finished нет, started 7 ч назад
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Backups = [Cluster("demo", "s1",
                new DeletingFullInfo("20260909050000Z", Now.ToUnixTimeSeconds() - 25200, null))],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().ContainSingle();
        alerts[0].Message.Should().Contain("20260909050000Z");
    }

    // AAA: несколько DELETING — алерт ОДИН, именно по старейшему (оба старше
    // порога: выбор определяется порядком, а не фильтром порога)
    [Fact]
    public void SeveralDeleting_OldestAlerted()
    {
        // Arrange — два застарелых: 7 ч и 20 ч (оба > порога 6 ч)
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Backups = [Cluster("demo", "s1",
                new DeletingFullInfo("old7h", Now.ToUnixTimeSeconds() - 25200, null),
                new DeletingFullInfo("oldest20h", Now.ToUnixTimeSeconds() - 72000, null))],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert — в алерт попал старейший (наибольший возраст)
        alerts.Should().ContainSingle();
        alerts[0].Message.Should().Contain("oldest20h")
            .And.NotContain("old7h");
    }

    // AAA: DELETING нет вовсе — пусто
    [Fact]
    public void NoDeleting_NoAlert()
    {
        // Arrange — снапшот без бэкапов
        var snapshot = TestSnapshots.Healthy(Now);

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }
}
