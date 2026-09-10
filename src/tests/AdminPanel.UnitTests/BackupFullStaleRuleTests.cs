using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// backup-full-stale (t02, spec §3.5): 4 случая каталога + переопределение
// панельного дефолта policy кластера. Критичный severity — данные без свежего
// полного = риск потери восстановимости.
public class BackupFullStaleRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 10, 12, 0, 0, TimeSpan.Zero);

    private static readonly IOptions<AlertsOptions> DefaultOptions = Options.Create(new AlertsOptions());

    private static IReadOnlyList<Alert> Evaluate(IAlertRule rule, EtcdSnapshot snapshot)
        => [.. rule.Evaluate(snapshot, new AlertContext(null, Now, 3))];

    // Снапшот с заданной Backups-картой поверх здорового etcd-базиса.
    private static EtcdSnapshot Snapshot(params ClusterBackupsInfo[] backups)
        => TestSnapshots.Healthy(Now) with { Backups = [.. backups] };

    private static ClusterBackupsInfo ClusterInfo(
        string cluster, long? fullMaxAgeSec, params (string Shard, long? Last)[] shards)
        => new(cluster, fullMaxAgeSec,
            shards.ToDictionary(p => p.Shard, p => p.Last));

    // AAA: свежий COMPLETED (finished 3600 c назад при пороге 86400) — пусто
    [Fact]
    public void FreshCompleted_NoAlert()
    {
        // Arrange — finished час назад, дефолтное суточное окно
        var rule = new BackupFullStaleRule(DefaultOptions);
        var snapshot = Snapshot(ClusterInfo("demo", null, ("s1", Now.ToUnixTimeSeconds() - 3600)));

        // Act
        var alerts = Evaluate(rule, snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: протаранный COMPLETED (90000 c > 86400) — Critical, target demo/s1
    [Fact]
    public void StaleCompleted_CriticalAlert()
    {
        // Arrange
        var rule = new BackupFullStaleRule(DefaultOptions);
        var snapshot = Snapshot(ClusterInfo("demo", null, ("s1", Now.ToUnixTimeSeconds() - 90000)));

        // Act
        var alerts = Evaluate(rule, snapshot);

        // Assert — поля канона: kind/severity/target + порог в details
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("backup-full-stale");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Target.Should().Be("demo/s1");
        alert.Id.Should().Be("backup-full-stale:demo/s1");
        alert.Details!["maxAgeSeconds"].Should().Be("86400");
        alert.Message.Should().Contain("порог 86400 c");
    }

    // AAA: ключи есть, COMPLETED нет — «никогда не завершался» (never-ветка)
    [Fact]
    public void NoCompleted_NeverCompleted()
    {
        // Arrange — попытки были (RUNNING/FAILED в словаре null), но COMPLETED не было
        var rule = new BackupFullStaleRule(DefaultOptions);
        var snapshot = Snapshot(ClusterInfo("demo", null, ("s1", null)));

        // Act
        var alerts = Evaluate(rule, snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Message.Should().Contain("никогда не завершался");
        alert.Details!["lastCompletedUnix"].Should().BeEmpty();
    }

    // AAA: кластера нет в snapshot.Backups — правилу нечего сказать
    [Fact]
    public void EmptyPrefix_ClusterSilent()
    {
        // Arrange — здоровый снапшот без Backups (подсистема не включена)
        var rule = new BackupFullStaleRule(DefaultOptions);
        var snapshot = TestSnapshots.Healthy(Now);

        // Act
        var alerts = Evaluate(rule, snapshot);

        // Assert
        alerts.Should().BeEmpty();
    }

    // AAA: policy кластера переопределяет дефолт панели (60 < 100 c → алерт)
    [Fact]
    public void ClusterPolicy_OverridesPanelDefault()
    {
        // Arrange — policy 60 c; finished 100 c назад (в панельном окне 86400 бы прошёл)
        var rule = new BackupFullStaleRule(DefaultOptions);
        var snapshot = Snapshot(ClusterInfo("demo", 60, ("s1", Now.ToUnixTimeSeconds() - 100)));

        // Act
        var alerts = Evaluate(rule, snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Details!["maxAgeSeconds"].Should().Be("60");
    }
}
