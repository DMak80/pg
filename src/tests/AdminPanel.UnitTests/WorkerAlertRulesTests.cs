using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// WorkerUnhealthyRule (spec D4в; arch/adminpanel/03 §4): живой lease, но
// /healthz ≠ Healthy — warning per-instance до истечения lease.
public class WorkerAlertRulesTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 1, 12, 0, 0, TimeSpan.Zero);

    // Оценка одного правила на снапшоте (харнесс ShardingAlertRulesTests).
    private static IReadOnlyList<Alert> Evaluate(IAlertRule rule, EtcdSnapshot snapshot)
        => [.. rule.Evaluate(snapshot, new AlertContext(null, Now, 3))];

    [Fact]
    public void WorkerUnhealthy_DegradedInstance_WarningPerInstance()
    {
        // Arrange: один инстанс Degraded при живом lease-ключе.
        var rule = new WorkerUnhealthyRule();
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            WorkerHealth = [new WorkerHealth("w1", "http://pgworker:8080", WorkerHealthStatus.Degraded,
                Now, "reconcile loop has not ticked for 120 s")],
        };

        // Act
        var alerts = Evaluate(rule, snapshot);

        // Assert: warning на конкретный инстанс, Detail в Message.
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("worker-unhealthy");
        alert.Target.Should().Be("pgworker/w1");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Message.Should().Contain("reconcile");
    }

    [Fact]
    public void WorkerUnhealthy_AllHealthy_NoAlerts()
    {
        // Arrange
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            WorkerHealth = [new WorkerHealth("w1", "http://pgworker:8080", WorkerHealthStatus.Healthy, Now, null)],
        };

        // Act + Assert
        Evaluate(new WorkerUnhealthyRule(), snapshot).Should().BeEmpty();
    }

    // AAA: инстанс EtcdWorker Degraded при живом lease — warning per-instance
    // ряда etcdworker/<id> (adminpanel/03 §4).
    [Fact]
    public void WorkerUnhealthy_EtcdWorkerDegraded_WarningPerInstance()
    {
        // Arrange: pg-инстансы здоровы, один инстанс EtcdWorker Degraded.
        var rule = new WorkerUnhealthyRule();
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            EtcdWorkerHealth = [new WorkerHealth("w2", "https://etcdworker:8080", WorkerHealthStatus.Degraded,
                Now, "etcd endpoints unreachable")],
        };

        // Act
        var alerts = Evaluate(rule, snapshot);

        // Assert: warning на etcdworker/w2, Detail в Message.
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("worker-unhealthy");
        alert.Target.Should().Be("etcdworker/w2");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Message.Should().Contain("etcd endpoints unreachable");
    }

    // AAA: нет живых ключей /etcdworker/api/ при живом PgWorker — только ряд
    // etcdworker-алерта (pg-ряд молчит, adminpanel/03 §4).
    [Fact]
    public void WorkerApiUnreachable_EtcdWorkerNoKeys_CriticalEtcdWorkerRowOnly()
    {
        // Arrange: здоровый снапшот (pg-ключ жив), ключи etcdworker пусты.
        var rule = new WorkerApiUnreachableRule();
        var snapshot = TestSnapshots.Healthy(Now) with { EtcdWorkerEndpoints = [] };

        // Act
        var alerts = Evaluate(rule, snapshot);

        // Assert: единственный critical — target etcdworker.
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("worker-api-unreachable");
        alert.Target.Should().Be("etcdworker");
        alert.Severity.Should().Be(AlertSeverity.Critical);
    }
}
