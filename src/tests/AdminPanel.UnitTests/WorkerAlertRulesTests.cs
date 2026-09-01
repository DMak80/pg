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
                Now, "цикл reconcile не тикал 120 с")],
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
}
