using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Каркас AlertEngine: сбор правил, сортировка, механика sinceUnix (spec §4.1, §3.4).
public class AlertEngineTests
{
    private static readonly DateTimeOffset BuiltAt = new(2026, 8, 22, 12, 0, 0, TimeSpan.Zero);

    // Фейк-правило: всегда возвращает заданный алерт (каркас-тесты без реальных правил).
    private sealed class ConstRule(string kind, Alert alert) : IAlertRule
    {
        public string Kind => kind;

        public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context) => [alert];
    }

    private static Alert Make(
        string id, AlertSeverity severity, string kind, string target, long? sinceUnix = null)
        => new(id, severity, kind, target, "message", null, sinceUnix);

    [Fact]
    public void Evaluate_NoRules_EmptyList()
    {
        // Arrange
        var engine = new AlertEngine([]);

        // Act
        var alerts = engine.Evaluate(TestSnapshots.Healthy(BuiltAt), null, BuiltAt, 3);

        // Assert
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void Evaluate_CollectsAllRules()
    {
        // Arrange
        var engine = new AlertEngine(
        [
            new ConstRule("kind-a", Make("kind-a:t", AlertSeverity.Warning, "kind-a", "t")),
            new ConstRule("kind-b", Make("kind-b:t", AlertSeverity.Critical, "kind-b", "t")),
        ]);

        // Act
        var alerts = engine.Evaluate(TestSnapshots.Healthy(BuiltAt), null, BuiltAt, 3);

        // Assert
        alerts.Should().HaveCount(2);
    }

    [Fact]
    public void Evaluate_Sorts_SeverityDescThenKindThenTarget()
    {
        // Arrange: critical всегда первый; внутри уровня — kind, затем target (Ordinal).
        var engine = new AlertEngine(
        [
            new ConstRule("k1", Make("k1:x", AlertSeverity.Warning, "k1", "x")),
            new ConstRule("k2", Make("k2:z", AlertSeverity.Critical, "k2", "z")),
            new ConstRule("k3", Make("k3:y", AlertSeverity.Warning, "k3", "y")),
            new ConstRule("k4", Make("k4:x", AlertSeverity.Warning, "k4", "x")),
        ]);

        // Act
        var alerts = engine.Evaluate(TestSnapshots.Healthy(BuiltAt), null, BuiltAt, 3);

        // Assert: critical первым; внутри уровня — по kind (Ordinal): k1 < k3 < k4.
        string.Join("|", alerts.Select(a => a.Id))
            .Should().Be("k2:z|k1:x|k3:y|k4:x");
    }

    [Fact]
    public void SinceUnix_CarriedFromPrevious()
    {
        // Arrange: id был в previous со since=1000 — переносится без изменений (spec §3.4).
        var engine = new AlertEngine([new ConstRule("k", Make("k:t", AlertSeverity.Warning, "k", "t"))]);
        var previous = TestSnapshots.Healthy(BuiltAt) with
        {
            Alerts = [Make("k:t", AlertSeverity.Warning, "k", "t", sinceUnix: 1000)],
        };

        // Act
        var alerts = engine.Evaluate(TestSnapshots.Healthy(BuiltAt + TimeSpan.FromSeconds(3)), previous, BuiltAt + TimeSpan.FromSeconds(3), 3);

        // Assert
        alerts.Single().SinceUnix.Should().Be(1000);
    }

    [Fact]
    public void SinceUnix_NewAlert_GetsCurrentUnix()
    {
        // Arrange: id в previous отсутствовал — since = unix времени оценки.
        var now = BuiltAt + TimeSpan.FromSeconds(5);
        var engine = new AlertEngine([new ConstRule("k", Make("k:t", AlertSeverity.Warning, "k", "t"))]);
        var previous = TestSnapshots.Healthy(BuiltAt) with { Alerts = [] };

        // Act
        var alerts = engine.Evaluate(TestSnapshots.Healthy(now), previous, now, 3);

        // Assert
        alerts.Single().SinceUnix.Should().Be(now.ToUnixTimeSeconds());
    }

    [Fact]
    public void SinceUnix_NullOnFirstTick()
    {
        // Arrange: previous нет (первый тик) — время появления неизвестно (spec §3.4).
        var engine = new AlertEngine([new ConstRule("k", Make("k:t", AlertSeverity.Warning, "k", "t"))]);

        // Act
        var alerts = engine.Evaluate(TestSnapshots.Healthy(BuiltAt), null, BuiltAt, 3);

        // Assert
        alerts.Single().SinceUnix.Should().BeNull();
    }
}
