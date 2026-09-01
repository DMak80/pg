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
        => new(id, severity, kind, target, "message", null, sinceUnix, "тестовый hint", AlertRemedy.WorkerAuto, "тестовое действие");

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

    private static IReadOnlyList<Alert> EvaluateAll(EtcdSnapshot snapshot, EtcdSnapshot? previous = null, DateTimeOffset? nowUtc = null)
        => new AlertEngine(AlertTestRules.All())
            .Evaluate(snapshot, previous, nowUtc ?? BuiltAt, 3);

    [Fact]
    public void Evaluate_HealthySnapshot_NoAlerts()
    {
        // Arrange / Act
        var alerts = EvaluateAll(TestSnapshots.Healthy(BuiltAt));

        // Assert: здоровая система — пустой список.
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void RuleKinds_AllUnique()
    {
        // Arrange / Act
        var kinds = AlertTestRules.All().Select(r => r.Kind).ToList();

        // Assert: защита каркаса от copy-paste новых правил t05/t06 (spec §10.1):
        // 15 (t04+t05) + 9 HA-правил t06 + cluster-not-initialized +
        // worker-api-unreachable (task etcd-via-worker-api, arch/03 §4.1) +
        // provision-stuck (spec D3) + worker-unhealthy (spec D4, arch/03 §4) — каталог 03 §4.
        kinds.Should().HaveCount(28).And.OnlyHaveUniqueItems();
    }

    [Fact]
    public void Unreachable_AtThresholdTwo_Critical()
    {
        // Arrange
        var one = TestSnapshots.Healthy(BuiltAt) with
        {
            Etcd = TestSnapshots.HealthyEtcd(BuiltAt) with { ConsecutiveFailures = 1 },
        };
        var two = TestSnapshots.Healthy(BuiltAt) with
        {
            Etcd = TestSnapshots.HealthyEtcd(BuiltAt) with { ConsecutiveFailures = 2 },
        };

        // Act
        var below = EvaluateAll(one);
        var atThreshold = EvaluateAll(two);

        // Assert: порог каталога — 2 тика (arch/03 §4).
        below.Should().BeEmpty();
        var alert = atThreshold.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Id.Should().Be("etcd-unreachable:etcd");
        alert.Details!["consecutiveFailures"].Should().Be("2");
    }

    [Fact]
    public void NoQuorum_WhenSuspected_CriticalWithErrors()
    {
        // Arrange: мёртвый endpoint даёт errors для details.
        var snapshot = TestSnapshots.Healthy(BuiltAt) with
        {
            Etcd = TestSnapshots.HealthyEtcd(BuiltAt, alive: 2, total: 3) with { QuorumSuspected = true },
        };

        // Act
        var alerts = EvaluateAll(snapshot);

        // Assert: quorum-алерт critical + один endpoint-down (мёртвый); ошибки склеены.
        var quorum = alerts.Single(a => a.Kind == "etcd-no-quorum");
        quorum.Severity.Should().Be(AlertSeverity.Critical);
        quorum.Id.Should().Be("etcd-no-quorum:etcd");
        quorum.Details!["errors"].Should().Contain("connection refused");
    }

    [Fact]
    public void EndpointDown_PerFailedEndpoint_Warning()
    {
        // Arrange: 2 из 3 упали.
        var snapshot = TestSnapshots.Healthy(BuiltAt) with
        {
            Etcd = TestSnapshots.HealthyEtcd(BuiltAt, alive: 1, total: 3),
        };

        // Act
        var alerts = EvaluateAll(snapshot);

        // Assert: по одному алерту на endpoint, target = URL.
        var down = alerts.Where(a => a.Kind == "etcd-endpoint-down").ToList();
        down.Should().HaveCount(2);
        down.Should().OnlyContain(a => a.Severity == AlertSeverity.Warning
            && a.Target.StartsWith("http://etcd", StringComparison.Ordinal)
            && a.Message.Contains(a.Target, StringComparison.Ordinal));
    }

    [Fact]
    public void EndpointDown_AllAlive_NoAlert()
    {
        // Arrange / Act
        var alerts = EvaluateAll(TestSnapshots.Healthy(BuiltAt));

        // Assert
        alerts.Should().NotContain(a => a.Kind == "etcd-endpoint-down");
    }

    [Fact]
    public void Alarm_PerAlarm_CriticalWithMemberIdType()
    {
        // Arrange: NOSPACE и CORRUPT — два alarm'а.
        var snapshot = TestSnapshots.Healthy(BuiltAt) with
        {
            Etcd = TestSnapshots.HealthyEtcd(BuiltAt) with
            {
                Alarms = [new EtcdAlarm(42, EtcdAlarmType.NoSpace), new EtcdAlarm(43, EtcdAlarmType.Corrupt)],
            },
        };

        // Act
        var alerts = EvaluateAll(snapshot);

        // Assert: target "{memberId}:{type}" (spec §3.7).
        var alarms = alerts.Where(a => a.Kind == "etcd-alarm").ToList();
        alarms.Should().HaveCount(2).And.OnlyContain(a => a.Severity == AlertSeverity.Critical);
        alarms.Should().Contain(a => a.Target == "42:nospace" && a.Details!["alarmType"] == "nospace");
        alarms.Should().Contain(a => a.Target == "43:corrupt" && a.Details!["memberId"] == "43");
    }

    [Fact]
    public void SnapshotStale_AfterThreeIntervals_Warning()
    {
        // Arrange: порог 3×3 c = 9 c (arch/03 §4).
        var snapshot = TestSnapshots.Healthy(BuiltAt);

        // Act
        var fresh = EvaluateAll(snapshot, nowUtc: BuiltAt + TimeSpan.FromSeconds(6));
        var stale = EvaluateAll(snapshot, nowUtc: BuiltAt + TimeSpan.FromSeconds(10));

        // Assert
        fresh.Should().NotContain(a => a.Kind == "snapshot-stale");
        var alert = stale.Single(a => a.Kind == "snapshot-stale");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Details!["ageSeconds"].Should().Be("10");
        alert.Details!["thresholdSeconds"].Should().Be("9");
    }

    [Fact]
    public void ClusterIncomplete_OnlyIncompleteClusters()
    {
        // Arrange: полный demo + ghost без config.
        var snapshot = TestSnapshots.Healthy(BuiltAt) with
        {
            Clusters = [TestSnapshots.FullCluster(), TestSnapshots.GhostCluster()],
        };

        // Act
        var alerts = EvaluateAll(snapshot);

        // Assert
        var alert = alerts.Single(a => a.Kind == "cluster-incomplete");
        alert.Target.Should().Be("ghost");
        alert.Details!["dbname"].Should().Be("missing");
    }

    [Fact]
    public void KeyMalformed_PerParseError()
    {
        // Arrange
        var snapshot = TestSnapshots.Healthy(BuiltAt) with
        {
            ParseErrors =
            [
                new KeyParseError("/clusters/demo/config", "битый JSON"),
                new KeyParseError("/clusters/demo/shards/s1/replicas", "не целое"),
            ],
        };

        // Act
        var alerts = EvaluateAll(snapshot);

        // Assert: по одному алерту на запись, target = ключ.
        var malformed = alerts.Where(a => a.Kind == "key-malformed").ToList();
        malformed.Should().HaveCount(2);
        malformed.Should().Contain(a => a.Target == "/clusters/demo/config" && a.Details!["reason"] == "битый JSON");
    }

    [Fact]
    public void SinceUnix_DisappearedAlert_NotResurrected()
    {
        // Arrange: previous содержал key-malformed, новый снапшот — без ошибок парсинга.
        var previous = TestSnapshots.Healthy(BuiltAt) with
        {
            ParseErrors = [new KeyParseError("/clusters/demo/config", "битый JSON")],
            Alerts = [new Alert("key-malformed:/clusters/demo/config", AlertSeverity.Warning,
                "key-malformed", "/clusters/demo/config", "ключ не разобран", null, 1000, "тестовый hint", AlertRemedy.WorkerAuto, "тестовое действие")],
        };

        // Act
        var alerts = EvaluateAll(TestSnapshots.Healthy(BuiltAt + TimeSpan.FromSeconds(3)), previous);

        // Assert: истории нет — исчезнувший алерт не возвращается (spec §3.4).
        alerts.Should().NotContain(a => a.Kind == "key-malformed");
    }

    [Fact]
    public void Evaluate_Ids_AreKindColonTarget()
    {
        // Arrange: все семь проблем разом.
        var snapshot = TestSnapshots.Healthy(BuiltAt) with
        {
            Etcd = TestSnapshots.HealthyEtcd(BuiltAt, alive: 1, total: 2) with
            {
                ConsecutiveFailures = 2,
                QuorumSuspected = true,
                Alarms = [new EtcdAlarm(42, EtcdAlarmType.NoSpace)],
            },
            Clusters = [TestSnapshots.FullCluster(), TestSnapshots.GhostCluster()],
            ParseErrors = [new KeyParseError("/clusters/demo/config", "битый JSON")],
        };

        // Act: 30 c с постройки → snapshot-stale тоже активен.
        var alerts = EvaluateAll(snapshot, nowUtc: BuiltAt + TimeSpan.FromSeconds(30));

        // Assert
        alerts.Should().HaveCount(7);
        alerts.Should().OnlyContain(a => a.Id == $"{a.Kind}:{a.Target}");
    }
}
