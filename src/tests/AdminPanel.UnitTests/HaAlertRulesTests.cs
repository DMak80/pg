using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// HA-правила алертов t06 (spec §10.1): источники /service/ и Patroni-проба.
// SQL-правила (slot-*/sync/inventory) — в этом же файле, добавляются следующим таском.
public class HaAlertRulesTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    // Хелпер порогов: имя LagOptions — чтобы не конфликтовать с Microsoft.Extensions.Options.Options.
    private static AlertsOptions LagOptions(long replicaLag = 16L * 1024 * 1024)
        => new() { ReplicaLagBytes = replicaLag };

    [Fact]
    public void ShardNoLeader_MatchedNoLeader_Critical()
    {
        // Arrange: matched-скоп без leader-ключа.
        var snapshot = TestSnapshots.WithHaScopes(Now) with
        {
            HaScopes = [TestSnapshots.HaScopeDemo(Now) with { LeaderName = null }],
        };

        // Act
        var alerts = new ShardNoLeaderRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("shard-no-leader:demo-s1");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Details!.Keys.Should().Contain(["scope", "cluster", "shard"]);
    }

    [Fact]
    public void ShardNoLeader_WithLeader_NoAlert()
    {
        // Arrange — лидер есть.
        var snapshot = TestSnapshots.WithHaScopes(Now);

        // Act
        var alerts = new ShardNoLeaderRule().Evaluate(snapshot, Context()).ToList();

        // Assert: other-scope без лидера не считается — unmatched не алертится (spec §3.10).
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void HaMemberNotStreaming_ReplicaNotStreaming_Warning()
    {
        // Arrange: реплика в starting с успешной пробой.
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members =
                [
                    new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, Now, null),
                    new HaMember("s1b", "s1b", 5432, "replica", "starting", 1L, 10L, Now, null),
                ],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new HaMemberNotStreamingRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("ha-member-not-streaming:demo-s1/s1b");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Details!["expected"].Should().Be("streaming");
        alert.Details["state"].Should().Be("starting");
    }

    [Fact]
    public void HaMemberNotStreaming_MasterNotRunning_Warning()
    {
        // Arrange: мастер остановился.
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members = [new HaMember("s1a", "s1a", 5432, "master", "stopped", 1L, null, Now, null)],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new HaMemberNotStreamingRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        alerts.Should().ContainSingle().Which.Details!["expected"].Should().Be("running");
    }

    [Fact]
    public void HaMemberNotStreaming_UnknownRole_Skipped()
    {
        // Arrange: sync_standby — каталожного ожидания нет (spec §3.13).
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members = [new HaMember("s1c", "s1c", 5432, "sync_standby", "streaming", 1L, 0L, Now, null)],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new HaMemberNotStreamingRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void HaMemberNotStreaming_ProbeErrorOrMissing_Skipped()
    {
        // Arrange: в одном matched-скопе — здоровый мастер, член с упавшей пробой
        // (DCS state "crashed", ProbeError задан) и член до первого тика пробы
        // (ProbeAtUtc null): ни один не должен алертиться этим правилом.
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members =
                [
                    new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, Now, null),
                    new HaMember("err", "err", 5432, "replica", "crashed", null, null, Now, "connection refused"),
                    new HaMember("cold", "cold", 5432, "replica", null, null, null, null, null),
                ],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new HaMemberNotStreamingRule().Evaluate(snapshot, Context()).ToList();

        // Assert: ошибка пробы — зона probe-failed; без пробы — данных нет (spec §3.13).
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void ReplicaLagHigh_AboveThreshold_Warning()
    {
        // Arrange: лаг s1b = 17 МБ > 16 МБ.
        var snapshot = TestSnapshots.WithHaScopes(Now);
        var rule = new ReplicaLagHighRule(Options.Create(LagOptions()));

        // Act
        var alerts = rule.Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("replica-lag-high:demo-s1/s1b");
        alert.Details!["lagBytes"].Should().Be((17L * 1024 * 1024).ToString());
        alert.Details["thresholdBytes"].Should().Be((16L * 1024 * 1024).ToString());
    }

    [Fact]
    public void ReplicaLagHigh_AtThreshold_NoAlert()
    {
        // Arrange: ровно порог — не «больше» (строгие сравнения каталога).
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members = [new HaMember("s1b", "s1b", 5432, "replica", "streaming", 1L, 16L * 1024 * 1024, Now, null)],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new ReplicaLagHighRule(Options.Create(LagOptions())).Evaluate(snapshot, Context()).ToList();

        // Assert
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void ReplicaLagHigh_CustomThreshold_FromOptions()
    {
        // Arrange: порог 100 байт из настроек.
        var snapshot = TestSnapshots.WithHaScopes(Now);
        var rule = new ReplicaLagHighRule(Options.Create(LagOptions(100)));

        // Act
        var alerts = rule.Evaluate(snapshot, Context()).ToList();

        // Assert: лаг 0 мастера не алертится, s1b (17 МБ) — да.
        alerts.Should().ContainSingle().Which.Target.Should().Be("demo-s1/s1b");
    }

    [Fact]
    public void ReplicaLagHigh_NoProbe_Silent()
    {
        // Arrange: член без проб (LagBytes null).
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members = [new HaMember("s1b", "s1b", 5432, "replica", "streaming", null, null, null, null)],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new ReplicaLagHighRule(Options.Create(LagOptions())).Evaluate(snapshot, Context()).ToList();

        // Assert: SQL/Patroni-алерты — только при включённых пробах (03 §4).
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void ProbeFailed_EachFailedResult_Info()
    {
        // Arrange: одна patroni- и одна sql-проба упали.
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Probes =
            [
                new ProbeResult("demo-s1/s1a", "patroni", false, 3.0, "connection refused", Now),
                new ProbeResult("demo/s1", "sql", false, 4.0, "timeout", Now),
                new ProbeResult("demo-s1/s1b", "patroni", true, 5.0, null, Now),
            ],
        };

        // Act
        var alerts = new ProbeFailedRule().Evaluate(snapshot, Context()).ToList();

        // Assert: id включает kind — уникальность при пересечении имён (spec §3.14).
        alerts.Should().HaveCount(2);
        alerts.Should().OnlyContain(a => a.Severity == AlertSeverity.Info);
        alerts.Select(a => a.Id).Should().BeEquivalentTo(
            ["probe-failed:patroni:demo-s1/s1a", "probe-failed:sql:demo/s1"]);
        alerts.Single(a => a.Kind == "probe-failed" && a.Details!["kind"] == "sql")
            .Details!["error"].Should().Be("timeout");
    }

    private static AlertContext Context() => new(null, Now, 3);
}
