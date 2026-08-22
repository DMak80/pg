using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Слияние состояния проб со снапшотом (spec §4.2, §10.6): обогащение членов,
// runtime шардов, перенос Probes; null-состояние и лишние ключи безопасны.
public class ProbeEnricherTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static HaMember Member(string name, string? role = "replica", string? state = "streaming") =>
        new(name, name, 5432, role, state, null, null, null, null);

    private static EtcdSnapshot SnapshotWithScope() => TestSnapshots.Healthy(Now) with
    {
        HaScopes =
        [
            new HaScope("demo-s1", "demo", "s1", true, "s1a", null, true,
                [Member("s1a", "master", "running"), Member("s1b")], null),
        ],
    };

    [Fact]
    public void Apply_NullState_NoChange()
    {
        // Arrange
        var snapshot = SnapshotWithScope();

        // Act
        var result = ProbeEnricher.Apply(snapshot, null);

        // Assert: тиков проб не было — снапшот не тронут (Probes уже пуст, t03).
        result.Should().BeSameAs(snapshot);
        result.HaScopes.Single().Members.Single(m => m.Name == "s1b").LagBytes.Should().BeNull();
    }

    [Fact]
    public void Apply_SuccessfulProbe_OverridesMemberFields()
    {
        // Arrange
        var snapshot = SnapshotWithScope();
        var state = new ProbeState(
            Now, [],
            new Dictionary<string, HaMemberProbe>
            {
                ["demo-s1/s1b"] = new("replica", "streaming", 2L, 12345L, Now, null),
            },
            new Dictionary<string, ShardRuntime>());

        // Act
        var result = ProbeEnricher.Apply(snapshot, state);

        // Assert: REST перекрывает DCS-поля, probeError снят (spec §3.5).
        var member = result.HaScopes.Single().Members.Single(m => m.Name == "s1b");
        member.Timeline.Should().Be(2L);
        member.LagBytes.Should().Be(12345L);
        member.ProbeAtUtc.Should().Be(Now);
        member.ProbeError.Should().BeNull();
    }

    [Fact]
    public void Apply_FailedProbe_KeepsDcsRoleState()
    {
        // Arrange
        var snapshot = SnapshotWithScope();
        var state = new ProbeState(
            Now,
            [new ProbeResult("demo-s1/s1b", "patroni", false, 5.0, "connection refused", Now)],
            new Dictionary<string, HaMemberProbe>
            {
                ["demo-s1/s1b"] = new(null, null, null, null, Now, "connection refused"),
            },
            new Dictionary<string, ShardRuntime>());

        // Act
        var result = ProbeEnricher.Apply(snapshot, state);

        // Assert: etcd-часть HA остаётся, лаги не показываем (spec §3.5).
        var member = result.HaScopes.Single().Members.Single(m => m.Name == "s1b");
        member.Role.Should().Be("replica");
        member.State.Should().Be("streaming");
        member.Timeline.Should().BeNull();
        member.LagBytes.Should().BeNull();
        member.ProbeAtUtc.Should().Be(Now);
        member.ProbeError.Should().Be("connection refused");
        result.Probes.Should().ContainSingle().Which.Ok.Should().BeFalse();
    }

    [Fact]
    public void Apply_SetsRuntimeAndProbes()
    {
        // Arrange
        var snapshot = TestSnapshots.Healthy(Now);
        var runtime = new ShardRuntime("s1", [], [], [], ["bucket_0"], false, null);
        var state = new ProbeState(
            Now,
            [new ProbeResult("demo/s1", "sql", true, 12.0, null, Now)],
            new Dictionary<string, HaMemberProbe>(),
            new Dictionary<string, ShardRuntime> { ["demo/s1"] = runtime });

        // Act
        var result = ProbeEnricher.Apply(snapshot, state);

        // Assert: runtime по ключу кластер/шард; Probes = список состояния (spec §4.2).
        result.Clusters.Single().Shards.Single().Runtime.Should().BeSameAs(runtime);
        result.Probes.Should().ContainSingle().Which.Target.Should().Be("demo/s1");
    }

    [Fact]
    public void Apply_StaleTargetsIgnored()
    {
        // Arrange: состояние ссылается на исчезнувшие скоп/шард — лишние ключи не падают.
        var snapshot = SnapshotWithScope();
        var state = new ProbeState(
            Now, [],
            new Dictionary<string, HaMemberProbe> { ["gone-scope/gone"] = new("replica", "streaming", 1L, 0L, Now, null) },
            new Dictionary<string, ShardRuntime> { ["demo/gone"] = new("gone", [], [], [], [], false, null) });

        // Act
        var result = ProbeEnricher.Apply(snapshot, state);

        // Assert: снапшот валиден, поля посторонних ключей не проявились.
        result.HaScopes.Single().Members.Should().OnlyContain(m => m.ProbeAtUtc == null);
        result.Clusters.Single().Shards.Single().Runtime.Should().BeNull();
    }
}
