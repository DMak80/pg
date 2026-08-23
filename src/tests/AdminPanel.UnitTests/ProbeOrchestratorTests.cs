using AdminPanel.Core;
using AdminPanel.Probes;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Оркестратор проб на фейках (spec §10.7): цели из снапшота (matched/Dsn),
// параллельность обеих проб, отключаемость, ошибка цели не роняет тик.
public class ProbeOrchestratorTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private sealed class FixedTime : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => Now;
    }

    // Фейк Patroni-пробы: помнит вызовы, поведение настраивается.
    private sealed class FakePatroniProbe : IPatroniRestProbe
    {
        public List<(string Scope, string Member)> Calls { get; } = [];

        public Func<string, string, PatroniMemberResult>? Respond { get; set; }

        public bool Throw { get; set; }

        public Task<PatroniMemberResult> ProbeAsync(HaScope scope, HaMember member, CancellationToken ct)
        {
            Calls.Add((scope.Scope, member.Name));
            if (Throw)
                throw new HttpRequestException("patroni probe crashed");
            return Task.FromResult(Respond!(scope.Scope, member.Name));
        }
    }

    // Фейк SQL-пробы.
    private sealed class FakeSqlProbe : ISqlProbe
    {
        public List<(string Cluster, string Shard)> Calls { get; } = [];

        public Func<string, string, SqlShardResult>? Respond { get; set; }

        public bool Throw { get; set; }

        public Task<SqlShardResult> ProbeAsync(ClusterInfo cluster, ShardInfo shard, CancellationToken ct)
        {
            Calls.Add((cluster.Name, shard.Name));
            if (Throw)
                throw new Npgsql.NpgsqlException("sql probe crashed");
            return Task.FromResult(Respond!(cluster.Name, shard.Name));
        }
    }

    // Снапшот с целями: matched-скоп demo-s1 (2 члена), unmatched-скоп, кластер demo
    // (s1 с DSN, s2-пустышка без хостов) — как spec §3.3.
    private static EtcdSnapshot Snapshot() => TestSnapshots.Healthy(Now) with
    {
        Clusters =
        [
            TestSnapshots.FullCluster() with
            {
                Shards =
                [
                    TestSnapshots.FullCluster().Shards.Single(),
                    new ShardInfo("empty", "", [], null, null, null, null, null, [], null),
                ],
            },
        ],
        HaScopes =
        [
            new HaScope("demo-s1", "demo", "s1", true, "s1a", null, true, null, null, null,
                [new HaMember("s1a", "s1a", 5432, "master", "running", null, null, null, null),
                 new HaMember("s1b", "s1b", 5432, "replica", "streaming", null, null, null, null)],
                null),
            new HaScope("other-scope", null, null, false, null, null, false, null, null, null,
                [new HaMember("n1", "n1", 5432, "replica", "streaming", null, null, null, null)],
                null),
        ],
    };

    private static (ProbeOrchestrator Orchestrator, FakePatroniProbe Patroni, FakeSqlProbe Sql, SettableStore Store)
        Orchestrator(ProbesOptions? options = null, EtcdSnapshot? snapshot = null)
    {
        var reader = new SnapshotReaderStub(snapshot);
        var store = new SettableStore();
        var patroni = new FakePatroniProbe
        {
            Respond = (scope, member) => new PatroniMemberResult(
                new HaMemberProbe("replica", "streaming", 1L, 0L, Now, null),
                new ProbeResult($"{scope}/{member}", "patroni", true, 1.0, null, Now)),
        };
        var sql = new FakeSqlProbe
        {
            Respond = (cluster, shard) => new SqlShardResult(
                new ShardRuntime(shard, [], [], [], [], false, null),
                new ProbeResult($"{cluster}/{shard}", "sql", true, 2.0, null, Now)),
        };
        var orchestrator = new ProbeOrchestrator(
            reader, store, patroni, sql,
            Options.Create(options ?? new ProbesOptions()),
            new FixedTime(),
            NullLogger<ProbeOrchestrator>.Instance);
        return (orchestrator, patroni, sql, store);
    }

    private sealed class SnapshotReaderStub(EtcdSnapshot? current) : ISnapshotReader
    {
        public EtcdSnapshot? Current { get; } = current;
    }

    internal sealed class SettableStore : IProbeStateStore
    {
        public ProbeState? Current { get; set; }

        public void Replace(ProbeState state) => Current = state;
    }

    [Fact]
    public async Task RunOnce_BuildsTargetsFromSnapshot()
    {
        // Arrange
        var (orchestrator, patroni, sql, _) = Orchestrator(snapshot: Snapshot());

        // Act
        await orchestrator.RunOnceAsync(CancellationToken.None);

        // Assert: matched-скоп — оба члена; шард с DSN — пробуется; unmatched и
        // шард без хостов — нет (spec §3.3).
        patroni.Calls.Should().BeEquivalentTo([("demo-s1", "s1a"), ("demo-s1", "s1b")]);
        sql.Calls.Should().ContainSingle().Which.Should().Be(("demo", "s1"));
    }

    [Fact]
    public async Task RunOnce_WritesStateWithBothKinds()
    {
        // Arrange
        var (orchestrator, _, _, store) = Orchestrator(snapshot: Snapshot());

        // Act
        await orchestrator.RunOnceAsync(CancellationToken.None);

        // Assert: members + runtimes + probes в одном состоянии, одна замена (spec §3.15).
        store.Current.Should().NotBeNull();
        store.Current!.Members.Keys.Should().BeEquivalentTo(["demo-s1/s1a", "demo-s1/s1b"]);
        store.Current.Runtimes.Keys.Should().BeEquivalentTo(["demo/s1"]);
        store.Current.Probes.Should().HaveCount(3);
        store.Current.AtUtc.Should().Be(Now);
    }

    [Fact]
    public async Task RunOnce_PatroniDisabled_Skipped()
    {
        // Arrange
        var (orchestrator, patroni, sql, store) = Orchestrator(
            new ProbesOptions { PatroniEnabled = false }, Snapshot());

        // Act
        await orchestrator.RunOnceAsync(CancellationToken.None);

        // Assert: sql работает, patroni-части пусты (spec §3.15).
        patroni.Calls.Should().BeEmpty();
        sql.Calls.Should().HaveCount(1);
        store.Current!.Members.Should().BeEmpty();
        store.Current.Runtimes.Should().NotBeEmpty();
        store.Current.Probes.Should().OnlyContain(p => p.Kind == "sql");
    }

    [Fact]
    public async Task RunOnce_SqlDisabled_Skipped()
    {
        // Arrange
        var (orchestrator, patroni, sql, store) = Orchestrator(
            new ProbesOptions { SqlEnabled = false }, Snapshot());

        // Act
        await orchestrator.RunOnceAsync(CancellationToken.None);

        // Assert
        sql.Calls.Should().BeEmpty();
        patroni.Calls.Should().HaveCount(2);
        store.Current!.Runtimes.Should().BeEmpty();
        store.Current.Probes.Should().OnlyContain(p => p.Kind == "patroni");
    }

    [Fact]
    public async Task RunOnce_NoSnapshot_EmptyState()
    {
        // Arrange: до первого тика refresher'а целей нет (spec §8).
        var (orchestrator, patroni, sql, store) = Orchestrator(snapshot: null);

        // Act
        await orchestrator.RunOnceAsync(CancellationToken.None);

        // Assert
        patroni.Calls.Should().BeEmpty();
        sql.Calls.Should().BeEmpty();
        store.Current.Should().NotBeNull();
        store.Current!.Probes.Should().BeEmpty();
    }

    [Fact]
    public async Task RunOnce_ProbeThrows_CapturedAsFailedResult()
    {
        // Arrange: реализации проб сами не бросают, но контракт не гарантирует —
        // оркестратор защищён (spec §3.15).
        var (orchestrator, patroni, sql, store) = Orchestrator(snapshot: Snapshot());
        patroni.Throw = true;
        patroni.Respond = null;
        sql.Throw = true;
        sql.Respond = null;

        // Act
        await orchestrator.Invoking(o => o.RunOnceAsync(CancellationToken.None))
            .Should().NotThrowAsync();
        await orchestrator.RunOnceAsync(CancellationToken.None);

        // Assert: тик не упал, ошибки — в ProbeResult(ok:false).
        store.Current!.Probes.Where(p => !p.Ok).Should().HaveCount(3);
        store.Current.Members.Values.Should().OnlyContain(m => m.Error != null);
        store.Current.Runtimes.Values.Should().OnlyContain(r => r.Error != null);
    }
}
