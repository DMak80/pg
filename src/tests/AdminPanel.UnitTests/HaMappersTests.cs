using AdminPanel.Api.Inspection;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Мапперы и хендлеры HA-зоны (spec §10.2): сводка (агрегаты) и детали (перенос полей).
public class HaMappersTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public void MapSummaries_CountsAndFlags()
    {
        // Arrange
        var scopes = new[] { TestSnapshots.HaScopeDemo(Now), TestSnapshots.UnmatchedNoLeader(Now) };

        // Act
        var summaries = HaMappers.MapSummaries(scopes);

        // Assert: membersHealthy — running/streaming; lagMax — max LagBytes (spec §3.17).
        summaries.Should().HaveCount(2);
        var demo = summaries.Single(s => s.Scope == "demo-s1");
        demo.Cluster.Should().Be("demo");
        demo.Shard.Should().Be("s1");
        demo.Matched.Should().BeTrue();
        demo.LeaderName.Should().Be("s1a");
        demo.MembersTotal.Should().Be(2);
        demo.MembersHealthy.Should().Be(2);
        demo.LagMaxBytes.Should().Be(17L * 1024 * 1024);
        var other = summaries.Single(s => s.Scope == "other-scope");
        other.Matched.Should().BeFalse();
        other.Cluster.Should().BeNull();
        other.LeaderName.Should().BeNull();
        other.MembersTotal.Should().Be(1);
        other.MembersHealthy.Should().Be(0);
        other.LagMaxBytes.Should().BeNull();
    }

    [Fact]
    public void MapSummaries_EmptyLag_NullLagMaxBytes()
    {
        // Arrange: ни у одного члена лага нет.
        var scope = TestSnapshots.HaScopeDemo(Now) with
        {
            Members = [new HaMember("s1a", "s1a", 5432, "master", "running", 1L, null, Now, null)],
        };

        // Act
        var summary = HaMappers.MapSummaries([scope]).Single();

        // Assert
        summary.LagMaxBytes.Should().BeNull();
    }

    [Fact]
    public void MapDetails_FullTransfer()
    {
        // Arrange
        var scope = TestSnapshots.HaScopeDemo(Now);

        // Act
        var details = HaMappers.MapDetails(scope);

        // Assert: все поля модели → DTO (arch/03 §2 HaScopeDto; Initialized в DTO не входит).
        details.Scope.Should().Be("demo-s1");
        details.Cluster.Should().Be("demo");
        details.Shard.Should().Be("s1");
        details.Matched.Should().BeTrue();
        details.LeaderName.Should().Be("s1a");
        details.OptimeLeader.Should().Be(738273634528L);
        details.RawConfig.Should().Be("{\"ttl\":5,\"loop_wait\":2}");
        var member = details.Members.Should().ContainSingle(m => m.Name == "s1b").Subject;
        member.Host.Should().Be("s1b");
        member.Port.Should().Be(5432);
        member.Role.Should().Be("replica");
        member.State.Should().Be("streaming");
        member.Timeline.Should().Be(1L);
        member.LagBytes.Should().Be(17L * 1024 * 1024);
        member.ProbeAtUtc.Should().Be(Now);
        member.ProbeError.Should().BeNull();
    }

    [Fact]
    public void MapDetails_WithRequests_MapsRequests()
    {
        // Arrange
        var scope = new HaScope("fresh-shard1", "fresh", "shard1", true, null, null, false,
            "0.5", "8Gi", "100Gi", [], null);

        // Act
        var dto = HaMappers.MapDetails(scope);

        // Assert
        var requests = dto.Requests.Should().NotBeNull().And.Subject.As<NodeRequestsDto>();
        requests.Cpu.Should().Be("0.5");
        requests.Disk.Should().Be("100Gi");
    }

    [Fact]
    public async Task HaScopesHandler_NoSnapshot_ReturnsSnapshotNotReady()
    {
        // Arrange
        var handler = new HaScopesQueryHandler(new SnapshotStore());

        // Act
        var result = await handler.Handle(new HaScopesQuery(), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InspectionModule.SnapshotNotReadyException>();
    }

    [Fact]
    public async Task HaDetailsHandler_UnknownScope_ReturnsScopeNotFound()
    {
        // Arrange: снапшот есть, скопа нет.
        var store = new SnapshotStore();
        store.Replace(TestSnapshots.WithHaScopes(Now));
        var handler = new HaScopeDetailsQueryHandler(store);

        // Act
        var result = await handler.Handle(new HaScopeDetailsQuery("nope"), CancellationToken.None);

        // Assert: 404-семантика (spec §3.18) — исключение различается эндпоинтом.
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InspectionModule.ScopeNotFoundException>();
    }
}
