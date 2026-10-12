using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.Docker.Drivers;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// Резолвер размещения (arch/23 §4): предикат равенства по атрибутам хостов,
// least-loaded + лексикографический tie-break — детерминизм повторных проходов.
public class PlacementResolverTests
{
    private static IReadOnlyList<S3HostAttributes> Hosts() =>
    [
        new("h1", 3, new Dictionary<string, string>()),
        new("h2", 1, new Dictionary<string, string>()),
        new("n1", 0, new Dictionary<string, string> { ["geo"] = "dc1", ["tier"] = "s3" }),
        new("n2", 5, new Dictionary<string, string> { ["geo"] = "dc1" }),
    ];

    [Fact]
    public void PlainSelector_DockerHost_MatchesByName()
    {
        // Arrange — plain-селектор docker_host=h1.
        var selector = new S3NodePlacement("h1", null, null);

        // Act
        var result = PlacementResolver.Resolve(selector, Hosts(), out var candidates);

        // Assert — точное совпадение имени.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("h1");
    }

    [Fact]
    public void NoCandidates_Failed_EmptyCandidates()
    {
        // Arrange — такого хоста нет.
        var selector = new S3NodePlacement("nope", null, null);

        // Act
        var result = PlacementResolver.Resolve(selector, Hosts(), out var candidates);

        // Assert — Failed (transient P1), перечень кандидатов пуст.
        result.IsSuccess.Should().BeFalse();
        candidates.Should().BeEmpty();
    }

    [Fact]
    public void LeastLoaded_PicksFewestSlots()
    {
        // Arrange — без селектора: h2 (1 слот) против h1 (3), n1/n2 (0/5, имена n*).
        var selector = new S3NodePlacement(null, null, null);

        // Act
        var result = PlacementResolver.Resolve(selector, Hosts(), out var candidates);

        // Assert — least-loaded: n1 (0 слотов) первым; порядок кандидатов детерминирован.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("n1");
        candidates.Should().Equal(["n1", "h2", "h1", "n2"]);
    }

    [Fact]
    public void TieBreak_LexicographicOrdinal()
    {
        // Arrange — два хоста с равными слотами.
        var hosts = new List<S3HostAttributes>
        {
            new("b-host", 1, new Dictionary<string, string>()),
            new("a-host", 1, new Dictionary<string, string>()),
        };
        var selector = new S3NodePlacement(null, null, null);

        // Act
        var result = PlacementResolver.Resolve(selector, hosts, out _);

        // Assert — tie 1/1 → лексикографически меньший (Ordinal).
        result.Value.Should().Be("a-host");
    }

    [Fact]
    public void SwarmSelector_NodeName_Matches()
    {
        // Arrange — swarm-селектор swarm_node=n1.
        var selector = new S3NodePlacement(null, "n1", null);

        // Act
        var result = PlacementResolver.Resolve(selector, Hosts(), out _);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("n1");
    }

    [Fact]
    public void SwarmLabels_SingleLabel_MatchesSubset()
    {
        // Arrange — метка geo=dc1: кандидаты n1 и n2.
        var selector = new S3NodePlacement(null, null, new Dictionary<string, string> { ["geo"] = "dc1" });

        // Act
        var result = PlacementResolver.Resolve(selector, Hosts(), out var candidates);

        // Assert — least-loaded из {n1(0), n2(5)}.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("n1");
        candidates.Should().Equal(["n1", "n2"]);
    }

    [Fact]
    public void SwarmLabels_Conjunction_HostWithoutTier_NotCandidate()
    {
        // Arrange — конъюнкция geo=dc1 И tier=s3: у n2 нет tier.
        var selector = new S3NodePlacement(null, null, new Dictionary<string, string>
        {
            ["geo"] = "dc1",
            ["tier"] = "s3",
        });

        // Act
        var result = PlacementResolver.Resolve(selector, Hosts(), out var candidates);

        // Assert — кандидат один.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("n1");
        candidates.Should().Equal(["n1"]);
    }

    [Fact]
    public void Idempotent_RepeatResolve_SameHost()
    {
        // Arrange — один и тот же селектор/хосты дважды.
        var selector = new S3NodePlacement(null, null, new Dictionary<string, string> { ["geo"] = "dc1" });

        // Act
        var first = PlacementResolver.Resolve(selector, Hosts(), out _);
        var second = PlacementResolver.Resolve(selector, Hosts(), out _);

        // Assert — идемпотентность повторных проходов.
        first.Value.Should().Be(second.Value);
    }
}
