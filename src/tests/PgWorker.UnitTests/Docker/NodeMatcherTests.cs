using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// Матчинг контейнер↔нода усыновления (adopt-repair spec §3.1): hostname/alias,
// patroni-сайдкар по env NODE_NAME, public-порты 5432/8008/6432, неоднозначность → пропуск.
public class NodeMatcherTests
{
    private static (DockerContainer, DockerContainerInspect) Container(
        string name, string hostname, string[]? aliases = null, string[]? env = null, PortMap[]? ports = null)
        => (new DockerContainer("id-" + name, [name], "running", "img"),
            new DockerContainerInspect("id-" + name, hostname, aliases ?? [], env ?? [], ports ?? []));

    [Fact]
    public void Match_ByHostname_FillsPgPortAndObject()
    {
        // Arrange: стендовый as-s2a (hostname s2a) публикует 5432→5435.
        var containers = new[] { Container("as-s2a", "s2a", ports: [new PortMap(5432, 5435)]) };

        // Act
        var found = NodeMatcher.Match("local", containers, ["s2a"]);

        // Assert: нода найдена, object=имя контейнера, patroni/doorman=0 (нет биндингов).
        var node = found["s2a"];
        Assert.Equal("local", node.Host);
        Assert.Equal("as-s2a", node.Object);
        Assert.Equal(5435, node.Pg);
        Assert.Equal(0, node.Patroni);
        Assert.Equal(0, node.Doorman);
    }

    [Fact]
    public void Match_ByNetworkAlias_FillsNode()
    {
        // Arrange: Names отличается, но alias в сети равен имени ноды.
        var containers = new[] { Container("as-s1b", "stand-s1b-1", aliases: ["s1b"], ports: [new PortMap(5432, 5434)]) };

        // Act
        var found = NodeMatcher.Match("local", containers, ["s1b"]);

        // Assert
        Assert.Equal("as-s1b", found["s1b"].Object);
        Assert.Equal(5434, found["s1b"].Pg);
    }

    [Fact]
    public void Match_PatroniSidecarByNodeNameEnv_MergesPatroniPort()
    {
        // Arrange: PG-контейнер ноды + отдельный эмулятор hc2a (env NODE_NAME=s2a, 8008→8021).
        var containers = new[]
        {
            Container("as-s2a", "s2a", ports: [new PortMap(5432, 5435)]),
            Container("as-hc2a", "hc2a", env: ["NODE_NAME=s2a"], ports: [new PortMap(8008, 8021)]),
        };

        // Act
        var found = NodeMatcher.Match("local", containers, ["s2a"]);

        // Assert: pg из контейнера ноды, patroni из сайдкара.
        Assert.Equal(5435, found["s2a"].Pg);
        Assert.Equal(8021, found["s2a"].Patroni);
        Assert.Equal("as-s2a", found["s2a"].Object);
    }

    [Fact]
    public void Match_CanonicalPgwContainer_MatchesByHostname()
    {
        // Arrange: наша нода pgw-demo-s2-s2a (hostname s2a) со всеми тремя портами.
        var containers = new[]
        {
            Container("pgw-demo-s2-s2a", "s2a",
                ports: [new PortMap(5432, 15432), new PortMap(8008, 18008), new PortMap(6432, 16432)]),
        };

        // Act
        var found = NodeMatcher.Match("local", containers, ["s2a"]);

        // Assert
        Assert.Equal((15432, 18008, 16432), (found["s2a"].Pg, found["s2a"].Patroni, found["s2a"].Doorman));
    }

    [Fact]
    public void Match_AmbiguousNodeContainer_SkipsName()
    {
        // Arrange: два живых контейнера претендуют на имя ноды.
        var containers = new[]
        {
            Container("a1", "s2a", ports: [new PortMap(5432, 5435)]),
            Container("a2", "s2a", ports: [new PortMap(5432, 5436)]),
        };

        // Act
        var found = NodeMatcher.Match("local", containers, ["s2a"]);

        // Assert: неоднозначность → безопасный пропуск (журнал — в AdoptionProcess, spec §3.1).
        Assert.False(found.ContainsKey("s2a"));
    }

    [Fact]
    public void Match_UnknownName_NotPresent()
    {
        // Arrange
        var containers = new[] { Container("as-s1a", "s1a") };

        // Act
        var found = NodeMatcher.Match("local", containers, ["s2a"]);

        // Assert
        Assert.Empty(found);
    }
}
