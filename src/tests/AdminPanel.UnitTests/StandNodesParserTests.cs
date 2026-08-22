using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер стендового топо-реестра /cluster/nodes/<node> → IP (spec §10.3, arch/02 §2.3).
public class StandNodesParserTests
{
    [Fact]
    public void Parse_Nodes_MappedToStandNode()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("stand-nodes.json");

        // Act
        var nodes = StandNodesParser.Parse(kvs);

        // Assert
        nodes.Should().Contain(n => n.Name == "s1a" && n.Address == "172.28.0.11");
        nodes.Should().HaveCount(4);
    }

    [Fact]
    public void Parse_EmptyValue_NullAddress()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("stand-nodes.json");

        // Act
        var nodes = StandNodesParser.Parse(kvs);

        // Assert
        nodes.Should().Contain(n => n.Name == "s2b" && n.Address == null);
    }

    [Fact]
    public void Parse_EmptyPrefix_EmptyResult()
    {
        // Arrange — в проде префикса нет: пустой ответ range
        // Act
        var nodes = StandNodesParser.Parse([]);

        // Assert
        nodes.Should().BeEmpty();
    }
}
