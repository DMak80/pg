using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Shared.Etcd.Client;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер домена /etcd/ (adminpanel/02 §2.3.5): config → декларация
// (nodes, created_unix), nodes/<k>/{state,placed_on} → managed-узлы.
// Битый config-JSON — parseError, тик не падает; unknown-листы игнорируются.
public class EtcdDomainParserTests
{
    // AAA: config + листы узлов → декларация 1:1, узлы отсортированы по K
    [Fact]
    public void Config_Valid_DeclarationParsed()
    {
        // Arrange
        var config = new Kv("/etcd/config", """{"nodes":3,"created_unix":1759330000}""", 1);
        var nodeKvs = new[]
        {
            new Kv("/etcd/nodes/1/state", "RUNNING", 1),
            new Kv("/etcd/nodes/1/placed_on", "local", 1),
            new Kv("/etcd/nodes/2/state", "PROVISIONING", 1),
        };

        // Act
        var (domain, errors) = EtcdDomainParser.Parse(config, nodeKvs);

        // Assert
        errors.Should().BeEmpty();
        var model = domain!; // Parse возвращает nullable-ссылку (не Nullable<T>): .Value нет
        model.DeclaredNodes.Should().Be(3);
        model.CreatedUnix.Should().Be(1759330000);
        model.Nodes.Should().HaveCount(2);
        model.Nodes[0].K.Should().Be(1);
        model.Nodes[0].State.Should().Be("RUNNING");
        model.Nodes[0].PlacedOn.Should().Be("local");
        model.Nodes[1].K.Should().Be(2);
        model.Nodes[1].State.Should().Be("PROVISIONING");
    }

    // AAA: config-ключа нет — декларация null, узлы парсятся
    [Fact]
    public void Config_Missing_OnlyNodes()
    {
        // Arrange
        var nodeKvs = new[] { new Kv("/etcd/nodes/1/state", "RUNNING", 1) };

        // Act
        var (domain, errors) = EtcdDomainParser.Parse(null, nodeKvs);

        // Assert
        errors.Should().BeEmpty();
        var model = domain!;
        model.DeclaredNodes.Should().BeNull();
        model.CreatedUnix.Should().BeNull();
        model.Nodes.Should().ContainSingle(n => n.K == 1 && n.State == "RUNNING");
    }

    // AAA: битый config-JSON — parseError-запись, домен не null (узлы пустые, declared null)
    [Fact]
    public void Config_BrokenJson_ParseError()
    {
        // Arrange
        var config = new Kv("/etcd/config", "not-json", 1);

        // Act
        var (domain, errors) = EtcdDomainParser.Parse(config, []);

        // Assert
        var model = domain!;
        model.DeclaredNodes.Should().BeNull();
        model.Nodes.Should().BeEmpty();
        errors.Should().ContainSingle(e => e.Key == "/etcd/config");
        errors.Single().Reason.Should().Contain("broken /etcd/config JSON");
    }

    // AAA: нечисловой номер узла — лист игнорируется без ошибки
    [Fact]
    public void Node_BadNumber_Ignored()
    {
        // Arrange
        var nodeKvs = new[] { new Kv("/etcd/nodes/abc/state", "RUNNING", 1) };

        // Act
        var (domain, errors) = EtcdDomainParser.Parse(null, nodeKvs);

        // Assert
        errors.Should().BeEmpty();
        domain!.Nodes.Should().BeEmpty();
    }

    // AAA: unknown-лист usage — узел не рождается (только state/placed_on), без ошибки
    [Fact]
    public void Node_UnknownLeaf_Ignored()
    {
        // Arrange
        var nodeKvs = new[] { new Kv("/etcd/nodes/1/usage", "5", 1) };

        // Act
        var (domain, errors) = EtcdDomainParser.Parse(null, nodeKvs);

        // Assert
        errors.Should().BeEmpty();
        domain!.Nodes.Should().BeEmpty();
    }

    // AAA: только placed_on — узел рождается с State null
    [Fact]
    public void Node_OnlyPlacedOn()
    {
        // Arrange
        var nodeKvs = new[] { new Kv("/etcd/nodes/2/placed_on", "swarm-1", 1) };

        // Act
        var (domain, errors) = EtcdDomainParser.Parse(null, nodeKvs);

        // Assert
        errors.Should().BeEmpty();
        var model = domain!;
        model.Nodes.Should().ContainSingle(n => n.K == 2);
        model.Nodes[0].State.Should().BeNull();
        model.Nodes[0].PlacedOn.Should().Be("swarm-1");
    }

    // AAA: значение в кавычках — парсер тримит кавычки
    [Fact]
    public void Node_QuotedValue_Trimmed()
    {
        // Arrange
        var nodeKvs = new[] { new Kv("/etcd/nodes/1/state", "\"RUNNING\"", 1) };

        // Act
        var (domain, errors) = EtcdDomainParser.Parse(null, nodeKvs);

        // Assert
        errors.Should().BeEmpty();
        domain!.Nodes.Should().ContainSingle(n => n.State == "RUNNING");
    }
}
