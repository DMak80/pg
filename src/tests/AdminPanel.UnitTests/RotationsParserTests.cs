using AdminPanel.Core;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Shared.Etcd.Client;
using Xunit;

namespace AdminPanel.UnitTests;

// RotationsParser (u07 — разбор /pgworker/rotations/<C>, arch/pgworker/00-etcd-contract.md §4): свои
// тесты (раньше — только happy-path внутри рефрешер-теста): валидная заявка
// (requested_by есть/нет), битый JSON, не-объект, нет requested_unix, пустой
// лист ключа; сортировка по кластеру. AAA.
public class RotationsParserTests
{
    private static Kv Kv(string key, string value) => new(key, value, 1);

    [Fact]
    public void Parse_ValidTickets_WithAndWithoutRequestedBy()
    {
        // Arrange — две заявки: с оператором и без
        var kvs = new List<Kv>
        {
            Kv("/pgworker/rotations/demo", """{"requested_unix":1761900000,"requested_by":"ops"}"""),
            Kv("/pgworker/rotations/other", """{"requested_unix":1761900100}"""),
        };

        // Act
        var result = RotationsParser.Parse(kvs);

        // Assert — обе записи, поля как есть
        result.Errors.Should().BeEmpty();
        result.Tickets.Should().HaveCount(2);
        var demo = result.Tickets.Should().Contain(t => t.Cluster == "demo").Subject;
        demo.RequestedUnix.Should().Be(1761900000);
        demo.RequestedBy.Should().Be("ops");
        var other = result.Tickets.Should().Contain(t => t.Cluster == "other").Subject;
        other.RequestedUnix.Should().Be(1761900100);
        other.RequestedBy.Should().BeNull();
    }

    [Fact]
    public void Parse_TicketsSortedByCluster()
    {
        // Arrange — порядок входа не канонический
        var kvs = new List<Kv>
        {
            Kv("/pgworker/rotations/zeta", """{"requested_unix":1}"""),
            Kv("/pgworker/rotations/alpha", """{"requested_unix":2}"""),
        };

        // Act
        var result = RotationsParser.Parse(kvs);

        // Assert — сортировка Ordinal по кластеру
        result.Tickets.Select(t => t.Cluster).Should().Equal(["alpha", "zeta"]);
    }

    [Fact]
    public void Parse_BrokenJson_ParseErrorTickNotLost()
    {
        // Arrange — битый JSON (толерантность: тик не роняет, key-malformed)
        var kvs = new List<Kv> { Kv("/pgworker/rotations/demo", "not-json{") };

        // Act
        var result = RotationsParser.Parse(kvs);

        // Assert
        result.Tickets.Should().BeEmpty();
        result.Errors.Should().ContainSingle().Which.Should().Match<KeyParseError>(e =>
            e.Key == "/pgworker/rotations/demo" && e.Reason.Contains("broken rotation JSON"));
    }

    [Theory]
    [InlineData("[]")]
    [InlineData("\"string\"")]
    [InlineData("42")]
    public void Parse_NonObjectValue_ParseError(string value)
    {
        // Arrange — валидный JSON, но не объект
        var kvs = new List<Kv> { Kv("/pgworker/rotations/demo", value) };

        // Act
        var result = RotationsParser.Parse(kvs);

        // Assert
        result.Tickets.Should().BeEmpty();
        result.Errors.Should().ContainSingle().Which.Reason.Should().Contain("not a JSON object");
    }

    [Fact]
    public void Parse_MissingRequestedUnix_ParseError()
    {
        // Arrange — объект без обязательного requested_unix
        var kvs = new List<Kv> { Kv("/pgworker/rotations/demo", """{"requested_by":"ops"}""") };

        // Act
        var result = RotationsParser.Parse(kvs);

        // Assert
        result.Tickets.Should().BeEmpty();
        result.Errors.Should().ContainSingle().Which.Reason.Should().Contain("requested_unix required");
    }

    [Fact]
    public void Parse_EmptyClusterLeaf_ParseError()
    {
        // Arrange — ключ с пустым листом (неканонический хвост range)
        var kvs = new List<Kv> { Kv("/pgworker/rotations/", """{"requested_unix":1}""") };

        // Act
        var result = RotationsParser.Parse(kvs);

        // Assert
        result.Tickets.Should().BeEmpty();
        result.Errors.Should().ContainSingle().Which.Reason.Should().Contain("empty cluster name");
    }

    [Fact]
    public void Parse_EmptyRange_EmptyResultNoErrors()
    {
        // Arrange/Act — ключей нет (заявок нет — норма)
        var result = RotationsParser.Parse([]);

        // Assert
        result.Tickets.Should().BeEmpty();
        result.Errors.Should().BeEmpty();
    }
}
