using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер libpq keyword-строки DSN из /clusters/<C>/shards/<X>/dsn (spec §6.4).
public class DsnParserTests
{
    [Fact]
    public void Parse_MultiHost_SplitsByComma()
    {
        // Arrange
        var dsn = "host=s1a,s1b port=5432 dbname=demo user=postgres";

        // Act
        var info = DsnParser.Parse(dsn);

        // Assert
        info.Hosts.Should().Equal("s1a", "s1b");
        info.Port.Should().Be(5432);
        info.DbName.Should().Be("demo");
        info.User.Should().Be("postgres");
    }

    [Fact]
    public void Parse_MissingKeywords_Nulls()
    {
        // Arrange
        var dsn = "host=n1";

        // Act
        var info = DsnParser.Parse(dsn);

        // Assert
        info.Hosts.Should().Equal("n1");
        info.Port.Should().BeNull();
        info.DbName.Should().BeNull();
        info.User.Should().BeNull();
    }

    [Fact]
    public void Parse_ExtraKeywords_Ignored()
    {
        // Arrange
        var dsn = "host=n1 port=5432 dbname=d user=u sslmode=require application_name=x";

        // Act
        var info = DsnParser.Parse(dsn);

        // Assert
        info.Hosts.Should().Equal("n1");
        info.DbName.Should().Be("d");
        info.User.Should().Be("u");
    }

    [Fact]
    public void Parse_Empty_NoHosts()
    {
        // Arrange
        // Act
        var info = DsnParser.Parse("");

        // Assert
        info.Hosts.Should().BeEmpty();
        info.Port.Should().BeNull();
    }
}
