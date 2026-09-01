using AdminPanel.Core;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// WorkJournalParser (arch/adminpanel/02 §2.3.1): журнал /pgworker/work/<C> →
// WorkJournalInfo; битый JSON — ParseError (алерт key-malformed), тик не роняет.
public class WorkJournalParserTests
{
    private static Kv WorkKv(string cluster, string json)
        => new($"/pgworker/work/{cluster}", json, 42);

    [Fact]
    public void Parse_ProvisionFailed_AllSeriesFields()
    {
        // Arrange: журнал с серией фейлов (канон arch/14 §3.3).
        var json = File.ReadAllText("EtcdFixtures/work-provision-failed.json");

        // Act
        var result = WorkJournalParser.Parse([WorkKv("shop", json)]);

        // Assert
        result.Errors.Should().BeEmpty();
        var w = result.Items.Should().ContainSingle().Subject;
        w.Cluster.Should().Be("shop");
        w.Op.Should().Be("provision");
        w.LastError.Should().Contain("не поднялся");
        w.FailCount.Should().Be(3);
        w.FailFirstUnix.Should().Be(1756005400);
        w.RetryNotBeforeUnix.Should().Be(1756009215);
    }

    [Fact]
    public void Parse_LegacyFormat_NullRetryFields()
    {
        // Arrange: старый формат без полей серии (обратная совместимость).
        var json = File.ReadAllText("EtcdFixtures/work-legacy.json");

        // Act
        var result = WorkJournalParser.Parse([WorkKv("demo", json)]);

        // Assert
        var w = result.Items.Should().ContainSingle().Subject;
        w.Op.Should().Be("supervise");
        w.FailCount.Should().BeNull();
        w.RetryNotBeforeUnix.Should().BeNull();
    }

    [Fact]
    public void Parse_MalformedJson_ParseErrorNotThrow()
    {
        // Arrange: битый JSON ключа — ключ скипается с ParseError (домен воркера, не трогаем).

        // Act
        var result = WorkJournalParser.Parse([WorkKv("bad", "{не-json")]);

        // Assert
        result.Items.Should().BeEmpty();
        result.Errors.Should().ContainSingle().Which.Key.Should().Be("/pgworker/work/bad");
    }
}
