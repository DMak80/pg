using AdminPanel.Core;
using Shared.Etcd.Client;
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

    [Fact]
    public void Parse_LastFailoverAndRebuild_Mapped()
    {
        // Arrange: work-ключ с закрытым failover и открытым rebuild
        var kv = new Kv("/pgworker/work/shop", """
            {"op":"supervise","phase":"supervising","instance":"i1","updated_unix":1756000000,
             "unreachable":{"shard1/shard1b":1756000000},
             "last_failover":{"shard":"shard1","node":"shard1b","cause":"accelerated","detected_unix":1000,"resolved_unix":1075,"duration_sec":75},
             "last_rebuild":{"shard":"shard1","node":"shard1a","cause":"auto-dead","detected_unix":900}}
            """, 42);

        // Act
        var result = WorkJournalParser.Parse([kv]);

        // Assert
        var item = result.Items.Should().ContainSingle().Subject;
        item.LastFailover.Should().NotBeNull();
        item.LastFailover!.Shard.Should().Be("shard1");
        item.LastFailover.Node.Should().Be("shard1b");
        item.LastFailover.Cause.Should().Be("accelerated");
        item.LastFailover.DetectedUnix.Should().Be(1000);
        item.LastFailover.ResolvedUnix.Should().Be(1075);
        item.LastFailover.DurationSec.Should().Be(75);
        item.LastRebuild!.Cause.Should().Be("auto-dead");
        item.LastRebuild.ResolvedUnix.Should().BeNull();
        result.Errors.Should().BeEmpty();
    }

    [Fact]
    public void Parse_OldKeyWithoutFacts_Nulls()
    {
        // Arrange: старый ключ (надзор прежней версии) — полей фактов нет
        var kv = new Kv("/pgworker/work/old",
            """{"op":"supervise","phase":"supervising","instance":"i","updated_unix":1756000000}""", 42);

        // Act
        var result = WorkJournalParser.Parse([kv]);

        // Assert: толерантный читатель — факты null, не ошибка
        var item = result.Items.Should().ContainSingle().Subject;
        item.LastFailover.Should().BeNull();
        item.LastRebuild.Should().BeNull();
        result.Errors.Should().BeEmpty();
    }

    [Fact]
    public void Parse_UnrelatedFields_Ignored()
    {
        // Arrange: незнакомые поля значения — игнор
        var kv = new Kv("/pgworker/work/shop", """
            {"op":"supervise","phase":"supervising","instance":"i1","updated_unix":1756000000,
             "future_field":123,"last_failover":{"shard":"s","node":"n","cause":"elections","detected_unix":1,"extra":"x"}}
            """, 42);

        // Act
        var result = WorkJournalParser.Parse([kv]);

        // Assert
        result.Items.Should().ContainSingle().Subject.LastFailover!.Cause.Should().Be("elections");
        result.Errors.Should().BeEmpty();
    }

    [Fact]
    public void Parse_MalformedFactField_ParseError()
    {
        // Arrange: поле last_failover не объект / без обязательных полей — битое значение ключа
        var kv = new Kv("/pgworker/work/shop",
            """{"op":"supervise","phase":"supervising","instance":"i1","updated_unix":1756000000,"last_failover":"oops"}""", 42);

        // Act
        var result = WorkJournalParser.Parse([kv]);

        // Assert: parseError-запись (существующий паттерн толерантности)
        result.Items.Should().BeEmpty();
        result.Errors.Should().ContainSingle().Which.Key.Should().Be("/pgworker/work/shop");
    }
}
