using AdminPanel.Core;
using Shared.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер range /pgworker/evacuations/ (u12, arch/14 §3.2/§5 D): валидный DONE +
// QUARANTINED с returned_unix и buckets-словарём; битый JSON / не объект /
// нет buckets → parseError, тик не роняет. AAA.
public class EvacuationsParserTests
{
    [Fact]
    public void Parse_ValidDone_WithBucketsMap()
    {
        // Arrange — журнал эвакуации E0–E4 (state=DONE, план старый→новый)
        var kvs = new[]
        {
            new Kv("/pgworker/evacuations/demo/s1",
                """{"evacuated_unix":1761900000,"reason":"shard dead > 300s","buckets":{"bucket_0":"shard2","bucket_1":"shard2","bucket_2":"shard3"},"state":"DONE"}""", 1),
        };

        // Act
        var result = EvacuationsParser.Parse(kvs);

        // Assert
        result.Errors.Should().BeEmpty();
        var item = result.Items.Should().ContainSingle().Subject;
        item.Cluster.Should().Be("demo");
        item.Shard.Should().Be("s1");
        item.EvacuatedUnix.Should().Be(1761900000);
        item.Reason.Should().Be("shard dead > 300s");
        item.State.Should().Be("DONE");
        item.ReturnedUnix.Should().BeNull();
        item.Buckets.Should().HaveCount(3);
        item.Buckets["bucket_0"].Should().Be("shard2");
    }

    [Fact]
    public void Parse_QuarantinedWithReturnedUnix_CarriesReturn()
    {
        // Arrange — возврат шарда: state=QUARANTINED + returned_unix (arch/14 §5 D)
        var kvs = new[]
        {
            new Kv("/pgworker/evacuations/demo/s2",
                """{"evacuated_unix":1761900000,"reason":"dead","buckets":{"bucket_3":"shard1"},"state":"QUARANTINED","returned_unix":1761903600}""", 1),
        };

        // Act
        var result = EvacuationsParser.Parse(kvs);

        // Assert
        result.Errors.Should().BeEmpty();
        var item = result.Items.Single();
        item.State.Should().Be("QUARANTINED");
        item.ReturnedUnix.Should().Be(1761903600);
    }

    [Fact]
    public void Parse_BrokenJson_ParseError()
    {
        // Arrange
        var kvs = new[]
        {
            new Kv("/pgworker/evacuations/demo/s3", "{broken", 1),
        };

        // Act
        var result = EvacuationsParser.Parse(kvs);

        // Assert — толерантно: parseError, ключ не трогаем
        result.Items.Should().BeEmpty();
        result.Errors.Should().ContainSingle()
            .Which.Key.Should().Be("/pgworker/evacuations/demo/s3");
    }

    [Fact]
    public void Parse_NotAnObject_ParseError()
    {
        // Arrange
        var kvs = new[]
        {
            new Kv("/pgworker/evacuations/demo/s4", "\"just-a-string\"", 1),
        };

        // Act
        var result = EvacuationsParser.Parse(kvs);

        // Assert
        result.Items.Should().BeEmpty();
        result.Errors.Should().ContainSingle();
    }

    [Fact]
    public void Parse_MissingBuckets_ParseError()
    {
        // Arrange — план эвакуации без раскладки бакетов неполон
        var kvs = new[]
        {
            new Kv("/pgworker/evacuations/demo/s5",
                """{"evacuated_unix":1761900000,"reason":"dead","state":"DONE"}""", 1),
        };

        // Act
        var result = EvacuationsParser.Parse(kvs);

        // Assert
        result.Items.Should().BeEmpty();
        result.Errors.Should().ContainSingle().Which.Reason.Should().Contain("buckets");
    }

    [Fact]
    public void Parse_BadState_ParseError()
    {
        // Arrange — незнакомое state (домен воркера развивается)
        var kvs = new[]
        {
            new Kv("/pgworker/evacuations/demo/s6",
                """{"evacuated_unix":1761900000,"reason":"dead","buckets":{},"state":"SOMEDAY"}""", 1),
        };

        // Act
        var result = EvacuationsParser.Parse(kvs);

        // Assert
        result.Items.Should().BeEmpty();
        result.Errors.Should().ContainSingle();
    }
}
