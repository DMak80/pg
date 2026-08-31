using AdminPanel.Core;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер ключей доступа воркеров /pgworker/api/ и /kafkaworker/api/ (arch/02 §2.3.1/§2.3.2):
// валидные → WorkerEndpoint, битый JSON/без url → KeyParseError, тик не роняют.
public class WorkerEndpointsParserTests
{
    [Fact]
    public void Parse_ValidAndMalformed()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/pgworker/api/abc123", """{"url":"http://h:8080","instance":"abc123","since_unix":1756000000}""", 1),
            new("/pgworker/api/bad", "{not-json", 2),
        };

        // Act
        var (endpoints, errors) = WorkerEndpointsParser.Parse(kvs);

        // Assert
        endpoints.Should().ContainSingle().Which
            .Should().Be(new WorkerEndpoint("abc123", "http://h:8080", 1756000000));
        errors.Should().ContainSingle(e => e.Key == "/pgworker/api/bad");
    }

    [Fact]
    public void Parse_MissingUrl_IsParseError()
    {
        // Arrange — JSON без url бесполезен для резолва API: parseError
        var kvs = new List<Kv>
        {
            new("/kafkaworker/api/xyz", """{"instance":"xyz","since_unix":1756000000}""", 3),
        };

        // Act
        var (endpoints, errors) = WorkerEndpointsParser.Parse(kvs);

        // Assert
        endpoints.Should().BeEmpty();
        errors.Should().ContainSingle(e => e.Key == "/kafkaworker/api/xyz");
    }

    [Fact]
    public void Parse_EmptyLeafAndNonObject_AreParseErrors()
    {
        // Arrange — пустой id листа и не-объект (массив) — неканонические значения
        var kvs = new List<Kv>
        {
            new("/pgworker/api/", """{"url":"http://h:8080"}""", 4),
            new("/pgworker/api/arr", "[1,2]", 5),
        };

        // Act
        var (endpoints, errors) = WorkerEndpointsParser.Parse(kvs);

        // Assert
        endpoints.Should().BeEmpty();
        errors.Should().HaveCount(2);
    }

    [Fact]
    public void Parse_MissingSinceUnix_ToleratedAsZero()
    {
        // Arrange — since_unix вторичен (статистика): отсутствие не роняет ключ
        var kvs = new List<Kv>
        {
            new("/kafkaworker/api/w1", """{"url":"http://w1:8080","instance":"w1"}""", 6),
        };

        // Act
        var (endpoints, errors) = WorkerEndpointsParser.Parse(kvs);

        // Assert
        errors.Should().BeEmpty();
        endpoints.Should().ContainSingle().Which
            .Should().Be(new WorkerEndpoint("w1", "http://w1:8080", 0));
    }
}
