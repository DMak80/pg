using PgWorker.Core.Model;
using Xunit;

namespace PgWorker.UnitTests.Model;

// Portalloc-контракт с object-полем усыновлённых нод (adopt-repair spec §3.2 AD2,
// arch/14 §2.4): имя фактического docker-контейнера; null = каноническая pgw-нода.
public class PortallocTests
{
    [Fact]
    public void Serialize_WithObject_WritesObjectField()
    {
        // Arrange: адрес усыновлённой ноды — object-контейнер вместо pgw-имени.
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["s2/s2a"] = new("local", new NodePorts(5435, 8021, 0), "as-s2a"),
        };

        // Act
        var json = Portalloc.Serialize(addresses);

        // Assert: object сериализуется, doorman=0 пишется (int, не nullable).
        Assert.Contains("\"object\":\"as-s2a\"", json);
        Assert.Contains("\"doorman\":0", json);
    }

    [Fact]
    public void RoundTrip_WithAndWithoutObject_PreservesEntries()
    {
        // Arrange: смешанный portalloc — усыновлённая нода с object и каноническая без.
        var raw = """
            {"s1/s1a":{"host":"local","pg":5433,"patroni":8011,"doorman":0,"object":"as-s1a"},
             "s1/s1b":{"host":"local","pg":5434,"patroni":8012,"doorman":16434}}
            """;

        // Act
        var parsed = Portalloc.Parse("demo", raw);
        var back = Portalloc.Serialize(parsed.Value);

        // Assert: object пережил roundtrip; у канонической ноды поле не пишется.
        Assert.True(parsed.IsSuccess);
        Assert.Equal("as-s1a", parsed.Value["s1/s1a"].Object);
        Assert.Null(parsed.Value["s1/s1b"].Object);
        Assert.DoesNotContain("\"object\"", back.Replace("\"object\":\"as-s1a\"", ""));
    }

    [Fact]
    public void Parse_LegacyJsonWithoutObject_StillWorks()
    {
        // Arrange: существующие кластеры — JSON без object (обратная совместимость).
        var raw = "{\"s1/s1a\":{\"host\":\"h1\",\"pg\":15432,\"patroni\":18008,\"doorman\":16432}}";

        // Act
        var parsed = Portalloc.Parse("shop", raw);

        // Assert
        Assert.True(parsed.IsSuccess);
        Assert.Null(parsed.Value["s1/s1a"].Object);
    }
}
