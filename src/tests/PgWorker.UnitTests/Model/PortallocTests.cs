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

    // AAA (t15 ревизия 3): сетевая идентичность записи — alias (docker-имя ноды,
    // резолвится DNS сети контура) и net (имя сети, информационное); сериализуются
    // только при наличии (WhenWritingNull).
    [Fact]
    public void Serialize_WithScrapeFields_WritesAliasNet()
    {
        // Arrange: каноническая нода с сетевой идентичностью (ключ ScrapeNetwork).
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["s1/s1a"] = new("h1", new NodePorts(15432, 18008, 16432),
                ScrapeAlias: "pgw-c1-s1-s1a", ScrapeNetwork: "pgw-metrics"),
        };

        // Act
        var json = Portalloc.Serialize(addresses);

        // Assert: оба поля в контрактом формате.
        Assert.Contains("\"alias\":\"pgw-c1-s1-s1a\"", json);
        Assert.Contains("\"net\":\"pgw-metrics\"", json);
    }

    // AAA (t15 ревизия 3): RMW-цикл parse→serialize БЕЗ Decorate сохраняет поля —
    // это и есть preserve remove-shard/чужих read-modify-write (симметричная
    // сериализация, arch/14 §2.4).
    [Fact]
    public void RoundTrip_PreservesScrapeFields()
    {
        // Arrange: portalloc с alias/net (пишет decorate точки записи).
        var raw = """
            {"s1/s1a":{"host":"h1","pg":15432,"patroni":18008,"doorman":16432,
             "alias":"pgw-c1-s1-s1a","net":"pgw-metrics"}}
            """;

        // Act
        var parsed = Portalloc.Parse("c1", raw);
        var back = Portalloc.Serialize(parsed.Value);

        // Assert: поля пережили цикл без Decorate.
        Assert.True(parsed.IsSuccess);
        Assert.Equal("pgw-c1-s1-s1a", parsed.Value["s1/s1a"].ScrapeAlias);
        Assert.Equal("pgw-metrics", parsed.Value["s1/s1a"].ScrapeNetwork);
        Assert.Contains("\"alias\":\"pgw-c1-s1-s1a\"", back);
        Assert.Contains("\"net\":\"pgw-metrics\"", back);
    }

    // AAA (t15 ревизия 3): ключ ScrapeNetwork не задан — записи НЕ содержат полей
    // (бинарно базе: поведение без ключа идентично).
    [Fact]
    public void Serialize_WithoutScrapeFields_NoFields()
    {
        // Arrange: адрес без сетевой идентичности (ключ не задан).
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["s1/s1a"] = new("h1", new NodePorts(15432, 18008, 16432)),
        };

        // Act
        var json = Portalloc.Serialize(addresses);

        // Assert: полей нет вовсе.
        Assert.DoesNotContain("\"alias\"", json);
        Assert.DoesNotContain("\"net\"", json);
    }

    // AAA (t15 ревизия 3): Parse толерантен к записям с alias/net (переходный
    // контур: генератор и панель читают один ключ).
    [Fact]
    public void Parse_LegacyJsonWithAlias_StillWorks()
    {
        // Arrange: запись уже несёт поля сетевой идентичности.
        var raw = "{\"s1/s1a\":{\"host\":\"h1\",\"pg\":15432,\"patroni\":18008,\"doorman\":16432," +
                  "\"alias\":\"pgw-c1-s1-s1a\",\"net\":\"pgw-metrics\"}}";

        // Act
        var parsed = Portalloc.Parse("c1", raw);

        // Assert: разбор успешен, поля прочитаны.
        Assert.True(parsed.IsSuccess);
        Assert.Equal("pgw-c1-s1-s1a", parsed.Value["s1/s1a"].ScrapeAlias);
        Assert.Equal("pgw-metrics", parsed.Value["s1/s1a"].ScrapeNetwork);
    }
}
