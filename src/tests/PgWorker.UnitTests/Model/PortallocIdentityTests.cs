using PgWorker.Core.Model;
using Xunit;

namespace PgWorker.UnitTests.Model;

// PortallocIdentity (t15 ревизия 3, arch/14 §2.4): при заданном ключе
// PgWorker:Docker:ScrapeNetwork каноническим записям (без object) дописываются
// alias = pgw-<C>-<X>-<n> и net = имя сети; чистая, идемпотентная функция.
public class PortallocIdentityTests
{
    // AAA: каноническая нода получает alias из ключа "<X>/<n>" и net из ключа конфига.
    [Fact]
    public void Decorate_CanonicalNode_AddsAliasNet()
    {
        // Arrange: portalloc кластера c1 без полей сетевой идентичности.
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["s1/s1a"] = new("h1", new NodePorts(15432, 18008, 16432)),
        };

        // Act
        var decorated = PortallocIdentity.Decorate(addresses, "c1", "pgw-metrics");

        // Assert: alias — полное docker-имя ноды (дефис вместо «/»), net — имя сети.
        Assert.Equal("pgw-c1-s1-s1a", decorated["s1/s1a"].ScrapeAlias);
        Assert.Equal("pgw-metrics", decorated["s1/s1a"].ScrapeNetwork);
    }

    // AAA: усыновлённая нода (object) — чужой контейнер (R9): без attach и без полей.
    [Fact]
    public void Decorate_AdoptedObject_Skipped()
    {
        // Arrange: object-запись усыновлённой ноды.
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["s1/s1a"] = new("h1", new NodePorts(15432, 18008, 0), "as-s1a"),
        };

        // Act
        var decorated = PortallocIdentity.Decorate(addresses, "c1", "pgw-metrics");

        // Assert: запись не тронута — генератор строит advertised-таргет.
        Assert.Null(decorated["s1/s1a"].ScrapeAlias);
        Assert.Null(decorated["s1/s1a"].ScrapeNetwork);
    }

    // AAA: повторный Decorate не меняет JSON — точки записи идемпотентны.
    [Fact]
    public void Decorate_Idempotent()
    {
        // Arrange: словарь записей.
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["s1/s1a"] = new("h1", new NodePorts(15432, 18008, 16432)),
        };

        // Act: двойной Decorate.
        var once = Portalloc.Serialize(PortallocIdentity.Decorate(addresses, "c1", "pgw-metrics"));
        var twice = Portalloc.Serialize(PortallocIdentity.Decorate(
            PortallocIdentity.Decorate(addresses, "c1", "pgw-metrics"), "c1", "pgw-metrics"));

        // Assert: JSON равен — вечной перезаписи portalloc нет.
        Assert.Equal(once, twice);
    }

    // AAA: ключ ScrapeNetwork пуст/null — словарь без изменений (дефолт поставки:
    // ни attach, ни полей — поведение бинарно базе).
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void Decorate_EmptyKey_ReturnsAsIs(string? scrapeNetwork)
    {
        // Arrange: записи без полей, ключ не задан.
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["s1/s1a"] = new("h1", new NodePorts(15432, 18008, 16432)),
        };

        // Act
        var decorated = PortallocIdentity.Decorate(addresses, "c1", scrapeNetwork);

        // Assert: поля не появились.
        Assert.Null(decorated["s1/s1a"].ScrapeAlias);
        Assert.Null(decorated["s1/s1a"].ScrapeNetwork);
    }
}
