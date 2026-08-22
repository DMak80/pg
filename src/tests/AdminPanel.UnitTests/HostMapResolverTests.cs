using AdminPanel.Probes;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Резолвер адресов проб: точное совпадение host:port → override, иначе адрес
// без изменений; порт — часть ключа (arch/02 §6, spec §10.4).
public class HostMapResolverTests
{
    [Fact]
    public void Resolve_ExactMatch_Overrides()
    {
        // Arrange
        var map = new Dictionary<string, string> { ["s1a:8008"] = "127.0.0.1:8011" };

        // Act
        var resolved = HostMapResolver.Resolve(map, "s1a", 8008);

        // Assert
        resolved.Should().Be("127.0.0.1:8011");
    }

    [Fact]
    public void Resolve_NoMatch_Identity()
    {
        // Arrange
        var map = new Dictionary<string, string> { ["s1a:5432"] = "127.0.0.1:5433" };

        // Act
        var resolved = HostMapResolver.Resolve(map, "s1a", 8008);

        // Assert: нет точного совпадения — адрес из etcd используется как есть.
        resolved.Should().Be("s1a:8008");
    }

    [Fact]
    public void Resolve_EmptyMap_Identity()
    {
        // Arrange — прод: HostMap пуст (arch/01 §6).
        // Act
        var resolved = HostMapResolver.Resolve(new Dictionary<string, string>(), "pg1", 5432);

        // Assert
        resolved.Should().Be("pg1:5432");
    }

    [Fact]
    public void Resolve_DifferentPort_NotMatched()
    {
        // Arrange: карта знает другой порт того же хоста — порт часть ключа.
        var map = new Dictionary<string, string> { ["s1a:5432"] = "127.0.0.1:5433" };

        // Act
        var resolved = HostMapResolver.Resolve(map, "s1a", 5433);

        // Assert
        resolved.Should().Be("s1a:5433");
    }
}
