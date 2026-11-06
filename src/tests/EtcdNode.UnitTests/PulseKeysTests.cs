using EtcdNode.Core;

namespace EtcdNode.UnitTests;

// Проверка канонического списка пульсов (arch/etcdnode/02-snapshots.md §3.1):
// какие ключи не триггерят внеочередной DR-слепок.

public class PulseKeysTests
{
    [Fact]
    public void ServicePrefix_IsPulse()
    {
        // Arrange: ключ Patroni-DCS под /service/
        var key = "/service/shop-1/leader";

        // Act
        var isPulse = PulseKeys.IsPulse(key);

        // Assert: весь префикс — пульс
        isPulse.Should().BeTrue();
    }

    [Fact]
    public void HaMasterKey_IsPulse()
    {
        // Arrange: мастер-ключ HA-контура
        var key = "/clusters/shop/shards/s1/master";

        // Act
        var isPulse = PulseKeys.IsPulse(key);

        // Assert
        isPulse.Should().BeTrue();
    }

    [Theory]
    [InlineData("/clusters/shop/shards/s1/routing")]
    [InlineData("/service")]
    [InlineData("/clusters/shop/shards/s1")]
    [InlineData("/clusters/shop/shards/s1/master/x")]
    [InlineData("/pgworker/work")]
    public void OtherKeys_NotPulse(string key)
    {
        // Arrange: ключи вне канонических пульсов

        // Act
        var isPulse = PulseKeys.IsPulse(key);

        // Assert: всё прочее — важное
        isPulse.Should().BeFalse();
    }
}
