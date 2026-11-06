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

    [Fact]
    public void AgentDomainPrefix_IsPulse()
    {
        // Arrange: служебные ключи домена самого агента (leader/instances/
        // snapshots) — статус-puts экспортёра не должны триггерить слепки
        var key = "/etcdnode/snapshots";

        // Act
        var isPulse = PulseKeys.IsPulse(key);

        // Assert
        isPulse.Should().BeTrue();
        PulseKeys.IsPulse("/etcdnode/leader").Should().BeTrue();
        PulseKeys.IsPulse("/etcdnode/instances/node1").Should().BeTrue();
    }

    [Theory]
    [InlineData("/clusters/shop/shards/s1/routing")]
    [InlineData("/service")]
    [InlineData("/clusters/shop/shards/s1")]
    [InlineData("/clusters/shop/shards/s1/master/x")]
    [InlineData("/pgworker/work")]
    [InlineData("/etcdnode")]
    public void OtherKeys_NotPulse(string key)
    {
        // Arrange: ключи вне канонических пульсов (в т.ч. «/etcdnode» без
        // завершающего слэша — префикс не совпадает)

        // Act
        var isPulse = PulseKeys.IsPulse(key);

        // Assert: всё прочее — важное
        isPulse.Should().BeFalse();
    }
}
