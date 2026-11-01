using EtcdWorker.Etcd;

namespace EtcdWorker.UnitTests.Etcd;

// JSON /etcd/config (EtcdConfigJson): строгий serialize + толерантный parse.
public class EtcdConfigJsonTests
{
    [Fact]
    public void Serialize_ProducesExactJson()
    {
        // Act
        var json = EtcdConfigJson.Serialize(2, 123);

        // Assert — ровно контрактный формат, без пробелов
        json.Should().Be("""{"nodes":2,"created_unix":123}""");
    }

    [Fact]
    public void Parse_ValidJson_ReturnsConfig()
    {
        // Act
        var config = EtcdConfigJson.Parse("""{"nodes":2,"created_unix":123}""");

        // Assert — оба поля разобраны
        config.Should().NotBeNull();
        config!.Nodes.Should().Be(2);
        config.CreatedUnix.Should().Be(123);
    }

    [Fact]
    public void Parse_BrokenJson_ReturnsNull()
    {
        // Act
        var config = EtcdConfigJson.Parse("""{"nodes":2,""");

        // Assert — битый JSON толерантно превращается в null
        config.Should().BeNull();
    }

    [Fact]
    public void Parse_MissingCreatedUnix_ReturnsNull()
    {
        // Act
        var config = EtcdConfigJson.Parse("""{"nodes":2}""");

        // Assert — неполный конфиг (нет created_unix) → null
        config.Should().BeNull();
    }
}
