using PgWorker.Etcd.S3;
using Shared.Etcd.Client;

namespace PgWorker.UnitTests.Etcd;

// Модель пула S3 (t36, arch/23 §2/§8): парсинг endpoints-строки/config/portalloc,
// имена инстансов — из ключей заявки nodes/<I>/state|placement (portalloc —
// только адреса).
public class S3PoolModelTests
{
    // AAA: endpoints-строка → упорядоченные пары host:port (порядок = порядок чтения)
    [Fact]
    public void ParseEndpoints_OrderPreserved()
    {
        var parsed = S3PoolModel.ParseEndpoints("h1:18001,h2:18002");
        parsed.Should().Equal("h1:18001", "h2:18002");
    }

    // AAA: битые элементы пропускаются без исключения (unknownKeys-толерантность)
    [Fact]
    public void ParseEndpoints_ToleratesGarbage()
    {
        var parsed = S3PoolModel.ParseEndpoints("h1:18001,,noport,x:нечисло");
        parsed.Should().Equal("h1:18001");
    }

    // AAA: config-JSON без state — Active-пул (state отсутствует = Active, arch/23 §2)
    [Fact]
    public void ParseConfig_ActiveCluster_HasNoToRemove()
    {
        var cfg = S3PoolModel.ParseConfig(
            """{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1760156800}""");
        cfg.Should().NotBeNull();
        cfg!.SecretProfile.Should().Be("backups-main");
        cfg.ToRemove.Should().BeFalse();
    }

    // AAA: config с state=TO_REMOVE — ToRemove=true (валидация policy-API)
    [Fact]
    public void ParseConfig_ToRemove()
    {
        var cfg = S3PoolModel.ParseConfig(
            """{"platform":"owns3","nodes":0,"secret_profile":"p","created_unix":1,"state":"TO_REMOVE"}""");
        cfg.Should().NotBeNull();
        cfg!.ToRemove.Should().BeTrue();
    }

    // AAA: portalloc — только адрес по имени; лишние/битые узлы молча пропускаются
    [Fact]
    public void ParsePortAlloc_ResolvesNodeName()
    {
        var map = S3PoolModel.ParsePortAlloc(
            """{"east1":{"host":"h1","s3":18001},"east2":{"host":"h2","s3":18002}}""");
        map["east1"].Should().Be(("h1", 18001));
        map.Count.Should().Be(2);
    }

    // AAA: имена инстансов — из ключей nodes/<I>/state И nodes/<I>/placement
    // (union; инстанс с placement без state — заявлен, валиден)
    [Fact]
    public void InstanceNames_UnionOfStateAndPlacementKeys()
    {
        var names = S3PoolModel.InstanceNames(
        [
            new Kv("/s3/clusters/b/nodes/east1/state", "\"RUNNING\"", 1),
            new Kv("/s3/clusters/b/nodes/east2/placement", """{"docker_host":"h2"}""", 2),
            new Kv("/s3/clusters/b/endpoints", "h1:1", 3),
        ]);
        names.Should().Equal("east1", "east2");
    }
}
