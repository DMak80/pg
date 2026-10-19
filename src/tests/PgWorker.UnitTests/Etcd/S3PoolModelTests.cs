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
        var parsed = S3PoolModel.ParseEndpoints("h1:18001,,noport,x:notanumber");
        parsed.Should().Equal("h1:18001");
    }

    // AAA: config с secret_profile (поле игнорируется, не читается) —
    // Active-пул, ToRemove=false (state отсутствует = Active, arch/23 §2)
    [Fact]
    public void ParseConfig_ActiveCluster_HasNoToRemove()
    {
        var cfg = S3PoolModel.ParseConfig(
            """{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1760156800}""");
        cfg.Should().NotBeNull();
        cfg!.ToRemove.Should().BeFalse();
    }

    // AAA: config без secret_profile — ToRemove-семантика прежняя:
    // state=TO_REMOVE → true (валидация policy-API)
    [Fact]
    public void ParseConfig_ToRemove()
    {
        var cfg = S3PoolModel.ParseConfig(
            """{"platform":"owns3","nodes":0,"created_unix":1,"state":"TO_REMOVE"}""");
        cfg.Should().NotBeNull();
        cfg!.ToRemove.Should().BeTrue();
    }

    // AAA: валидный secrets-JSON (arch/23 §2.1) — креды элемента
    // access_keys с name=="backups"
    [Fact]
    public void ParseSecrets_BackupsKey_ReturnsCredentials()
    {
        var parsed = S3PoolModel.ParseSecrets(
            """{"root_user":"s3w-c-root","root_password":"pw","access_keys":[{"name":"backups","access_key":"ak1","secret_key":"sk1","policy":"read-write"},{"name":"panel","access_key":"ak2","secret_key":"sk2","policy":"read-only"}]}""");
        parsed.Should().NotBeNull();
        parsed!.AccessKey.Should().Be("ak1");
        parsed.SecretKey.Should().Be("sk1");
    }

    // AAA: access_keys без элемента backups → null
    [Fact]
    public void ParseSecrets_NoBackupsKey_ReturnsNull()
    {
        var parsed = S3PoolModel.ParseSecrets(
            """{"root_user":"s3w-c-root","root_password":"pw","access_keys":[{"name":"panel","access_key":"ak2","secret_key":"sk2","policy":"read-only"}]}""");
        parsed.Should().BeNull();
    }

    // AAA: битый JSON → null (не краш, толерантность parseError-пути)
    [Fact]
    public void ParseSecrets_BrokenJson_ReturnsNull()
    {
        var parsed = S3PoolModel.ParseSecrets("""{"access_keys":[{"name":"backups"}""");
        parsed.Should().BeNull();
    }

    // AAA: пустые access_key/secret_key у элемента backups → null
    [Fact]
    public void ParseSecrets_EmptyValues_ReturnsNull()
    {
        var parsed = S3PoolModel.ParseSecrets(
            """{"access_keys":[{"name":"backups","access_key":"","secret_key":"","policy":"read-write"}]}""");
        parsed.Should().BeNull();
    }

    // AAA: лишние ключи (top-level и внутри элемента) не мешают разбору
    [Fact]
    public void ParseSecrets_ExtraKeys_Tolerated()
    {
        var parsed = S3PoolModel.ParseSecrets(
            """{"extra_top":123,"root_user":"u","root_password":"p","access_keys":[{"name":"backups","access_key":"ak1","secret_key":"sk1","policy":"read-write","comment":"x"}],"after":true}""");
        parsed.Should().NotBeNull();
        parsed!.AccessKey.Should().Be("ak1");
        parsed.SecretKey.Should().Be("sk1");
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
