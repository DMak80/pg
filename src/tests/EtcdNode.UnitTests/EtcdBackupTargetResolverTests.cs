using EtcdNode.Core;
using Shared.Core;
using Shared.Etcd.Client;

namespace EtcdNode.UnitTests;

// Юниты резолвера цели DR-выгрузки на фейковом IEtcdGateway (словарь):
// каждый отказ — Failed с маркером EtcdBackupMarkers.Missing в сообщении,
// валидный контур — Success с http-endpoints, кредами backups и константным
// bucket. Битые элементы endpoints проверяются через ResolveAsync
// (ParseEndpoints/ParseBackupsSecrets — internal другой сборки).
public class EtcdBackupTargetResolverTests
{
    private const string Cluster = "c1";

    private static readonly string ConfigKey = $"/s3/clusters/{Cluster}/config";
    private static readonly string EndpointsKey = $"/s3/clusters/{Cluster}/endpoints";
    private static readonly string SecretsKey = $"/s3/clusters/{Cluster}/secrets";

    // secrets-JSON с записью ключа backups (access/secret непустые).
    private const string SecretsBackups =
        """{"access_keys":[{"name":"backups","access_key":"ak-1","secret_key":"sk-1"}]}""";

    private static EtcdBackupTargetResolver Resolver(Dictionary<string, string> kv)
        => new(new Fakes.FakeEtcdGateway(kv), ["http://localhost:2379"]);

    [Fact]
    public async Task Resolve_EmptyClustersPrefix_Failed_WithMissingMarker()
    {
        // Arrange — /s3/clusters/ пуст
        var resolver = Resolver(new Dictionary<string, string>());

        // Act
        var result = await resolver.ResolveAsync(CancellationToken.None);

        // Assert — отказ с маркером etcd-backup-missing
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain(EtcdBackupMarkers.Missing);
    }

    [Fact]
    public async Task Resolve_ConfigInToRemoveState_Failed_WithMissingMarker()
    {
        // Arrange — единственный контур с etcd_backup=true в состоянии TO_REMOVE
        var kv = new Dictionary<string, string>
        {
            [ConfigKey] = """{"etcd_backup":true,"state":"TO_REMOVE"}""",
        };
        var resolver = Resolver(kv);

        // Act
        var result = await resolver.ResolveAsync(CancellationToken.None);

        // Assert — отказ с маркером
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain(EtcdBackupMarkers.Missing);
    }

    [Fact]
    public async Task Resolve_SecretsWithoutBackupsKey_Failed_WithMissingMarker()
    {
        // Arrange — secrets без записи name=backups (битые креды)
        var kv = new Dictionary<string, string>
        {
            [ConfigKey] = """{"etcd_backup":true}""",
            [EndpointsKey] = "h1:9000",
            [SecretsKey] = """{"access_keys":[{"name":"other","access_key":"a","secret_key":"s"}]}""",
        };
        var resolver = Resolver(kv);

        // Act
        var result = await resolver.ResolveAsync(CancellationToken.None);

        // Assert — отказ с маркером
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain(EtcdBackupMarkers.Missing);
    }

    [Fact]
    public async Task Resolve_MissingEndpointsKey_Failed_WithMissingMarker()
    {
        // Arrange — ключа /s3/clusters/<C>/endpoints нет
        var kv = new Dictionary<string, string>
        {
            [ConfigKey] = """{"etcd_backup":true}""",
            [SecretsKey] = SecretsBackups,
        };
        var resolver = Resolver(kv);

        // Act
        var result = await resolver.ResolveAsync(CancellationToken.None);

        // Assert — отказ с маркером
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain(EtcdBackupMarkers.Missing);
    }

    [Fact]
    public async Task Resolve_ValidContour_Success_EndpointsCredentialsBucket()
    {
        // Arrange — валидный контур: config с etcd_backup=true, endpoints, secrets
        var kv = new Dictionary<string, string>
        {
            [ConfigKey] = """{"etcd_backup":true}""",
            [EndpointsKey] = "h1:9000,h2:9001",
            [SecretsKey] = SecretsBackups,
        };
        var resolver = Resolver(kv);

        // Act
        var result = await resolver.ResolveAsync(CancellationToken.None);

        // Assert — endpoints с http-префиксом, креды из backups, bucket — константа
        result.IsSuccess.Should().BeTrue();
        var target = result.Value;
        target.Endpoints.Should().Equal("http://h1:9000", "http://h2:9001");
        target.AccessKey.Should().Be("ak-1");
        target.SecretKey.Should().Be("sk-1");
        target.Bucket.Should().Be(Shared.Core.EtcdSnapshotBucket.Name);
    }

    [Fact]
    public async Task Resolve_BrokenEndpointsEntries_OnlyReadableSurvive()
    {
        // Arrange — валидный контур с битыми элементами endpoints:
        // «h1:» — пустой порт, «h2:abc» — нечисловой порт, « » — пустой элемент
        var kv = new Dictionary<string, string>
        {
            [ConfigKey] = """{"etcd_backup":true}""",
            [EndpointsKey] = "h1:,h2:abc, ,h3:9000",
            [SecretsKey] = SecretsBackups,
        };
        var resolver = Resolver(kv);

        // Act
        var result = await resolver.ResolveAsync(CancellationToken.None);

        // Assert — выжил только читаемый элемент с http-префиксом
        result.IsSuccess.Should().BeTrue();
        result.Value.Endpoints.Should().ContainSingle().Which.Should().Be("http://h3:9000");
    }
}
