using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Provisioning.Processes;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.Etcd;

// Ensure rest_password с ЖИВЫМ etcd (t22, arch/14 §4 гр.1): седьмой ключ
// put-if-absent, повторный ensure не меняет значения, пустое значение
// добирается txn. Канон ClusterSecretEnsurer — живой контракт etcd.
[Collection(NonE2eCollection.Name)]
public class RestPasswordEnsureEtcdTests(EtcdFixture fixture)
{
    private EtcdGateway Gateway => fixture.Gateway;

    private string Endpoint => fixture.Endpoint;

    private static ClusterConfig Config(string cluster)
        => new(cluster, 2, cluster, null, ClusterState.Active);

    [Fact]
    public async Task Ensure_EmptyPrefix_PutsSeventhKeyPutIfAbsent()
    {
        // Arrange: чистый префикс кластера (guid-имя).
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"rst{Guid.NewGuid():N}".ToLowerInvariant()[..16];

        // Act: ensure четвёрки/семёрки на живом etcd.
        var sut = new ClusterSecretEnsurer(Gateway, [Endpoint]);
        var result = await sut.EnsureAsync(cluster, Config(cluster), ct);

        // Assert: ключ rest_password появился (32 [A-Za-z0-9]) и в кредлах.
        result.IsSuccess.Should().BeTrue(result.Error?.ToString());
        var key = (await Gateway.GetAsync(Endpoint, $"/clusters/{cluster}/rest_password", ct)).Value;
        key.Should().NotBeNull();
        key!.Value.Should().MatchRegex("^[A-Za-z0-9]{32}$");
        result.Value!.RestPassword.Should().Be(key.Value);
    }

    [Fact]
    public async Task Ensure_ExistingKey_NotOverwritten()
    {
        // Arrange: ключ уже записан (внешний etcdctl).
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"rst{Guid.NewGuid():N}".ToLowerInvariant()[..16];
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/rest_password",
            "External0Pass00000000000000000000Z", null, ct);

        // Act: повторный ensure.
        var sut = new ClusterSecretEnsurer(Gateway, [Endpoint]);
        var result = await sut.EnsureAsync(cluster, Config(cluster), ct);

        // Assert: put-if-absent — значение не изменилось, Success.
        result.IsSuccess.Should().BeTrue();
        var key = (await Gateway.GetAsync(Endpoint, $"/clusters/{cluster}/rest_password", ct)).Value;
        key!.Value.Should().Be("External0Pass00000000000000000000Z");
        result.Value!.RestPassword.Should().Be("External0Pass00000000000000000000Z");
    }

    [Fact]
    public async Task Ensure_EmptyValueKey_RefilledByTxn()
    {
        // Arrange: ключ с ПУСТЫМ значением (битое состояние) — добирается txn.
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"rst{Guid.NewGuid():N}".ToLowerInvariant()[..16];
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/rest_password", " ", null, ct);

        // Act
        var sut = new ClusterSecretEnsurer(Gateway, [Endpoint]);
        var result = await sut.EnsureAsync(cluster, Config(cluster), ct);

        // Assert: пустое значение заменено сгенерированным.
        result.IsSuccess.Should().BeTrue();
        var key = (await Gateway.GetAsync(Endpoint, $"/clusters/{cluster}/rest_password", ct)).Value;
        key!.Value.Should().MatchRegex("^[A-Za-z0-9]{32}$");
    }
}
