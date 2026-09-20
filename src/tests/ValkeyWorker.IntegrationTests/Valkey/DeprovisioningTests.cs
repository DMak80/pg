using FluentAssertions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// Deprovisioning реального контейнера (spec §6.2 сценарий 3): TO_REMOVE-сид →
// DeprovisioningProcess → контейнера нет, префиксы домена и координации пусты,
// portalloc снят.
[Collection(ValkeyClusterCollection.Name)]
public class DeprovisioningTests(ValkeyClusterFixture fx)
{
    [Fact]
    public async Task ToRemove_Демонтаж_КонтейнераНетEtcdЧист()
    {
        // Arrange: прогнанный provisioning → RUNNING-кластер с контейнером.
        var cluster = fx.Cluster("dep");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        var provision = fx.NewProvisioning(claims, fx.NewJournal(),
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex(), fx.NewSecretEnsurer());
        (await provision.TickAsync(await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();

        // Заявка TO_REMOVE + тик демонтажа.
        await fx.PutAsync($"/valkey/clusters/{cluster}/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"TO_REMOVE"}""");
        var deprovision = fx.NewDeprovisioning(claims, fx.NewJournal());
        var result = await deprovision.TickAsync((await fx.RequireSnapshotAsync(cluster))!, TestContext.Current.CancellationToken);

        // Assert: успех; контейнера нет; TLS-volume vwk-<C>-tls снят (X1, t06);
        // префиксы домена и координации пусты.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        (await fx.Driver.ListNodeObjectsAsync(cluster, TestContext.Current.CancellationToken))
            .Value.Should().BeEmpty();
        (await fx.Driver.GetTlsArchiveAsync(cluster, ValkeyClusterFixture.DockerHost, fx.Options.NodeImage, TestContext.Current.CancellationToken))
            .Value.Should().BeNull("TLS-volume удалён демонтажем (X1)");
        foreach (var prefix in new[]
                 {
                     $"/valkey/clusters/{cluster}/",
                     $"/valkeyworker/claims/{cluster}",
                     $"/valkeyworker/portalloc/{cluster}",
                     $"/valkeyworker/rotations/{cluster}",
                 })
        {
            var range = await fx.Gateway.RangeAsync(
                fx.Endpoint, prefix, TestContext.Current.CancellationToken);
            range.Value.Should().BeEmpty($"{prefix} пуст после демонтажа");
        }

        // work/<C> пуст ПОСЛЕ чистки (финальной journal-записи нет: «done»
        // воскресил бы удалённый ключ — arch/21 §5 B, образец kfw).
        (await fx.GetAsync($"/valkeyworker/work/{cluster}")).Should().BeNull("координация <C> чиста");
    }
}
