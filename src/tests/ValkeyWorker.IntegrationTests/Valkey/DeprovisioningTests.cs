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

        // Assert: успех; контейнера нет; префиксы домена и координации пусты.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        (await fx.Driver.ListNodeObjectsAsync(cluster, TestContext.Current.CancellationToken))
            .Value.Should().BeEmpty();
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

        // work/<C> несёт journal done (фаза X3 пишется после чистки X2 — S6-трек).
        (await fx.GetAsync($"/valkeyworker/work/{cluster}")).Should().Contain("done");
    }
}
