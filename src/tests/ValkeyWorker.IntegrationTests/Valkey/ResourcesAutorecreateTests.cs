using FluentAssertions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// Автоконверге лимитов (spec §6.2 сценарий 5 + §9.8): PUT resources → тик
// надзора → контейнер пересоздан с новыми лимитами (одно за тик), RUNNING.
[Collection(ValkeyClusterCollection.Name)]
public class ResourcesAutorecreateTests(ValkeyClusterFixture fx)
{
    [Fact]
    public async Task МутацияResources_ПересозданиеСНовымиЛимитами()
    {
        // Arrange: RUNNING-кластер (cpu 1/mem 1Gi).
        var cluster = fx.Cluster("res");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        var provision = fx.NewProvisioning(claims, fx.NewJournal(),
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex(), fx.NewSecretEnsurer());
        (await provision.TickAsync(await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();

        // Act: PUT resources (cpu=2, mem=2Gi) + тик надзора.
        await fx.PutAsync($"/valkey/clusters/{cluster}/nodes/node1/resources",
            """{"cpu":"2","mem":"2Gi","disk":"10Gi"}""");
        var portLock = fx.NewPortAllocLock(claims.InstanceId);
        var portIndex = fx.NewPortAllocIndex();
        var supervisor = fx.NewSupervisor(claims, fx.NewJournal(),
            fx.NewHealer(claims, fx.NewJournal(), portLock, portIndex));
        var result = await supervisor.TickAsync((await fx.RequireSnapshotAsync(cluster))!, TestContext.Current.CancellationToken);

        // Assert: контейнер пересоздан с новыми лимитами (inspect), ОДНО за тик.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var ensured = await fx.Driver.NodeResourcesAsync(cluster, "node1", TestContext.Current.CancellationToken);
        ensured.Value.Should().NotBeNull();
        ensured.Value!.CpuCores.Should().Be(2m);
        ensured.Value.MemoryBytes.Should().Be(2L * 1024 * 1024 * 1024);

        // state → PROVISIONING при пересоздании; следующий тик по PING → RUNNING.
        (await fx.GetAsync($"/valkey/clusters/{cluster}/nodes/node1/state")).Should().Be("PROVISIONING");
        var second = await supervisor.TickAsync((await fx.RequireSnapshotAsync(cluster))!, TestContext.Current.CancellationToken);
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        (await fx.GetAsync($"/valkey/clusters/{cluster}/nodes/node1/state")).Should().Be("RUNNING");
    }
}
