using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// Converge D (spec §6.2 сценарий 4): мутация config в etcd → CONFIG SET
// без рестарта контейнера (Id до/после равен — docker inspect).
[Collection(ValkeyClusterCollection.Name)]
public class ConvergeTests(ValkeyClusterFixture fx)
{
    [Fact]
    public async Task МутацияConfig_ConfigSetБезПересоздания()
    {
        // Arrange: RUNNING-кластер.
        var cluster = fx.Cluster("cnv");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        var provision = fx.NewProvisioning(claims, fx.NewJournal(),
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex(), fx.NewSecretEnsurer());
        (await provision.TickAsync(await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();

        var endpoints = (await fx.GetAsync($"/valkey/clusters/{cluster}/endpoints"))!;
        var port = int.Parse(endpoints.Split(':')[1]);
        var adminPassword = (await fx.GetAsync($"/valkey/clusters/{cluster}/admin_password"))!;
        var containerBefore = ContainerId($"vwk-{cluster}-node1");

        // Act: мутация config в etcd (как PUT /config) + тик конвергера.
        await fx.PutAsync($"/valkey/clusters/{cluster}/config",
            """{"nodes":1,"maxmemory_bytes":268435456,"maxmemory_policy":"volatile-ttl","created_unix":1756500000}""");
        var converger = fx.NewConverger();
        var result = await converger.TickAsync((await fx.RequireSnapshotAsync(cluster))!, TestContext.Current.CancellationToken);

        // Assert: CONFIG GET = новое значение; контейнер НЕ пересоздан.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var maxmemory = RespProbe.Execute("localhost", port, "admin", adminPassword,
            "CONFIG", "GET", "maxmemory");
        maxmemory.Value.Should().Contain("268435456");
        var policy = RespProbe.Execute("localhost", port, "admin", adminPassword,
            "CONFIG", "GET", "maxmemory-policy");
        policy.Value.Should().Contain("volatile-ttl");
        ContainerId($"vwk-{cluster}-node1").Should().Be(containerBefore,
            "converge D применяет CONFIG SET без рестартов");
    }

    private static string ContainerId(string name)
    {
        var startInfo = new System.Diagnostics.ProcessStartInfo("docker", $"inspect --format {{{{.Id}}}} {name}")
        {
            RedirectStandardOutput = true,
        };
        using var process = System.Diagnostics.Process.Start(startInfo)!;
        var id = process.StandardOutput.ReadToEnd().Trim();
        process.WaitForExit();
        return id;
    }
}
