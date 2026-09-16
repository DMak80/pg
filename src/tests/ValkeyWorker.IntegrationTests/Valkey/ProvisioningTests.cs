using FluentAssertions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// Provisioning реального valkey/valkey:9.1.2 (spec §6.2 сценарий 1): сид →
// тики ProvisioningProcess → контейнер vwk-<C>-node1 (persistence off в Cmd),
// PING admin → PONG, endpoints, state=RUNNING, config без state, креды 32 симв.
[Collection(ValkeyClusterCollection.Name)]
public class ProvisioningTests(ValkeyClusterFixture fx)
{
    [Fact]
    public async Task Сид_ТикиProvisioning_КонтейнерИДискавери()
    {
        // Arrange: сид NOT_INITIALIZED + клэйм.
        var cluster = fx.Cluster("prov");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        (await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken)).Value.Should().BeTrue();
        var journal = fx.NewJournal();
        var process = fx.NewProvisioning(claims, journal,
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex(),
            fx.NewSecretEnsurer());

        // Act: один тик (V0–V5 внутри).
        var snap = await fx.SnapshotAsync(cluster);
        var result = await process.TickAsync(snap!, TestContext.Current.CancellationToken);

        // Assert: процесс done.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);

        // Контейнер: имя vwk-<C>-node1, образ 9.1.2, Cmd с persistence off.
        var objects = await fx.Driver.ListNodeObjectsAsync(cluster, TestContext.Current.CancellationToken);
        objects.Value.Should().ContainSingle().Which.Should().Be($"vwk-{cluster}-node1");
        var args = await fx.Driver.NodeArgsAsync(cluster, "node1", TestContext.Current.CancellationToken);
        args.Value.Should().NotBeNull();
        args.Value![0].Should().Be("valkey-server");
        var saveIndex = args.Value.ToList().IndexOf("--save");
        args.Value[saveIndex + 1].Should().Be("", "persistence off: --save \"\"");
        args.Value.Should().Contain("--appendonly").And.Contain("no");

        // Креды: 32 симв; PING admin-кредом → PONG.
        var adminPassword = await fx.GetAsync($"/valkey/clusters/{cluster}/admin_password");
        adminPassword.Should().HaveLength(32);
        var appPassword = await fx.GetAsync($"/valkey/clusters/{cluster}/app_password");
        appPassword.Should().HaveLength(32);
        var endpoints = await fx.GetAsync($"/valkey/clusters/{cluster}/endpoints");
        endpoints.Should().NotBeNull();
        var port = int.Parse(endpoints!.Split(':')[1]);
        var ping = RespProbe.Execute("localhost", port, "admin", adminPassword!, "PING");
        ping.Ok.Should().BeTrue(ping.Error);

        // Дискавери: endpoints = localhost:<фактический порт>; state=RUNNING;
        // config без state.
        endpoints.Should().StartWith($"{ValkeyClusterFixture.AdvertisedClientHost}:");
        (await fx.GetAsync($"/valkey/clusters/{cluster}/nodes/node1/state")).Should().Be("RUNNING");
        var config = await fx.GetAsync($"/valkey/clusters/{cluster}/config");
        config.Should().NotContain("state").And.Contain("maxmemory_bytes");
        // journal: provision/done
        (await fx.GetAsync($"/valkeyworker/work/{cluster}")).Should().Contain("done");
    }
}

// Один etcd + один docker-драйвер на Valkey-группу (кластеры RunTag-уникальны).
[CollectionDefinition(Name)]
public class ValkeyClusterCollection : ICollectionFixture<ValkeyClusterFixture>
{
    public const string Name = "valkey-cluster";
}
