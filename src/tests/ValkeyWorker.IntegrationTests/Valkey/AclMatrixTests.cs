using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// ACL-матрица реальной ноды (spec §6.2 сценарий 2): default off (AUTH-отказ);
// app — read/write, БЕЗ админ-команд; admin — +@all (PING, CONFIG SET).
[Collection(ValkeyClusterCollection.Name)]
public class AclMatrixTests(ValkeyClusterFixture fx)
{
    [Fact]
    public async Task Матрица_DefaultAppAdmin()
    {
        // Arrange: поднимаем кластер provisioning-процессом.
        var cluster = fx.Cluster("acl");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        var process = fx.NewProvisioning(claims, fx.NewJournal(),
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex(), fx.NewSecretEnsurer());
        (await process.TickAsync(await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();

        var adminPassword = (await fx.GetAsync($"/valkey/clusters/{cluster}/admin_password"))!;
        var appPassword = (await fx.GetAsync($"/valkey/clusters/{cluster}/app_password"))!;
        var endpoints = (await fx.GetAsync($"/valkey/clusters/{cluster}/endpoints"))!;
        var port = int.Parse(endpoints.Split(':')[1]);
        var caPem = (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_pem"))!;

        // Assert 1: default без пароля — AUTH-отказ (вход только по named-пользователям).
        var defaultProbe = RespProbe.ExecuteTls("localhost", port, "default", "", caPem);
        defaultProbe.Ok.Should().BeFalse();
        defaultProbe.Error.Should().Contain("AUTH");

        // Assert 2: app — SET/GET работает.
        var set = RespProbe.ExecuteTls("localhost", port, "app", appPassword, caPem, "SET", "acl:key", "value1");
        set.Ok.Should().BeTrue(set.Error);
        var get = RespProbe.ExecuteTls("localhost", port, "app", appPassword, caPem, "GET", "acl:key");
        get.Ok.Should().BeTrue(get.Error);
        get.Value.Should().Be("value1");

        // Assert 3: app — админ-команды отклонены (ACL LIST, CONFIG GET).
        var aclList = RespProbe.ExecuteTls("localhost", port, "app", appPassword, caPem, "ACL", "LIST");
        aclList.Ok.Should().BeFalse();
        var configGet = RespProbe.ExecuteTls("localhost", port, "app", appPassword, caPem, "CONFIG", "GET", "maxmemory");
        configGet.Ok.Should().BeFalse();

        // Assert 4: admin — +@all (PING, CONFIG SET/GET).
        var adminPing = RespProbe.ExecuteTls("localhost", port, "admin", adminPassword, caPem, "PING");
        adminPing.Ok.Should().BeTrue(adminPing.Error);
        var adminConfigSet = RespProbe.ExecuteTls("localhost", port, "admin", adminPassword, caPem,
            "CONFIG", "SET", "maxmemory", "536870912");
        adminConfigSet.Ok.Should().BeTrue(adminConfigSet.Error);
        var adminConfigGet = RespProbe.ExecuteTls("localhost", port, "admin", adminPassword, caPem,
            "CONFIG", "GET", "maxmemory");
        adminConfigGet.Ok.Should().BeTrue(adminConfigGet.Error);
        adminConfigGet.Value.Should().Contain("536870912");
    }
}
