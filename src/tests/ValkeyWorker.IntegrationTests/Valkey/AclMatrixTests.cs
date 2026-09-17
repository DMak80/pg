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

        // Assert 1: default без пароля — AUTH-отказ (вход только по named-пользователям).
        var defaultProbe = RespProbe.Execute("localhost", port, "default", "");
        defaultProbe.Ok.Should().BeFalse();
        defaultProbe.Error.Should().Contain("AUTH");

        // Assert 2: app — SET/GET работает.
        var set = RespProbe.Execute("localhost", port, "app", appPassword, "SET", "acl:key", "value1");
        set.Ok.Should().BeTrue(set.Error);
        var get = RespProbe.Execute("localhost", port, "app", appPassword, "GET", "acl:key");
        get.Ok.Should().BeTrue(get.Error);
        get.Value.Should().Be("value1");

        // Assert 3: app — админ-команды отклонены (ACL LIST, CONFIG GET).
        var aclList = RespProbe.Execute("localhost", port, "app", appPassword, "ACL", "LIST");
        aclList.Ok.Should().BeFalse();
        var configGet = RespProbe.Execute("localhost", port, "app", appPassword, "CONFIG", "GET", "maxmemory");
        configGet.Ok.Should().BeFalse();

        // Assert 4: admin — +@all (PING, CONFIG SET/GET).
        var adminPing = RespProbe.Execute("localhost", port, "admin", adminPassword, "PING");
        adminPing.Ok.Should().BeTrue(adminPing.Error);
        var adminConfigSet = RespProbe.Execute("localhost", port, "admin", adminPassword,
            "CONFIG", "SET", "maxmemory", "536870912");
        adminConfigSet.Ok.Should().BeTrue(adminConfigSet.Error);
        var adminConfigGet = RespProbe.Execute("localhost", port, "admin", adminPassword,
            "CONFIG", "GET", "maxmemory");
        adminConfigGet.Ok.Should().BeTrue(adminConfigGet.Error);
        adminConfigGet.Value.Should().Contain("536870912");
    }
}
