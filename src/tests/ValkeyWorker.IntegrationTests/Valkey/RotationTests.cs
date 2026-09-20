using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// Ротация app-пароля (spec §6.2 сценарий 6): окно E1 (оба пароля валидны) →
// тик ротатора доигрывает E2/E3 → OLD отвергнут/NEW работает, заявка удалена,
// admin-кред не тронут.
[Collection(ValkeyClusterCollection.Name)]
public class RotationTests(ValkeyClusterFixture fx)
{
    [Fact]
    public async Task РотацияApp_ОкноИФинал()
    {
        // Arrange: RUNNING-кластер; заявка role=app.
        var cluster = fx.Cluster("rot");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        var provision = fx.NewProvisioning(claims, fx.NewJournal(),
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex(), fx.NewSecretEnsurer());
        (await provision.TickAsync(await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();

        var endpoints = (await fx.GetAsync($"/valkey/clusters/{cluster}/endpoints"))!;
        var port = int.Parse(endpoints.Split(':')[1]);
        var oldAdmin = (await fx.GetAsync($"/valkey/clusters/{cluster}/admin_password"))!;
        var oldApp = (await fx.GetAsync($"/valkey/clusters/{cluster}/app_password"))!;
        var caPem = (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_pem"))!;
        await fx.PutAsync($"/valkeyworker/rotations/{cluster}",
            """{"role":"app","requested_unix":1756500000,"requested_by":"test"}""");

        // Окно двух паролей: вручную E1 (ACL SETUSER app >NEW) — ОБА валидны.
        const string newApp = "NewAppPassword0123456789abcdef123456";
        var admin = new ValkeyWorker.Core.Valkey.ValkeyEndpoint("localhost", port, "admin", oldAdmin, caPem);
        var connection = new ValkeyWorker.Core.Valkey.ValkeyConnection(TimeSpan.FromSeconds(2));
        (await connection.AclSetUserAsync(admin, ["app", $">{newApp}"], TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        // Проба окна — SET (право app: +@read +@write; PING в эти категории не входит).
        var oldWorks = RespProbe.ExecuteTls("localhost", port, "app", oldApp, caPem, "SET", "rot:probe", "1");
        var newWorks = RespProbe.ExecuteTls("localhost", port, "app", newApp, caPem, "GET", "rot:probe");
        oldWorks.Ok.Should().BeTrue(oldWorks.Error);
        newWorks.Ok.Should().BeTrue(newWorks.Error);

        // Act: тик ротатора — доигрывает с E1 (journal-фаза null → полный цикл;
        // после E2 etcd NEW, после E3 OLD удалён).
        var rotator = fx.NewRotator(claims, fx.NewJournal());
        var result = await rotator.TickAsync((await fx.RequireSnapshotAsync(cluster))!, TestContext.Current.CancellationToken);

        // Assert: etcd содержит НОВЫЙ пароль ротатора (не OLD и не наш ручной),
        // заявка удалена; OLD отвергнут; коммитетed NEW работает; admin не тронут.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var committed = (await fx.GetAsync($"/valkey/clusters/{cluster}/app_password"))!;
        committed.Should().NotBe(oldApp).And.NotBe(newApp).And.HaveLength(32);
        (await fx.GetAsync($"/valkeyworker/rotations/{cluster}")).Should().BeNull();
        var oldRejected = RespProbe.ExecuteTls("localhost", port, "app", oldApp, caPem, "SET", "rot:probe2", "1");
        oldRejected.Ok.Should().BeFalse("OLD удалён фазой E3");
        var newAccepted = RespProbe.ExecuteTls("localhost", port, "app", committed, caPem, "SET", "rot:probe3", "1");
        newAccepted.Ok.Should().BeTrue(newAccepted.Error);
        var adminStillOk = RespProbe.ExecuteTls("localhost", port, "admin", oldAdmin, caPem, "PING");
        adminStillOk.Ok.Should().BeTrue("ротация app не трогает admin-кред");
    }
}
