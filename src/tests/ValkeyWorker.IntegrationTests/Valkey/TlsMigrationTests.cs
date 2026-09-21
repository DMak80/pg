using FluentAssertions;
using ValkeyWorker.Provisioning.Processes;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// TlsMigrator на реальном valkey/valkey:9.1.2 (t06, spec §4.9): премиграционный
// plain-контейнер (аргументы старого канона, ca-ключей нет) → тик мигратора →
// контейнер пересоздан с каноническими TLS-args на ТОМ ЖЕ host-порту, volume
// vwk-<C>-tls записан, ca_pem/ca_key появились, PING по TLS отвечает, journal
// op=migrate-tls доведён до done; повторный тик — NotNeeded (контейнер не тронут).
[Collection(ValkeyClusterCollection.Name)]
public class TlsMigrationTests(ValkeyClusterFixture fx)
{
    [Fact]
    public async Task PlainCluster_MigratesToTls()
    {
        // Arrange: симуляция премиграционного кластера — заявка, portalloc,
        // контейнер СТАРЫМ каноном вручную (args без TLS-хвоста, без volume),
        // endpoints/state проставлены руками, ca-ключей НЕТ.
        var cluster = fx.Cluster("tlsmig");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        (await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken)).Value.Should().BeTrue();
        var ct = TestContext.Current.CancellationToken;

        // ensure секретов вручную (креды для args старого канона; CA не пишем).
        var ensured = await fx.NewSecretEnsurer().EnsureAsync(cluster, ct);
        ensured.IsSuccess.Should().BeTrue(ensured.Error?.Message);
        // Подмена: remove ca-ключей (эмуляция премиграционного etcd).
        await fx.DelAsync($"/valkey/clusters/{cluster}/ca_pem");
        await fx.DelAsync($"/valkey/clusters/{cluster}/ca_key");

        // portalloc руками (без V1-плана): порт через FreePortWindow.
        var port = fx.NextPort();
        await fx.PutAsync($"/valkeyworker/portalloc/{cluster}",
            "{\"node1\":{\"host\":\"local\",\"client\":" + port + "}}");
        // Контейнер старым каноном: args БЕЗ TLS-хвоста, TlsVolume: null.
        var plainArgs = NodeArgsBuilder.Build(
            536870912, "allkeys-lru", ensured.Value.AdminPassword, ensured.Value.AppPassword)
            .TakeWhile((arg, i) => arg != "--tls-port").ToArray();
        var created = await fx.Driver.EnsureNodeAsync(new(
            cluster, "node1", ValkeyClusterFixture.DockerHost, port, fx.Options.NodeImage, plainArgs,
            1m, 1024L * 1024 * 1024),
            ct);
        created.IsSuccess.Should().BeTrue(created.Error?.Message);
        await fx.PutAsync($"/valkey/clusters/{cluster}/endpoints", $"localhost:{port}");
        await fx.PutAsync($"/valkey/clusters/{cluster}/nodes/node1/state", "RUNNING");

        var idBefore = ContainerId($"vwk-{cluster}-node1");
        idBefore.Should().NotBeEmpty("plain-контейнер поднят руками");

        // Act: тик мигратора.
        var migrator = new TlsMigrator(
            fx.Gateway, [fx.Endpoint], fx.Driver, claims, fx.NewJournal(),
            fx.NewSecretEnsurer(), fx.NewTlsProvisioner(),
            new ValkeyWorker.Core.Valkey.ValkeyConnection(TimeSpan.FromSeconds(2)),
            fx.Options);
        var outcome = await migrator.RunAsync(await fx.RequireSnapshotAsync(cluster), ct);

        // Assert: InProgress; контейнер пересоздан (Id сменился) с TLS-args;
        // host-порт ТОТ ЖЕ; volume записан; ca_pem появился; PING по TLS;
        // journal op=migrate-tls → done.
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.Message);
        outcome.Value.Should().Be(TlsMigrator.MigrationOutcome.InProgress);
        var idAfter = ContainerId($"vwk-{cluster}-node1");
        idAfter.Should().NotBe(idBefore, "контейнер пересоздан (не рестартнут)");
        var args = (await fx.Driver.NodeArgsAsync(cluster, "node1", ct)).Value!;
        args.Should().Contain("--tls-port").And.Contain("--port");
        args[args.ToList().IndexOf("--port") + 1].Should().Be("0");
        var endpoints = (await fx.GetAsync($"/valkey/clusters/{cluster}/endpoints"))!;
        int.Parse(endpoints.Split(':')[1]).Should().Be(port, "portalloc не меняется");
        var tar = (await fx.Driver.GetTlsArchiveAsync(cluster, ValkeyClusterFixture.DockerHost, fx.Options.NodeImage, ct)).Value;
        tar.Should().NotBeNull();
        TarArchive.Read(tar!)
            .Keys.Should().BeEquivalentTo("node.crt", "node.key", "ca.pem");
        var caPem = (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_pem"))!;
        caPem.Should().Contain("BEGIN CERTIFICATE");
        var ping = RespProbe.ExecuteTls("localhost", port, "admin", ensured.Value.AdminPassword, caPem, "PING");
        ping.Ok.Should().BeTrue(ping.Error);
        var journal = (await fx.GetAsync($"/valkeyworker/work/{cluster}"))!;
        journal.Should().Contain("migrate-tls").And.Contain("done");

        // Повторный RunAsync — NotNeeded (контейнер не тронут — Id неизменен).
        var rerun = await migrator.RunAsync(await fx.RequireSnapshotAsync(cluster), ct);
        rerun.IsSuccess.Should().BeTrue(rerun.Error?.Message);
        rerun.Value.Should().Be(TlsMigrator.MigrationOutcome.NotNeeded);
        ContainerId($"vwk-{cluster}-node1").Should().Be(idAfter, "повторный детект — no-op");
    }

    [Fact]
    public async Task Deprovision_AfterMigration_CleansVolumeAndKeys()
    {
        // Arrange: plain-кластер → миграция (volume+ca-ключи) → демонтаж.
        var cluster = fx.Cluster("tlsdep");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        (await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken)).Value.Should().BeTrue();
        var ct = TestContext.Current.CancellationToken;

        var ensured = await fx.NewSecretEnsurer().EnsureAsync(cluster, ct);
        ensured.IsSuccess.Should().BeTrue(ensured.Error?.Message);
        await fx.DelAsync($"/valkey/clusters/{cluster}/ca_pem");
        await fx.DelAsync($"/valkey/clusters/{cluster}/ca_key");
        var port = fx.NextPort();
        await fx.PutAsync($"/valkeyworker/portalloc/{cluster}",
            "{\"node1\":{\"host\":\"local\",\"client\":" + port + "}}");
        var plainArgs = NodeArgsBuilder.Build(
            536870912, "allkeys-lru", ensured.Value.AdminPassword, ensured.Value.AppPassword)
            .TakeWhile((arg, i) => arg != "--tls-port").ToArray();
        (await fx.Driver.EnsureNodeAsync(new(
            cluster, "node1", ValkeyClusterFixture.DockerHost, port, fx.Options.NodeImage, plainArgs,
            1m, 1024L * 1024 * 1024),
            ct)).IsSuccess.Should().BeTrue();

        var migrator = new TlsMigrator(
            fx.Gateway, [fx.Endpoint], fx.Driver, claims, fx.NewJournal(),
            fx.NewSecretEnsurer(), fx.NewTlsProvisioner(),
            new ValkeyWorker.Core.Valkey.ValkeyConnection(TimeSpan.FromSeconds(2)),
            fx.Options);
        (await migrator.RunAsync(await fx.RequireSnapshotAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();
        (await fx.Driver.GetTlsArchiveAsync(cluster, ValkeyClusterFixture.DockerHost, fx.Options.NodeImage, ct)).Value.Should().NotBeNull();

        // Act: демонтаж X0–X3 (TO_REMOVE + тик депровижининга).
        await fx.PutAsync($"/valkey/clusters/{cluster}/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"TO_REMOVE"}""");
        var deprovision = fx.NewDeprovisioning(claims, fx.NewJournal());
        var result = await deprovision.TickAsync(await fx.RequireSnapshotAsync(cluster), ct);

        // Assert: ни контейнера, НЕТ volume vwk-<C>-tls, пустой префикс etcd.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        (await fx.Driver.ListNodeObjectsAsync(cluster, ct)).Value.Should().BeEmpty();
        (await fx.Driver.GetTlsArchiveAsync(cluster, ValkeyClusterFixture.DockerHost, fx.Options.NodeImage, ct)).Value.Should().BeNull(
            "TLS-volume удалён демонтажем (X1)");
        var prefix = await fx.Gateway.RangeAsync(fx.Endpoint, $"/valkey/clusters/{cluster}/", ct);
        prefix.Value.Should().BeEmpty();
    }

    // docker inspect -f {{.Id}} (пусто — объекта нет).
    private static string ContainerId(string name)
    {
        var psi = new System.Diagnostics.ProcessStartInfo(
            "docker", $"inspect --format {{{{.Id}}}} {name}")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var proc = System.Diagnostics.Process.Start(psi)!;
        var id = proc.StandardOutput.ReadToEnd().Trim();
        proc.WaitForExit(5000);
        return proc.ExitCode == 0 ? id : string.Empty;
    }
}
