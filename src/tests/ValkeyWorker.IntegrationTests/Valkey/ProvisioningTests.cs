using FluentAssertions;
using ValkeyWorker.Core.Valkey;
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

        // Контейнер: имя vwk-<C>-node1, образ 9.1.2; Cmd — обёртка env-TLS
        // (sh -c раскатка + exec valkey-server), persistence off в args.
        var objects = await fx.Driver.ListNodeObjectsAsync(cluster, TestContext.Current.CancellationToken);
        objects.Value.Should().ContainSingle().Which.Should().Be($"vwk-{cluster}-node1");
        var args = await fx.Driver.NodeArgsAsync(cluster, "node1", TestContext.Current.CancellationToken);
        args.Value.Should().NotBeNull();
        args.Value!.Should().HaveCount(3);
        args.Value[0].Should().Be("sh");
        args.Value[1].Should().Be("-c");
        args.Value[2].Should().StartWith("umask 077; mkdir -p /tls;");
        args.Value[2].Should().Contain("exec 'valkey-server'");
        args.Value[2].Should().Contain("'--save' ''", "persistence off: --save \"\"");
        args.Value[2].Should().Contain("'--appendonly' 'no'");
        args.Value[2].Should().Contain("'--tls-cert-file' '/tls/node.crt'");

        // Env-модель: контейнер несёт VALKEY_TLS_{CERT,KEY,CA}; CA == etcd-факту.
        var env = await fx.Driver.NodeEnvAsync(cluster, "node1", TestContext.Current.CancellationToken);
        env.IsSuccess.Should().BeTrue(env.Error?.Message);
        env.Value.Should().NotBeNull();
        env.Value.Should().ContainKeys("VALKEY_TLS_CERT", "VALKEY_TLS_KEY", "VALKEY_TLS_CA");
        env.Value!["VALKEY_TLS_CERT"].Should().Contain("BEGIN CERTIFICATE");
        env.Value["VALKEY_TLS_KEY"].Should().Contain("BEGIN PRIVATE KEY");
        var envCa = await fx.GetAsync($"/valkey/clusters/{cluster}/ca_pem");
        env.Value["VALKEY_TLS_CA"].Trim().Should().Be(envCa!.Trim(), "CA env == ca_pem кластера");

        // Креды: 32 симв; PING admin-кредом → PONG.
        var adminPassword = await fx.GetAsync($"/valkey/clusters/{cluster}/admin_password");
        adminPassword.Should().HaveLength(32);
        var appPassword = await fx.GetAsync($"/valkey/clusters/{cluster}/app_password");
        appPassword.Should().HaveLength(32);
        var endpoints = await fx.GetAsync($"/valkey/clusters/{cluster}/endpoints");
        endpoints.Should().NotBeNull();
        var port = int.Parse(endpoints!.Split(':')[1]);
        var caPem = await fx.GetAsync($"/valkey/clusters/{cluster}/ca_pem");
        caPem.Should().NotBeNullOrEmpty();
        var ping = RespProbe.ExecuteTls("localhost", port, "admin", adminPassword!, caPem!, "PING");
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

    // t06: новый кластер TLS-only — ca-ключи в etcd, env VALKEY_TLS_* у ноды,
    // app-кред roundtrip по TLS, plain-подключение отклонено, ACL-матрица прежняя.
    [Fact]
    public async Task NewCluster_TlsOnly_PlainRejected()
    {
        // Arrange: заявка + клэйм + один тик provisioning.
        var cluster = fx.Cluster("tlsprov");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        (await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken)).Value.Should().BeTrue();
        var process = fx.NewProvisioning(claims, fx.NewJournal(),
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex(), fx.NewSecretEnsurer());

        // Act
        var result = await process.TickAsync(await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken);

        // Assert 1: ca_pem/ca_key в etcd, PEM валиден.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var caPem = (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_pem"))!;
        var caKey = (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_key"))!;
        caPem.Should().Contain("BEGIN CERTIFICATE");
        caKey.Should().Contain("BEGIN PRIVATE KEY");
        ValkeyPki.TryParseCertificate(caPem, out _).Should().BeTrue();
        ValkeyPki.TryParseRsaKey(caKey, out _).Should().BeTrue();

        // Assert 2: endpoints записаны; env контейнера — VALKEY_TLS_* c CA
        // из etcd (env-модель: TLS-материал доставляется env ноды).
        var endpoints = (await fx.GetAsync($"/valkey/clusters/{cluster}/endpoints"))!;
        var port = int.Parse(endpoints.Split(':')[1]);
        var nodeEnv = await fx.Driver.NodeEnvAsync(cluster, "node1", TestContext.Current.CancellationToken);
        nodeEnv.IsSuccess.Should().BeTrue(nodeEnv.Error?.Message);
        nodeEnv.Value.Should().NotBeNull();
        nodeEnv.Value.Should().ContainKeys("VALKEY_TLS_CERT", "VALKEY_TLS_KEY", "VALKEY_TLS_CA");
        nodeEnv.Value!["VALKEY_TLS_CA"].Trim().Should().Be(caPem.Trim(), "CA env == ca_pem кластера");

        // Assert 3: app-кред roundtrip по TLS с ca_pem из etcd.
        var appPassword = (await fx.GetAsync($"/valkey/clusters/{cluster}/app_password"))!;
        var set = RespProbe.ExecuteTls("localhost", port, "app", appPassword, caPem, "SET", "tls:key", "v1");
        set.Ok.Should().BeTrue(set.Error);
        var get = RespProbe.ExecuteTls("localhost", port, "app", appPassword, caPem, "GET", "tls:key");
        get.Value.Should().Be("v1");

        // Assert 4: plain-подключение отклонено (сырой PING без TLS: сервер
        // рвёт соединение — отказ это и Ok=false, и исключение сброса кадра).
        var plainRejected = false;
        try
        {
            var plain = RespProbe.Execute("localhost", port, "app", appPassword, "PING");
            plainRejected = !plain.Ok;
        }
        catch (ApplicationException)
        {
            // Соединение сброшено на хендшейке — plain-порт закрыт (--port 0).
            plainRejected = true;
        }

        plainRejected.Should().BeTrue("plain-порт закрыт (--port 0)");

        // Assert 5: ACL-матрица прежняя (app без +@all: CONFIG GET → отказ).
        var configGet = RespProbe.ExecuteTls("localhost", port, "app", appPassword, caPem, "CONFIG", "GET", "maxmemory");
        configGet.Ok.Should().BeFalse();
    }
}

// Один etcd + один docker-драйвер на Valkey-группу (кластеры RunTag-уникальны).
[CollectionDefinition(Name)]
public class ValkeyClusterCollection : ICollectionFixture<ValkeyClusterFixture>
{
    public const string Name = "valkey-cluster";
}
