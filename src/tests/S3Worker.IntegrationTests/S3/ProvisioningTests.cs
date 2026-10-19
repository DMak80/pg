using System.Net;
using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.Provisioning.Processes;
using S3Worker.IntegrationTests.Etcd;
using Xunit;

namespace S3Worker.IntegrationTests.S3;

// Provisioning реального ownS3-образа (spec §11 сценарий 1): сид → тик
// ProvisioningProcess → контейнер s3w-<C>-node1 + volume, /healthz 200,
// bucket существует (AWSSDK), endpoints, state=RUNNING, config без state.
[Collection(S3ClusterCollection.Name)]
public class ProvisioningTests(S3ClusterFixture fx)
{
    [Fact]
    public async Task Seed_ProvisioningTick_NodeAndDiscovery()
    {
        // Arrange: сид NOT_INITIALIZED + клэйм.
        DockerTrait.SkipIfUnavailable();
        var cluster = fx.Cluster("prov");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        (await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken)).Value.Should().BeTrue();
        var journal = fx.NewJournal();
        var process = fx.NewProvisioning(claims, journal,
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex());

        // Act: цикл тиков до Active (ownS3 стартует дольше тика — бюджет
        // NodeBootSec; spec §15.3 «циклом тиков ≤ NodeBootSec»).
        var result = await RunToActiveAsync(fx, process, cluster);

        // Assert: процесс done.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);

        // Контейнер + volume: s3w-<C>-node1, том s3w-<C>-node1-data.
        var objects = await fx.Driver.ListNodeObjectsAsync(cluster, TestContext.Current.CancellationToken);
        objects.Value.Should().Contain($"s3w-{cluster}-node1");
        objects.Value.Should().Contain($"s3w-{cluster}-node1-data");

        // Адрес из portalloc → health 200; bucket существует (AWSSDK exists).
        var portAlloc = await fx.GetAsync($"/s3worker/portalloc/{cluster}");
        portAlloc.Should().NotBeNull();
        var address = S3ProcessCommon.ParsePortAlloc(portAlloc!)["node1"];
        var health = await fx.NodeClient.HealthAsync(
            S3ClusterFixture.AdvertisedHost, address.S3Port, TestContext.Current.CancellationToken);
        health.IsSuccess.Should().BeTrue();
        health.Value.Should().BeTrue("ownS3 /healthz after provisioning");
        var secrets = fx.NodeSecrets;
        var bucket = await fx.NodeClient.BucketExistsAsync(
            S3ClusterFixture.AdvertisedHost, address.S3Port, secrets, "pgworker-backups", TestContext.Current.CancellationToken);
        bucket.IsSuccess.Should().BeTrue(bucket.Error?.Message);
        bucket.Value.Should().BeTrue("per-install bucket created by root credentials");

        // Дискавери: endpoints = localhost:<фактический порт>; state=RUNNING;
        // config без state; journal done.
        var endpoints = await fx.GetAsync($"/s3/clusters/{cluster}/endpoints");
        endpoints.Should().Be($"{S3ClusterFixture.AdvertisedHost}:{address.S3Port}");
        (await fx.GetAsync($"/s3/clusters/{cluster}/nodes/node1/state")).Should().Be("RUNNING");
        (await fx.GetAsync($"/s3/clusters/{cluster}/nodes/node1/placed_on")).Should().Be("local");
        var config = await fx.GetAsync($"/s3/clusters/{cluster}/config");
        config.Should().NotContain("state").And.Contain("\"platform\":\"owns3\"");
        (await fx.GetAsync($"/s3worker/work/{cluster}")).Should().Contain("done");

        // PKI per-cluster (arch/23 §6): ключи ca/orchestrator/инстанса в etcd.
        (await fx.GetAsync($"/s3/clusters/{cluster}/pki/ca")).Should().Contain("publicKeyPem");
        (await fx.GetAsync($"/s3/clusters/{cluster}/pki/orchestrator")).Should().Contain("certificate");
        (await fx.GetAsync($"/s3/clusters/{cluster}/nodes/node1/pki")).Should().Contain("certificate");

        // Репликационный материал в env живого контейнера (docker inspect env,
        // §3.6): сертификат/ключ/trust-anchor значениями + advertised-endpoint.
        var env = await fx.Driver.NodeEnvAsync(cluster, "node1", TestContext.Current.CancellationToken);
        env.IsSuccess.Should().BeTrue(env.Error?.Message);
        env.Value.Should().NotBeNull();
        env.Value!["OWNS3_REPLICATION_CERT"].Should().Contain("publicKeyPem");
        env.Value["OWNS3_REPLICATION_PRIVATE_KEY"].Should().Contain("BEGIN PRIVATE KEY");
        env.Value["OWNS3_REPLICATION_TRUST_CA"].Should().Contain("publicKeyPem");
        env.Value["OWNS3_REPLICATION_ADVERTISED_ENDPOINT"]
            .Should().Be($"http://{S3ClusterFixture.AdvertisedHost}:{address.S3Port}");
    }

    [Fact]
    public async Task RepeatTick_Idempotent()
    {
        // Arrange: поднятый кластер (первый тест серии — reuse запрещён;
        // поднимаем свой).
        DockerTrait.SkipIfUnavailable();
        var cluster = fx.Cluster("idem");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        (await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken)).Value.Should().BeTrue();
        var process = fx.NewProvisioning(claims, fx.NewJournal(),
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex());
        await RunToActiveAsync(fx, process, cluster);

        var configBefore = await fx.GetAsync($"/s3/clusters/{cluster}/config");
        var portAllocBefore = await fx.GetAsync($"/s3worker/portalloc/{cluster}");
        var endpointBefore = await fx.GetAsync($"/s3/clusters/{cluster}/endpoints");

        // Act: повторный тик (уже Active).
        var result = await process.RunAsync(
            await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken);

        // Assert: Success; portalloc/endpoints/config стабильны.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        (await fx.GetAsync($"/s3worker/portalloc/{cluster}")).Should().Be(portAllocBefore);
        (await fx.GetAsync($"/s3/clusters/{cluster}/endpoints")).Should().Be(endpointBefore);
        (await fx.GetAsync($"/s3/clusters/{cluster}/config")).Should().Be(configBefore);
    }

    // Цикл тиков provisioning до journal done (бюджет NodeBootSec).
    internal static async Task<Shared.Core.Result> RunToActiveAsync(
        S3ClusterFixture fx, ProvisioningProcess process, string cluster)
    {
        Shared.Core.Result last = Shared.Core.Result.Success();
        var deadline = DateTimeOffset.UtcNow.AddSeconds(fx.Options.NodeBootSec);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fx.RequireSnapshotAsync(cluster);
            last = await process.RunAsync(snap, TestContext.Current.CancellationToken);
            var phase = await fx.GetAsync($"/s3worker/work/{cluster}");
            if (last.IsSuccess && phase!.Contains("done", StringComparison.Ordinal))
                return last;
            await Task.Delay(1000, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException(
            $"provisioning of {cluster} did not reach done within {fx.Options.NodeBootSec} s: {last.Error?.Message}");
    }
}


